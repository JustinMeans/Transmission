import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore

/// The server end of one socket.
public protocol LiveServerTransport: Sendable {
    /// Resumes when the bytes have been written, which is how slowness is seen.
    func write(_ frame: LiveWireFrame) async throws
    /// Must not wait for the peer: this is how a stalled peer gets dropped.
    func close(code: LiveCloseCode) async
}

/// One client. Owns a single writer task, one bounded mailbox per subscription
/// and a reader that handles control frames in order.
public final class LiveConnection: Sendable {
    private enum Phase {
        case awaitingHello
        case open
        case closed
    }

    private struct State {
        var phase = Phase.awaitingHello
        var mailboxes: [UInt64: LiveMailbox] = [:]
        var order: [UInt64] = []
        var nextIndex = 0
        var control: [LiveWireFrame] = []
        var writeStartedAt: ContinuousClock.Instant?
        var lastInbound = ContinuousClock.now
        var lastHeartbeat = ContinuousClock.now
        var lastRevalidate = ContinuousClock.now
        var expiryWarned = false
        var identity: LiveIdentity?
        var clientID: String?
        var inboundQueued = 0
    }

    /// Control frames are few; anything past this is a client abusing `SUB`.
    private static let maxControlFrames = 256
    private static let maxInboundQueued = 256

    public let hub: LiveHub
    private let configuration: LiveHubConfiguration
    private let transport: any LiveServerTransport
    private let logger: Logger
    private let state: NIOLockedValueBox<State>

    private let doorbell: AsyncStream<Void>
    private let doorbellContinuation: AsyncStream<Void>.Continuation
    private let inbound: AsyncStream<[UInt8]>
    private let inboundContinuation: AsyncStream<[UInt8]>.Continuation
    private let tasks = NIOLockedValueBox<[Task<Void, Never>]>([])

    public init(hub: LiveHub, transport: any LiveServerTransport, identity: LiveIdentity?) {
        self.hub = hub
        self.configuration = hub.configuration
        self.transport = transport
        self.logger = hub.logger
        var initial = State()
        initial.identity = identity
        state = NIOLockedValueBox(initial)
        (doorbell, doorbellContinuation) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
        (inbound, inboundContinuation) = AsyncStream.makeStream(of: [UInt8].self)
    }

    /// Starts reading and writing. Call once.
    public func start() async {
        await hub.add(self)
        // The socket can die during that await, and its shutdown then ran before
        // the hub knew about this connection.
        let closed = state.withLockedValue { state -> (String?, String?)? in
            state.phase == .closed ? (state.clientID, state.identity?.subject) : nil
        }
        if let closed {
            await hub.remove(self, clientID: closed.0, subject: closed.1)
            return
        }
        let writer = Task { await self.writeLoop() }
        let reader = Task { await self.readLoop() }
        let ticker = Task { await self.tickLoop() }
        tasks.withLockedValue { $0 = [writer, reader, ticker] }
        // Same race, one step later: shutdown saw no tasks to cancel.
        if isClosed {
            tasks.withLockedValue { $0.forEach { $0.cancel() } }
        }
    }

    /// Hands over one inbound message. Never blocks.
    public func receive(_ bytes: [UInt8]) {
        let overflow = state.withLockedValue { state -> Bool in
            state.lastInbound = .now
            state.inboundQueued += 1
            return state.inboundQueued > Self.maxInboundQueued
        }
        if overflow {
            shutdown(code: .protocolError)
        } else {
            inboundContinuation.yield(bytes)
        }
    }

    /// The socket is gone. Idempotent.
    public func transportClosed() {
        shutdown(code: nil)
    }

    /// Closes from the server side. Idempotent.
    public func close(code: LiveCloseCode) {
        shutdown(code: code)
    }

    public var isClosed: Bool {
        state.withLockedValue { $0.phase == .closed }
    }

    public var subscriptionCount: Int {
        state.withLockedValue { $0.mailboxes.count }
    }

    /// The last sequence a subscription has acknowledged, for lag monitoring.
    public func ackedSeq(subID: UInt64) -> UInt64? {
        state.withLockedValue { $0.mailboxes[subID]?.ackedSeq }
    }

    func topicAdvanced() {
        doorbellContinuation.yield()
    }

    // MARK: Writer

    private enum WriterAction {
        case write(LiveWireFrame)
        case wait
        case stop
    }

    private func nextAction(now: ContinuousClock.Instant) -> (WriterAction, ContinuousClock.Instant?) {
        state.withLockedValue { state in
            guard state.phase != .closed else { return (.stop, nil) }
            if !state.control.isEmpty {
                state.writeStartedAt = now
                return (.write(state.control.removeFirst()), nil)
            }
            var earliestWake: ContinuousClock.Instant?
            let count = state.order.count
            for offset in 0..<count {
                let id = state.order[(state.nextIndex + offset) % count]
                guard var mailbox = state.mailboxes[id] else { continue }
                let pull = mailbox.pull(now: now)
                state.mailboxes[id] = mailbox
                switch pull {
                case .frame(let frame):
                    state.nextIndex = (state.nextIndex + offset + 1) % count
                    state.writeStartedAt = now
                    return (.write(frame), nil)
                case .wake(let instant):
                    earliestWake = min(earliestWake ?? instant, instant)
                case .idle:
                    break
                }
            }
            return (.wait, earliestWake)
        }
    }

    private func writeLoop() async {
        var bell = doorbell.makeAsyncIterator()
        var wakeTask: Task<Void, Never>?
        defer { wakeTask?.cancel() }
        while !Task.isCancelled {
            let (action, wake) = nextAction(now: .now)
            switch action {
            case .stop:
                return
            case .write(let frame):
                do {
                    try await transport.write(frame)
                } catch {
                    shutdown(code: nil)
                    return
                }
                state.withLockedValue { $0.writeStartedAt = nil }
            case .wait:
                wakeTask?.cancel()
                wakeTask = nil
                if let wake {
                    let continuation = doorbellContinuation
                    wakeTask = Task {
                        try? await Task.sleep(until: wake, clock: .continuous)
                        if !Task.isCancelled { continuation.yield() }
                    }
                }
                if await bell.next() == nil { return }
            }
        }
    }

    // MARK: Ticker

    private func tickLoop() async {
        while !Task.isCancelled {
            try? await Task.sleep(for: configuration.tickInterval)
            guard !Task.isCancelled else { return }
            await tick(now: .now)
        }
    }

    /// Stalls, heartbeats and identity checks. Runs on its own task because the
    /// writer may be stuck in exactly the write this needs to notice.
    private func tick(now: ContinuousClock.Instant) async {
        let heartbeat = configuration.heartbeat
        enum Verdict { case fine, slow, dead, expired }
        var sendBeat = false
        var warnSeconds: UInt64?
        var revalidate: LiveIdentity?
        let verdict = state.withLockedValue { state -> Verdict in
            guard state.phase != .closed else { return .fine }
            if let started = state.writeStartedAt, started.duration(to: now) >= configuration.stallLimit {
                return .slow
            }
            let behind = state.mailboxes.values.reduce(0) { $0 + $1.bytesBehind }
            if behind > configuration.maxQueuedBytes {
                return .slow
            }
            if state.lastInbound.duration(to: now) >= heartbeat * 2 {
                return .dead
            }
            if state.lastHeartbeat.duration(to: now) >= heartbeat {
                state.lastHeartbeat = now
                sendBeat = true
            }
            if let expiry = state.identity?.expiresAt {
                let left = expiry.timeIntervalSinceNow
                if left <= 0 { return .expired }
                if !state.expiryWarned, left <= Self.seconds(configuration.expiryWarning) {
                    state.expiryWarned = true
                    warnSeconds = UInt64(left.rounded(.up))
                }
            }
            if state.lastRevalidate.duration(to: now) >= configuration.revalidateInterval {
                state.lastRevalidate = now
                revalidate = state.identity
            }
            return .fine
        }
        switch verdict {
        case .slow:
            logger.notice("Closing slow consumer")
            shutdown(code: .slowConsumer)
        case .dead:
            shutdown(code: .deadPeer)
        case .expired:
            shutdown(code: .authExpired)
        case .fine:
            if sendBeat {
                enqueue(.heartbeat(serverTimeMs: Self.nowMs()))
            }
            if let warnSeconds {
                enqueue(.authExpiring(secondsLeft: warnSeconds))
            }
            if let revalidate, await !hub.authorizer.revalidate(revalidate) {
                shutdown(code: .authExpired)
            }
        }
    }

    private static func seconds(_ duration: Duration) -> Double {
        let (seconds, attoseconds) = duration.components
        return Double(seconds) + Double(attoseconds) / 1e18
    }

    private static func nowMs() -> UInt64 {
        UInt64(Date().timeIntervalSince1970 * 1000)
    }

    private func enqueue(_ frame: LiveFrame) {
        let overflow = state.withLockedValue { state -> Bool in
            state.control.append(LiveCodec.wireFrame(frame))
            return state.control.count > Self.maxControlFrames
        }
        if overflow {
            shutdown(code: .protocolError)
        } else {
            doorbellContinuation.yield()
        }
    }

    // MARK: Reader

    private func readLoop() async {
        for await bytes in inbound {
            state.withLockedValue { $0.inboundQueued -= 1 }
            do {
                try await handle(LiveCodec.decode(bytes))
            } catch {
                enqueue(.error(subID: 0, code: .malformed, retryable: false, message: "Malformed frame"))
                shutdown(code: .protocolError)
                return
            }
        }
    }

    private func handle(_ frame: LiveFrame) async throws {
        let phase = state.withLockedValue { $0.phase }
        switch (phase, frame) {
        case (.closed, _):
            return
        case (.awaitingHello, .hello(let protoMin, let protoMax, _, let clientID)):
            await handleHello(protoMin: protoMin, protoMax: protoMax, clientID: clientID)
        case (.awaitingHello, _):
            shutdown(code: .protocolError)
        case (.open, .sub(let subID, let topic, let params, let schema, let maxMilliHz, let resume)):
            await handleSub(subID: subID, topic: topic, params: params, schema: schema, maxMilliHz: maxMilliHz, resume: resume)
        case (.open, .unsub(let subID)):
            await removeSubscription(subID)
        case (.open, .pause(let subID)):
            state.withLockedValue { $0.mailboxes[subID]?.paused = true }
        case (.open, .resume(let subID)):
            state.withLockedValue { $0.mailboxes[subID]?.paused = false }
            doorbellContinuation.yield()
        case (.open, .ack(let subID, let seq)):
            state.withLockedValue { $0.mailboxes[subID]?.ackedSeq = seq }
        case (.open, .heartbeat):
            break
        case (.open, .authRefresh(let token)):
            await handleAuthRefresh(token)
        case (.open, .call(_, let callID, let payload)):
            _ = payload
            enqueue(.reply(callID: callID, isError: true, payload: Data("No such alias".utf8)))
        default:
            // Unknown types and frames that only flow the other way.
            break
        }
    }

    private func handleHello(protoMin: UInt64, protoMax: UInt64, clientID: String) async {
        guard protoMin <= LiveProtocol.version, protoMax >= LiveProtocol.version else {
            enqueue(.error(subID: 0, code: .updateRequired, retryable: false, message: "Update required"))
            await drainThenClose(code: .updateRequired)
            return
        }
        let subject = state.withLockedValue { state -> String? in
            state.clientID = clientID
            state.phase = .open
            return state.identity?.subject
        }
        if let previous = await hub.claim(clientID: clientID, subject: subject, connection: self) {
            previous.shutdown(code: .superseded)
        }
        let heartbeatMs = UInt64(Self.seconds(configuration.heartbeat) * 1000)
        enqueue(.welcome(
            proto: LiveProtocol.version, epoch: hub.epoch, serverTimeMs: Self.nowMs(),
            heartbeatMs: heartbeatMs, maxFrame: UInt64(configuration.maxFrame),
            topics: await hub.adverts()))
    }

    private func handleSub(
        subID: UInt64, topic: String, params: [String: String],
        schema: UInt64, maxMilliHz: UInt64, resume: LiveCursor?
    ) async {
        guard subID != 0, let schemaValue = Int(exactly: schema) else {
            enqueue(.error(subID: subID, code: .malformed, retryable: false, message: "Invalid subscription"))
            return
        }
        let isNew = state.withLockedValue { $0.mailboxes[subID] == nil }
        let full = state.withLockedValue { $0.mailboxes.count >= configuration.maxSubscriptionsPerConnection }
        guard !(isNew && full) else {
            enqueue(.error(subID: subID, code: .tooManySubscriptions, retryable: false, message: "Too many subscriptions"))
            return
        }
        let key = TopicKey(name: topic, params: params, schema: schemaValue)
        let identity = state.withLockedValue { $0.identity }
        switch await hub.subscribe(self, key: key, identity: identity) {
        case .refused(let code, let message):
            enqueue(.error(subID: subID, code: code, retryable: false, message: message))
        case .ok(let instance):
            await removeSubscription(subID)
            install(subID: subID, instance: instance, schema: schema, maxMilliHz: maxMilliHz, resume: resume)
        }
    }

    private func install(
        subID: UInt64, instance: LiveTopicInstance, schema: UInt64, maxMilliHz: UInt64, resume: LiveCursor?
    ) {
        let ring = instance.ring
        var cursor: UInt64
        var pending: [LiveWireFrame] = []
        var replay = false
        if let resume, resume.epoch == ring.epoch, resume.seq <= ring.headSeq,
           ring.canReplay(from: resume.seq) || instance.kind == .log || instance.kind == .event {
            cursor = resume.seq
            replay = true
        } else {
            do {
                let snapshot = try instance.snapshot()
                cursor = snapshot.seq
                pending = snapshot.parts.map {
                    LiveWireFrame(type: FrameType.snapshot, flags: $0.flags, subID: subID, body: $0.body)
                }
            } catch {
                logger.error("Snapshot failed for \(instance.key): \(error)")
                enqueue(.error(subID: subID, code: .unspecified, retryable: true, message: "Snapshot failed"))
                return
            }
        }
        let head = replay ? ring.headSeq : cursor
        let ack = LiveCodec.wireFrame(.subAck(
            subID: subID, epoch: ring.epoch, headSeq: head, schema: schema, replay: replay))
        let mailbox = LiveMailbox(
            subID: subID, instance: instance, capacity: configuration.mailboxCapacity,
            maxMilliHz: maxMilliHz, cursor: cursor, pending: [ack] + pending)
        state.withLockedValue { state in
            state.mailboxes[subID] = mailbox
            state.order.append(subID)
        }
        doorbellContinuation.yield()
    }

    private func removeSubscription(_ subID: UInt64) async {
        let removed = state.withLockedValue { state -> TopicKey? in
            guard let mailbox = state.mailboxes.removeValue(forKey: subID) else { return nil }
            state.order.removeAll { $0 == subID }
            state.nextIndex = 0
            return mailbox.instance.key
        }
        if let removed {
            await hub.unsubscribe(self, key: removed)
        }
    }

    private func handleAuthRefresh(_ token: String) async {
        guard let identity = try? await hub.authorizer.authenticate(token: token) else {
            enqueue(.error(subID: 0, code: .authFailed, retryable: false, message: "Token rejected"))
            await drainThenClose(code: .authFailed)
            return
        }
        // The subscriptions were authorized for this subject; a swap would inherit them.
        let sameSubject = state.withLockedValue { state -> Bool in
            guard state.identity == nil || state.identity?.subject == identity.subject else { return false }
            state.identity = identity
            state.expiryWarned = false
            return true
        }
        if !sameSubject {
            await drainThenClose(code: .authFailed)
        }
    }

    // MARK: Shutdown

    /// Gives queued control frames, chiefly the reason for closing, a moment to
    /// leave before the socket goes.
    private func drainThenClose(code: LiveCloseCode) async {
        for _ in 0..<50 {
            let idle = state.withLockedValue { $0.control.isEmpty && $0.writeStartedAt == nil }
            if idle { break }
            try? await Task.sleep(for: .milliseconds(10))
        }
        shutdown(code: code)
    }

    /// Never awaits a client. `code` is nil when the socket is already gone.
    private func shutdown(code: LiveCloseCode?) {
        let details = state.withLockedValue { state -> (String?, String?)? in
            guard state.phase != .closed else { return nil }
            state.phase = .closed
            state.mailboxes.removeAll()
            state.order.removeAll()
            state.control.removeAll()
            return (state.clientID, state.identity?.subject)
        }
        guard let (clientID, subject) = details else { return }
        inboundContinuation.finish()
        doorbellContinuation.finish()
        tasks.withLockedValue { tasks in
            tasks.forEach { $0.cancel() }
        }
        let hub = self.hub
        let transport = self.transport
        Task {
            await hub.remove(self, clientID: clientID, subject: subject)
            if let code {
                await transport.close(code: code)
            }
        }
    }
}
