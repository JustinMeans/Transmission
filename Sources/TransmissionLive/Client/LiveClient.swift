import Foundation
import Logging

/// Retry behaviour for `LiveClient`.
public struct LiveReconnectPolicy: Sendable {
    public var minimumDelay: Duration
    public var maximumDelay: Duration
    public var multiplier: Double
    /// A session must last this long before it resets the curve.
    public var minimumSessionLifetime: Duration

    public init(
        minimumDelay: Duration = .milliseconds(500),
        maximumDelay: Duration = .seconds(30),
        multiplier: Double = 1.618,
        minimumSessionLifetime: Duration = .seconds(10)
    ) {
        self.minimumDelay = minimumDelay
        self.maximumDelay = maximumDelay
        self.multiplier = multiplier
        self.minimumSessionLifetime = minimumSessionLifetime
    }
}

public enum LiveConnectionState: Sendable, Equatable {
    case idle
    case connecting
    case connected
    case waitingToRetry
    /// `pause()` was called. Cursors are kept.
    case paused
    /// The server requires a newer build. Terminal until the app updates.
    case updateRequired
    /// Another connection for this client took over.
    case superseded
    case stopped
}

public struct LiveClientConfiguration: Sendable {
    public var url: URL
    public var clientID: String
    public var build: String
    public var tokenProvider: (@Sendable () async throws -> String?)?
    public var reconnect = LiveReconnectPolicy()
    public var maxMessageSize = 16 << 20
    /// Send `ACK` after this many deltas on a subscription.
    public var ackEvery = 32
    /// Used until `WELCOME` supplies the server's interval.
    public var defaultHeartbeat: Duration = .seconds(25)
    public var protoMin = LiveProtocol.version
    public var protoMax = LiveProtocol.version

    public init(
        url: URL,
        clientID: String,
        build: String,
        tokenProvider: (@Sendable () async throws -> String?)? = nil
    ) {
        self.url = url
        self.clientID = clientID
        self.build = build
        self.tokenProvider = tokenProvider
    }
}

/// The client SDK. Subscriptions belong to this object, not to a socket: they
/// survive reconnects and resume from their cursors.
public actor LiveClient {
    private let configuration: LiveClientConfiguration
    private let transport: any LiveTransport
    private let logger: Logger

    private var subscriptions: [UInt64: any LiveClientSubscription] = [:]
    private var nextSubID: UInt64 = 1
    private var loop: Task<Void, Never>?
    private var connection: (any LiveTransportConnection)?
    private var sender: AsyncStream<Data>.Continuation?
    private var retrySleep: Task<Void, any Error>?
    private var skipWait = false
    private var lastInbound = ContinuousClock.now
    private var isConnected = false

    public private(set) var state = LiveConnectionState.idle
    private let statesStream: AsyncStream<LiveConnectionState>
    private let statesContinuation: AsyncStream<LiveConnectionState>.Continuation

    #if !canImport(FoundationNetworking)
    public init(configuration: LiveClientConfiguration, logger: Logger = Logger(label: "transmission.live.client")) {
        self.init(configuration: configuration, transport: URLSessionLiveTransport(), logger: logger)
    }
    #endif

    public init(configuration: LiveClientConfiguration, transport: any LiveTransport, logger: Logger = Logger(label: "transmission.live.client")) {
        self.configuration = configuration
        self.transport = transport
        self.logger = logger
        (statesStream, statesContinuation) = AsyncStream.makeStream(bufferingPolicy: .bufferingNewest(16))
    }

    /// Connection state changes, for diagnostics and banners.
    public nonisolated var stateChanges: AsyncStream<LiveConnectionState> { statesStream }

    // MARK: Lifecycle

    /// Connects and stays connected until `stop` or `pause`. Idempotent.
    public func start() {
        guard loop == nil else { return }
        skipWait = false
        loop = Task { await self.runLoop() }
    }

    /// Ends the session and finishes every subscription's stream.
    public func stop() async {
        await endLoop(code: .normal)
        setState(.stopped)
        for subscription in subscriptions.values { subscription.finish() }
        subscriptions.removeAll()
    }

    /// For app background: tells the server, closes cleanly, keeps cursors.
    public func pause() async {
        guard state != .paused else { return }
        for subscription in subscriptions.values where !subscription.isPaused {
            enqueue(.pause(subID: subscription.subID))
        }
        await endLoop(code: .normal)
        setState(.paused)
    }

    /// For app foreground: reconnects at once and resumes from the cursors.
    public func resume() {
        if state == .paused {
            setState(.idle)
        }
        skipWait = true
        retrySleep?.cancel()
        start()
    }

    /// Retries now, bypassing backoff: for a network path change. A session
    /// that is up is left alone unless `dropExisting`, since the OS may already
    /// have killed a socket that still looks open.
    public func reconnectNow(dropExisting: Bool = false) async {
        skipWait = true
        retrySleep?.cancel()
        if dropExisting, let connection {
            await connection.close(code: .normal)
        }
    }

    private func endLoop(code: LiveCloseCode) async {
        let task = loop
        loop = nil
        task?.cancel()
        retrySleep?.cancel()
        sender?.finish()
        if let connection {
            await connection.close(code: code)
        }
        await task?.value
    }

    /// Hands the server a new token mid-session.
    public func refreshToken(_ token: String) {
        enqueue(.authRefresh(token: token))
    }

    private func refreshTokenFromProvider() {
        guard let provider = configuration.tokenProvider else { return }
        Task {
            if let token = try? await provider() {
                self.refreshToken(token)
            }
        }
    }

    // MARK: Subscribing

    public func subscribe<State, Delta>(
        _ topic: Topic<State, Delta>,
        maxHz: Double = 0
    ) -> LiveSubscription<State, Delta> {
        // Logs keep every update so that none is lost to a busy consumer; state
        // topics only ever need the newest.
        let buffering: AsyncStream<LiveUpdate<State, Delta>>.Continuation.BufferingPolicy =
            topic.kind == .log || topic.kind == .event ? .bufferingNewest(4096) : .bufferingNewest(1)
        let (stream, continuation) = AsyncStream.makeStream(of: LiveUpdate<State, Delta>.self, bufferingPolicy: buffering)
        let subID = nextSubID
        nextSubID += 1
        let subscription = TypedClientSubscription(
            subID: subID, topic: topic, maxMilliHz: UInt64((maxHz * 1000).rounded()), continuation: continuation)
        subscriptions[subID] = subscription
        if isConnected {
            issueSub(subscription)
        }
        return LiveSubscription(updates: stream, subID: subID, client: self)
    }

    func unsubscribe(subID: UInt64) {
        guard let subscription = subscriptions.removeValue(forKey: subID) else { return }
        subscription.finish()
        enqueue(.unsub(subID: subID))
    }

    func setPaused(subID: UInt64, paused: Bool) {
        guard let subscription = subscriptions[subID] else { return }
        subscription.isPaused = paused
        enqueue(paused ? .pause(subID: subID) : .resume(subID: subID))
    }

    private func issueSub(_ subscription: any LiveClientSubscription) {
        subscription.beginResync()
        let frame = subscription.frame
        enqueue(.sub(
            subID: subscription.subID, topic: frame.topic, params: frame.params, schema: frame.schema,
            maxMilliHz: frame.maxMilliHz, resume: subscription.cursor))
        if subscription.isPaused {
            enqueue(.pause(subID: subscription.subID))
        }
    }

    private func enqueue(_ frame: LiveFrame) {
        sender?.yield(LiveCodec.encode(frame))
    }

    private func setState(_ new: LiveConnectionState) {
        guard new != state else { return }
        state = new
        statesContinuation.yield(new)
    }

    // MARK: Session

    private func runLoop() async {
        let policy = configuration.reconnect
        let clock = ContinuousClock()
        var delay = policy.minimumDelay
        while !Task.isCancelled {
            setState(.connecting)
            let started = clock.now
            let outcome = await runSession()
            connectionEnded()
            if Task.isCancelled { return }
            switch outcome {
            case .terminal(let terminal):
                setState(terminal)
                loop = nil
                return
            case .retry(let established):
                if established, started.duration(to: clock.now) >= policy.minimumSessionLifetime {
                    delay = policy.minimumDelay
                }
            }
            setState(.waitingToRetry)
            let jittered = delay * Double.random(in: 0.75...1.25)
            await waitBeforeRetry(jittered)
            delay = min(delay * policy.multiplier, policy.maximumDelay)
            if skipWait {
                skipWait = false
                delay = policy.minimumDelay
            }
        }
    }

    private func waitBeforeRetry(_ delay: Duration) async {
        guard !skipWait else { return }
        let sleep = Task { try await Task.sleep(for: delay) }
        retrySleep = sleep
        _ = await withTaskCancellationHandler {
            await sleep.result
        } onCancel: {
            sleep.cancel()
        }
        retrySleep = nil
    }

    private enum SessionOutcome {
        case retry(established: Bool)
        case terminal(LiveConnectionState)
    }

    private func runSession() async -> SessionOutcome {
        var headers: [String: String] = [:]
        do {
            if let token = try await configuration.tokenProvider?() {
                headers["Authorization"] = "Bearer \(token)"
            }
            let connection = try await transport.connect(LiveConnectRequest(
                url: configuration.url,
                headers: headers,
                subprotocols: [LiveProtocol.subprotocol],
                maxMessageSize: configuration.maxMessageSize))
            if Task.isCancelled {
                await connection.close(code: .normal)
                return .retry(established: false)
            }
            self.connection = connection
            return await drive(connection)
        } catch {
            logger.debug("Live connect failed: \(error)")
            return .retry(established: false)
        }
    }

    private func drive(_ connection: any LiveTransportConnection) async -> SessionOutcome {
        let (outgoing, sink) = AsyncStream.makeStream(of: Data.self)
        sender = sink
        lastInbound = .now

        // One sender task, so that handling a frame never waits on the socket.
        let writer = Task {
            for await data in outgoing {
                do {
                    try await connection.send(data)
                } catch {
                    await connection.close(code: .normal)
                    return
                }
            }
        }
        defer {
            writer.cancel()
            sink.finish()
            sender = nil
        }

        enqueue(.hello(
            protoMin: configuration.protoMin, protoMax: configuration.protoMax,
            build: configuration.build, clientID: configuration.clientID))

        var watchdog: Task<Void, Never>?
        defer { watchdog?.cancel() }
        var established = false
        while true {
            let data: Data
            do {
                data = try await connection.receive()
            } catch let closed as LiveTransportClosed {
                return outcome(for: closed.code, established: established)
            } catch {
                return .retry(established: established)
            }
            lastInbound = .now
            let frame: LiveFrame
            do {
                frame = try LiveCodec.decode(data)
            } catch {
                logger.error("Malformed live frame: \(error)")
                await connection.close(code: .protocolError)
                return .retry(established: established)
            }
            switch frame {
            case .welcome(_, _, _, let heartbeatMs, _, _):
                established = true
                isConnected = true
                setState(.connected)
                watchdog?.cancel()
                watchdog = startWatchdog(heartbeat: .milliseconds(Int64(heartbeatMs)), connection: connection)
                for subscription in subscriptions.values.sorted(by: { $0.subID < $1.subID }) {
                    issueSub(subscription)
                }
            case .error(_, .updateRequired, _, _):
                await connection.close(code: .normal)
                return .terminal(.updateRequired)
            default:
                handle(frame)
            }
        }
    }

    private func outcome(for code: LiveCloseCode?, established: Bool) -> SessionOutcome {
        switch code {
        case .updateRequired?:
            return .terminal(.updateRequired)
        case .superseded?:
            return .terminal(.superseded)
        default:
            return .retry(established: established)
        }
    }

    /// Hearing nothing for two intervals means the socket is dead, however it looks.
    private func startWatchdog(heartbeat: Duration, connection: any LiveTransportConnection) -> Task<Void, Never> {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(for: heartbeat / 2)
                guard let self, !Task.isCancelled else { return }
                if await self.silence() >= heartbeat * 2 {
                    await connection.close(code: .deadPeer)
                    return
                }
            }
        }
    }

    private func silence() -> Duration {
        lastInbound.duration(to: .now)
    }

    private func connectionEnded() {
        isConnected = false
        connection = nil
        let now = Date()
        for subscription in subscriptions.values {
            subscription.connectionLost(at: now)
        }
    }

    private func handle(_ frame: LiveFrame) {
        switch frame {
        case let .subAck(subID, epoch, headSeq, _, replay):
            subscriptions[subID]?.handleAck(epoch: epoch, headSeq: headSeq, replay: replay)
        case let .snapshot(subID, epoch, seq, _, part, last, payload):
            guard let subscription = subscriptions[subID] else { return }
            if !subscription.handleSnapshot(epoch: epoch, seq: seq, part: part, last: last, payload: payload) {
                issueSub(subscription)
            } else if last {
                enqueue(.ack(subID: subID, seq: seq))
            }
        case let .delta(subID, seq, _, span, payload):
            guard let subscription = subscriptions[subID] else { return }
            if !subscription.handleDelta(seq: seq, span: span, payload: payload) {
                issueSub(subscription)
            } else if let ack = subscription.shouldAck(every: configuration.ackEvery) {
                enqueue(.ack(subID: subID, seq: ack))
            }
        case let .gap(subID, fromSeq, toSeq, droppedCount, _):
            guard let subscription = subscriptions[subID] else { return }
            if !subscription.handleGap(fromSeq: fromSeq, toSeq: toSeq, dropped: droppedCount) {
                issueSub(subscription)
            }
        case .heartbeat(let serverTimeMs):
            enqueue(.heartbeat(serverTimeMs: serverTimeMs))
        case .authExpiring:
            refreshTokenFromProvider()
        case let .error(subID, code, _, message):
            logger.warning("Live error \(code) on \(subID): \(message)")
        default:
            // Unknown frames and types that only flow the other way.
            break
        }
    }
}
