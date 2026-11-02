import Foundation

/// Limits for the calls one connection may run and queue.
public struct DispatchLimits: Sendable {
    public enum Ordering: Sendable {
        /// Calls from one connection to one actor start in arrival order, each
        /// waiting for its predecessor to finish or for `orderingStallLimit`.
        case perActor
        /// No ordering. Every call starts as soon as a slot is free.
        case concurrent
    }

    public var maxConcurrentCalls: Int
    public var maxQueuedCalls: Int
    public var maxQueuedBytes: Int
    public var ordering: Ordering
    /// How long a running call may hold up the next call to the same actor.
    /// This is what keeps a handler that calls back into its own actor from
    /// deadlocking on the ordering queue: past the limit the next call starts,
    /// which matches ordinary actor reentrancy at a suspension point.
    public var orderingStallLimit: Duration

    public init(
        maxConcurrentCalls: Int = 64,
        maxQueuedCalls: Int = 4096,
        maxQueuedBytes: Int = 256 << 20,
        ordering: Ordering = .perActor,
        orderingStallLimit: Duration = .seconds(2)
    ) {
        self.maxConcurrentCalls = maxConcurrentCalls
        self.maxQueuedCalls = maxQueuedCalls
        self.maxQueuedBytes = maxQueuedBytes
        self.ordering = ordering
        self.orderingStallLimit = orderingStallLimit
    }
}

/// The one dispatch model, shared by every transport.
///
/// A transport hands over complete messages with `receive(_:)`, which never
/// blocks and never waits for a handler. One reader task decodes them in
/// arrival order. Replies complete their pending call inline, so a handler
/// waiting on a reply can always make progress. Calls go to a per-actor FIFO
/// and run in their own tasks, bounded by `DispatchLimits`; when the queue is
/// full the caller gets `RemoteCallError.Code.overloaded` straight away.
public final class InboundDispatcher: @unchecked Sendable {
    private struct Queued {
        let call: CallEnvelope
        let bytes: Int
    }

    private struct ActorQueue {
        var pending: [Queued] = []
        var head = 0
        /// The call currently holding this actor's ordering slot.
        var holder: UInt64?
        var stallTimer: Task<Void, Never>?
        var timerHolder: UInt64?

        var isEmpty: Bool { head == pending.count }

        mutating func popFirst() -> Queued {
            let item = pending[head]
            head += 1
            if head == pending.count {
                pending.removeAll(keepingCapacity: true)
                head = 0
            }
            return item
        }
    }

    private weak var system: TransmissionSystem?
    private let node: RemoteNode
    private let limits: DispatchLimits
    private let lock = NSLock()
    private var queues: [String: ActorQueue] = [:]
    private var runnable: [String] = []
    private var running = 0
    private var queuedCalls = 0
    private var queuedBytes = 0
    private var nextToken: UInt64 = 0
    private var tasks: [UInt64: Task<Void, Never>] = [:]
    private var isShutDown = false

    private let messages: AsyncStream<Data>
    private let messageSink: AsyncStream<Data>.Continuation
    private var reader: Task<Void, Never>?

    /// Messages received before `start()` are buffered, so a transport can
    /// finish identifying the connection before the first call runs.
    public init(system: TransmissionSystem, node: RemoteNode, limits: DispatchLimits) {
        self.system = system
        self.node = node
        self.limits = limits
        (messages, messageSink) = AsyncStream.makeStream(of: Data.self)
    }

    public func start() {
        let messages = self.messages
        let task = Task { [weak self] in
            for await message in messages {
                guard let self else { return }
                await self.route(message)
            }
        }
        lock.withLock { reader = task }
    }

    /// Hands over one complete message. Safe from any thread, never blocks.
    public func receive(_ data: Data) {
        system?.metrics.recordMessageReceived(bytes: data.count)
        messageSink.yield(data)
    }

    /// Stops reading, drops queued calls and cancels the running ones.
    public func shutdown() {
        messageSink.finish()
        let (running, timers, reader) = lock.withLock { () -> ([Task<Void, Never>], [Task<Void, Never>], Task<Void, Never>?) in
            isShutDown = true
            let timers = queues.values.compactMap(\.stallTimer)
            queues.removeAll()
            runnable.removeAll()
            queuedCalls = 0
            queuedBytes = 0
            return (Array(tasks.values), timers, self.reader)
        }
        reader?.cancel()
        running.forEach { $0.cancel() }
        timers.forEach { $0.cancel() }
    }

    private func route(_ data: Data) async {
        guard let system else { return }
        let envelope: WireEnvelope
        do {
            envelope = try system.decodeEnvelope(data)
        } catch {
            system.logger.error("Failed to decode message: \(error)")
            return
        }
        switch envelope {
        case .call(let call):
            enqueue(call, bytes: data.count)
        case .reply(let reply):
            await system.pendingCalls.receive(reply: reply)
        case .error(let failure):
            await system.pendingCalls.receive(error: failure)
        case .close:
            await node.close()
        }
    }

    private func enqueue(_ call: CallEnvelope, bytes: Int) {
        let accepted = lock.withLock { () -> Bool in
            guard !isShutDown else { return true }
            guard queuedCalls < limits.maxQueuedCalls, queuedBytes + bytes <= limits.maxQueuedBytes else {
                return false
            }
            let key = orderingKey(for: call)
            var queue = queues[key] ?? ActorQueue()
            let wasEmpty = queue.isEmpty
            queue.pending.append(Queued(call: call, bytes: bytes))
            queues[key] = queue
            queuedCalls += 1
            queuedBytes += bytes
            if wasEmpty, queue.holder == nil { runnable.append(key) }
            armStallTimer(key)
            pump()
            return true
        }
        guard !accepted else { return }
        let node = self.node
        Task { [weak system] in
            let refusal = ErrorEnvelope(
                callID: call.callID, sender: call.recipient, code: .overloaded,
                errorType: nil, message: "Call queue full", payload: nil)
            do {
                try await node.send(.error(refusal))
            } catch {
                system?.logger.debug("Could not send overload refusal: \(error)")
            }
        }
    }

    private func orderingKey(for call: CallEnvelope) -> String {
        switch limits.ordering {
        case .perActor: return call.recipient.id
        case .concurrent: return call.callID.description
        }
    }

    /// Lock held.
    private func pump() {
        while running < limits.maxConcurrentCalls, !runnable.isEmpty {
            let key = runnable.removeFirst()
            guard var queue = queues[key], !queue.isEmpty, queue.holder == nil else { continue }
            let item = queue.popFirst()
            nextToken += 1
            let token = nextToken
            queue.holder = token
            queues[key] = queue
            queuedCalls -= 1
            queuedBytes -= item.bytes
            running += 1
            armStallTimer(key)
            let node = self.node
            tasks[token] = Task { [weak self, weak system] in
                if let system {
                    let reply = await system.dispatch(item.call, from: node)
                    await system.send(reply, for: item.call, to: node)
                }
                self?.finished(key: key, token: token)
            }
        }
    }

    /// Lock held. Starts the stall clock for the holder once something waits behind it.
    private func armStallTimer(_ key: String) {
        guard var queue = queues[key], let holder = queue.holder, !queue.isEmpty,
              queue.timerHolder != holder else { return }
        queue.stallTimer?.cancel()
        queue.timerHolder = holder
        let limit = limits.orderingStallLimit
        queue.stallTimer = Task { [weak self] in
            do {
                try await Task.sleep(for: limit)
            } catch {
                return
            }
            self?.stallExpired(key: key, token: holder)
        }
        queues[key] = queue
    }

    private func stallExpired(key: String, token: UInt64) {
        lock.withLock {
            guard var queue = queues[key], queue.holder == token else { return }
            queue.holder = nil
            queue.stallTimer = nil
            queue.timerHolder = nil
            queues[key] = queue
            if !queue.isEmpty { runnable.append(key) }
            pump()
        }
    }

    private func finished(key: String, token: UInt64) {
        lock.withLock {
            tasks[token] = nil
            running -= 1
            guard !isShutDown else { return }
            if var queue = queues[key], queue.holder == token {
                queue.stallTimer?.cancel()
                queue.stallTimer = nil
                queue.timerHolder = nil
                queue.holder = nil
                if queue.isEmpty {
                    queues[key] = nil
                } else {
                    queues[key] = queue
                    runnable.append(key)
                }
            }
            pump()
        }
    }
}
