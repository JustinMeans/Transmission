import Foundation

public actor PriorityMessageQueue {
    private var queues: [MessagePriority: [QueuedMessage]] = [
        .realtime: [],
        .high: [],
        .normal: [],
        .low: []
    ]

    private var continuations: [CheckedContinuation<QueuedMessage, Never>] = []

    /// Maximum age (in seconds) a message may wait before being promoted
    /// one priority level to prevent indefinite starvation.
    public let promotionInterval: TimeInterval

    public init(promotionInterval: TimeInterval = 5.0) {
        self.promotionInterval = promotionInterval
    }

    public func enqueue(_ message: QueuedMessage) {
        if continuations.isEmpty {
            queues[message.priority, default: []].append(message)
        } else {
            // A waiter is suspended. Add the message to its queue first,
            // then pick the highest-priority message (respecting any already
            // queued messages) so priority ordering is never bypassed.
            queues[message.priority, default: []].append(message)
            let best = dequeueBest()!
            let continuation = continuations.removeFirst()
            continuation.resume(returning: best)
        }
    }

    public func dequeue() async -> QueuedMessage {
        promoteAgedMessages()
        if let message = dequeueBest() {
            return message
        }

        return await withCheckedContinuation { continuation in
            continuations.append(continuation)
        }
    }

    public var count: Int {
        queues.values.reduce(0) { $0 + $1.count }
    }

    // MARK: - Private helpers

    /// Pull the highest-priority non-empty message from queues.
    private func dequeueBest() -> QueuedMessage? {
        for priority in [MessagePriority.realtime, .high, .normal, .low] {
            if var queue = queues[priority], !queue.isEmpty {
                let message = queue.removeFirst()
                queues[priority] = queue
                return message
            }
        }
        return nil
    }

    /// Promote messages that have waited longer than `promotionInterval`.
    /// Moves each qualifying message up one priority level, preventing
    /// indefinite starvation under sustained high-priority load.
    private func promoteAgedMessages() {
        let now = Date()
        let promotionOrder: [(from: MessagePriority, to: MessagePriority)] = [
            (.normal, .high),
            (.low, .normal),
        ]
        for (fromPriority, toPriority) in promotionOrder {
            guard let fromQueue = queues[fromPriority], !fromQueue.isEmpty else { continue }
            var promoted: [QueuedMessage] = []
            var remaining: [QueuedMessage] = []
            for message in fromQueue {
                if now.timeIntervalSince(message.enqueuedAt) >= promotionInterval {
                    promoted.append(message.promoted(to: toPriority))
                } else {
                    remaining.append(message)
                }
            }
            if !promoted.isEmpty {
                queues[fromPriority] = remaining
                queues[toPriority, default: []] += promoted
            }
        }
    }
}

public struct QueuedMessage: Sendable {
    public let envelope: CallEnvelope
    public let node: RemoteNode
    /// The time this message was first enqueued (preserved across promotions).
    public let enqueuedAt: Date
    /// The effective priority after any starvation-prevention promotions.
    public let effectivePriority: MessagePriority

    public var priority: MessagePriority {
        effectivePriority
    }

    public init(envelope: CallEnvelope, node: RemoteNode) {
        self.envelope = envelope
        self.node = node
        self.enqueuedAt = Date()
        self.effectivePriority = envelope.priority
    }

    // Internal init used by promotion logic and tests (accessible via @testable import).
    init(
        envelope: CallEnvelope,
        node: RemoteNode,
        enqueuedAt: Date,
        effectivePriority: MessagePriority
    ) {
        self.envelope = envelope
        self.node = node
        self.enqueuedAt = enqueuedAt
        self.effectivePriority = effectivePriority
    }

    func promoted(to newPriority: MessagePriority) -> QueuedMessage {
        QueuedMessage(
            envelope: envelope,
            node: node,
            enqueuedAt: enqueuedAt,
            effectivePriority: newPriority
        )
    }
}
