import Foundation

public actor PriorityMessageQueue {
    private var queues: [MessagePriority: [QueuedMessage]] = [
        .realtime: [],
        .high: [],
        .normal: [],
        .low: []
    ]

    private var continuations: [CheckedContinuation<QueuedMessage, Never>] = []

    public init() {}

    public func enqueue(_ message: QueuedMessage) {
        if let continuation = continuations.first {
            continuations.removeFirst()
            continuation.resume(returning: message)
            return
        }

        queues[message.priority, default: []].append(message)
    }

    public func dequeue() async -> QueuedMessage {
        for priority in [MessagePriority.realtime, .high, .normal, .low] {
            if var queue = queues[priority], !queue.isEmpty {
                let message = queue.removeFirst()
                queues[priority] = queue
                return message
            }
        }

        return await withCheckedContinuation { continuation in
            continuations.append(continuation)
        }
    }

    public var count: Int {
        queues.values.reduce(0) { $0 + $1.count }
    }
}

public struct QueuedMessage: Sendable {
    public let envelope: CallEnvelope
    public let node: RemoteNode

    public var priority: MessagePriority {
        envelope.priority
    }

    public init(envelope: CallEnvelope, node: RemoteNode) {
        self.envelope = envelope
        self.node = node
    }
}
