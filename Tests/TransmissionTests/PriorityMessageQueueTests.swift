import Testing
import Foundation
@testable import Transmission

// A minimal RemoteNode stub that uses closure-based init (no NIO required).
private func makeStubNode(id: String = "stub") -> RemoteNode {
    RemoteNode(
        nodeID: NodeIdentity(id: id),
        send: { _ in },
        close: {}
    )
}

private func makeMessage(priority: MessagePriority, actorID: String = "actor") -> QueuedMessage {
    let envelope = CallEnvelope(
        callID: CallID(),
        recipient: ActorIdentity(id: actorID),
        target: "test()",
        genericSubs: [],
        args: [],
        priority: priority
    )
    return QueuedMessage(envelope: envelope, node: makeStubNode())
}

@Suite("PriorityMessageQueue ordering and async tests")
struct PriorityMessageQueueTests {

    @Test("Dequeue returns realtime before high before normal before low")
    func priorityOrderingAcrossLevels() async {
        let queue = PriorityMessageQueue()

        await queue.enqueue(makeMessage(priority: .low))
        await queue.enqueue(makeMessage(priority: .normal))
        await queue.enqueue(makeMessage(priority: .high))
        await queue.enqueue(makeMessage(priority: .realtime))

        let first = await queue.dequeue()
        let second = await queue.dequeue()
        let third = await queue.dequeue()
        let fourth = await queue.dequeue()

        #expect(first.priority == .realtime)
        #expect(second.priority == .high)
        #expect(third.priority == .normal)
        #expect(fourth.priority == .low)
    }

    @Test("FIFO ordering within the same priority level")
    func fifoWithinSamePriority() async {
        let queue = PriorityMessageQueue()

        await queue.enqueue(makeMessage(priority: .normal, actorID: "first"))
        await queue.enqueue(makeMessage(priority: .normal, actorID: "second"))
        await queue.enqueue(makeMessage(priority: .normal, actorID: "third"))

        let a = await queue.dequeue()
        let b = await queue.dequeue()
        let c = await queue.dequeue()

        #expect(a.envelope.recipient.id == "first")
        #expect(b.envelope.recipient.id == "second")
        #expect(c.envelope.recipient.id == "third")
    }

    @Test("Count reflects enqueue and dequeue")
    func countTracksEnqueueDequeue() async {
        let queue = PriorityMessageQueue()

        #expect(await queue.count == 0)

        await queue.enqueue(makeMessage(priority: .normal))
        #expect(await queue.count == 1)

        await queue.enqueue(makeMessage(priority: .high))
        #expect(await queue.count == 2)

        _ = await queue.dequeue()
        #expect(await queue.count == 1)

        _ = await queue.dequeue()
        #expect(await queue.count == 0)
    }

    @Test("Dequeue before enqueue suspends then resumes when message arrives")
    func dequeueSuspendsUntilEnqueue() async throws {
        let queue = PriorityMessageQueue()

        // Start a dequeue task that will suspend (queue is empty).
        let dequeuedTask = Task<QueuedMessage, Never> {
            await queue.dequeue()
        }

        // Yield to let the dequeue task reach the continuation.
        try await Task.sleep(for: .milliseconds(50))

        let sent = makeMessage(priority: .high, actorID: "late-arrival")
        await queue.enqueue(sent)

        let received = await dequeuedTask.value
        #expect(received.envelope.recipient.id == "late-arrival")
        #expect(received.priority == .high)
    }

    @Test("High-priority message jumps ahead of queued lower-priority messages")
    func highPriorityJumpsAhead() async {
        let queue = PriorityMessageQueue()

        // Enqueue several low messages first.
        for i in 0..<5 {
            await queue.enqueue(makeMessage(priority: .low, actorID: "low-\(i)"))
        }

        // Then enqueue a single realtime message.
        await queue.enqueue(makeMessage(priority: .realtime, actorID: "urgent"))

        let first = await queue.dequeue()
        #expect(first.priority == .realtime)
        #expect(first.envelope.recipient.id == "urgent")

        // Remaining should all be low.
        for _ in 0..<5 {
            let msg = await queue.dequeue()
            #expect(msg.priority == .low)
        }
    }

    @Test("Mixed priorities dequeue in strict priority order")
    func mixedPrioritiesStrictOrder() async {
        let queue = PriorityMessageQueue()

        // Enqueue in worst-case order (ascending priority last).
        await queue.enqueue(makeMessage(priority: .low))
        await queue.enqueue(makeMessage(priority: .low))
        await queue.enqueue(makeMessage(priority: .normal))
        await queue.enqueue(makeMessage(priority: .normal))
        await queue.enqueue(makeMessage(priority: .high))
        await queue.enqueue(makeMessage(priority: .realtime))

        var dequeued: [MessagePriority] = []
        for _ in 0..<6 {
            let msg = await queue.dequeue()
            dequeued.append(msg.priority)
        }

        // Realtime, then high, then normal x2, then low x2.
        #expect(dequeued == [.realtime, .high, .normal, .normal, .low, .low])
    }
}
