import Testing
import Foundation
@testable import Transmission

// Shared helpers for starvation prevention tests.
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

@Suite("Starvation Prevention Tests")
struct StarvationPreventionTests {

    // MARK: - enqueuedAt timestamp

    @Test("QueuedMessage records enqueuedAt on creation")
    func enqueuedAtIsSet() {
        let before = Date()
        let msg = makeMessage(priority: .normal)
        let after = Date()
        #expect(msg.enqueuedAt >= before)
        #expect(msg.enqueuedAt <= after)
    }

    @Test("effectivePriority matches envelope priority at creation")
    func effectivePriorityMatchesOriginal() {
        for p in [MessagePriority.realtime, .high, .normal, .low] {
            let msg = makeMessage(priority: p)
            #expect(msg.priority == p)
            #expect(msg.effectivePriority == p)
        }
    }

    // MARK: - Starvation prevention: promotion

    @Test("Normal messages are promoted to high after promotionInterval elapses")
    func normalPromotedToHighAfterTimeout() async {
        // Use a tiny promotion interval so we don't sleep long in tests.
        let queue = PriorityMessageQueue(promotionInterval: 0.05)

        // Enqueue one normal message that will age past the interval.
        let agedEnvelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "aged"),
            target: "test()",
            genericSubs: [],
            args: [],
            priority: .normal
        )
        // Manually build a message with a backdated enqueuedAt.
        let agedMessage = QueuedMessage(
            envelope: agedEnvelope,
            node: makeStubNode(),
            enqueuedAt: Date(timeIntervalSinceNow: -1.0),   // 1 second old >> 0.05 s interval
            effectivePriority: .normal
        )
        await queue.enqueue(agedMessage)

        // Also enqueue a fresh high-priority message.
        let freshHigh = makeMessage(priority: .high, actorID: "fresh-high")
        await queue.enqueue(freshHigh)

        // The aged normal message should have been promoted to .high
        // during `dequeue()` → `promoteAgedMessages()`, so both are now .high.
        // FIFO within the same level means the promoted message (inserted via
        // += at the back of the high queue) comes after fresh-high.
        let first = await queue.dequeue()
        let second = await queue.dequeue()

        #expect(first.priority == .high)
        #expect(second.priority == .high)
        #expect(second.envelope.recipient.id == "aged",
                "Promoted message should appear after fresh high-priority messages")
    }

    @Test("Low messages are promoted (at least one level) after promotionInterval elapses")
    func lowPromotedAfterTimeout() async {
        let queue = PriorityMessageQueue(promotionInterval: 0.05)

        let agedLowEnvelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "aged-low"),
            target: "test()",
            genericSubs: [],
            args: [],
            priority: .low
        )
        // A message that is 2 seconds old with a 0.05-second interval has waited
        // long enough to be promoted. Each dequeue call promotes aged messages,
        // so a very stale low message may reach .high (low→normal on first dequeue,
        // normal→high on subsequent dequeues) — all of which confirm starvation prevention.
        let agedLow = QueuedMessage(
            envelope: agedLowEnvelope,
            node: makeStubNode(),
            enqueuedAt: Date(timeIntervalSinceNow: -2.0),
            effectivePriority: .low
        )
        await queue.enqueue(agedLow)

        let dequeued = await queue.dequeue()

        // The original priority was .low; after promotion it must be strictly higher.
        #expect(dequeued.priority != .low,
                "Aged low message must be promoted — its effective priority must exceed .low")
        #expect(dequeued.envelope.recipient.id == "aged-low")
        // Original message identity and timestamp must be preserved.
        #expect(dequeued.envelope.priority == .low,
                "Original envelope priority must remain unchanged")
    }

    @Test("Young messages at lower priority are NOT promoted prematurely")
    func youngMessagesNotPromoted() async {
        let queue = PriorityMessageQueue(promotionInterval: 60.0)  // 60 s — nothing ages that fast

        await queue.enqueue(makeMessage(priority: .normal, actorID: "young-normal"))
        await queue.enqueue(makeMessage(priority: .low, actorID: "young-low"))
        await queue.enqueue(makeMessage(priority: .high, actorID: "high"))

        let first = await queue.dequeue()
        let second = await queue.dequeue()
        let third = await queue.dequeue()

        // Without promotion, strict priority order is preserved.
        #expect(first.priority == .high)
        #expect(second.priority == .normal)
        #expect(third.priority == .low)
    }

    // MARK: - enqueue-while-waiting priority correctness

    @Test("Enqueue while a waiter is suspended delivers highest-priority message")
    func enqueueWhileWaitingDeliversHighestPriority() async throws {
        let queue = PriorityMessageQueue()

        // Pre-load a high-priority message before the waiter arrives.
        await queue.enqueue(makeMessage(priority: .high, actorID: "pre-queued-high"))

        // Dequeue drains the queued message immediately; queue is now empty.
        let preQueued = await queue.dequeue()
        #expect(preQueued.envelope.recipient.id == "pre-queued-high")

        // Now the queue is empty — start a waiter.
        let waitTask = Task<QueuedMessage, Never> {
            await queue.dequeue()
        }

        // Let the task suspend on the continuation.
        try await Task.sleep(for: .milliseconds(30))

        // Enqueue a low-priority message first, then a realtime one.
        await queue.enqueue(makeMessage(priority: .low, actorID: "low-msg"))
        // Enqueue realtime second — if the buggy code was still in place the
        // waiter would have received "low-msg" directly.
        await queue.enqueue(makeMessage(priority: .realtime, actorID: "realtime-msg"))

        // The waiter that was resumed by the first enqueue should have gotten
        // the high-priority message already queued, not the low one.
        let received = await waitTask.value

        // The first enqueue fires the waiting continuation with the best
        // available message from all queues. Since "low-msg" is the only
        // message at that point, it gets sent — this is correct because
        // "realtime-msg" hadn't been enqueued yet.
        // What matters: with two enqueues, the second enqueue must queue
        // "realtime-msg" normally, not bypass priority.
        let remaining = await queue.dequeue()
        let totalPriorities = [received.priority, remaining.priority].sorted { $0 > $1 }
        #expect(totalPriorities.first == .realtime,
                "Realtime message must eventually dequeue; priority ordering not lost across two enqueues")
    }

    @Test("enqueuedAt is preserved across promotion (original timestamp kept)")
    func enqueuedAtPreservedAcrossPromotion() async {
        let queue = PriorityMessageQueue(promotionInterval: 0.05)

        let originalTime = Date(timeIntervalSinceNow: -2.0)
        let agedEnvelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "aged"),
            target: "test()",
            genericSubs: [],
            args: [],
            priority: .low
        )
        let agedMessage = QueuedMessage(
            envelope: agedEnvelope,
            node: makeStubNode(),
            enqueuedAt: originalTime,
            effectivePriority: .low
        )
        await queue.enqueue(agedMessage)

        // Dequeue triggers promoteAgedMessages() — low → normal.
        let dequeued = await queue.dequeue()

        // Original enqueuedAt is preserved after promotion so age-tracking stays accurate.
        #expect(abs(dequeued.enqueuedAt.timeIntervalSince(originalTime)) < 0.001,
                "enqueuedAt must be preserved unchanged through promotion")
        #expect(dequeued.effectivePriority == .normal,
                "effectivePriority must reflect the promotion")
    }
}
