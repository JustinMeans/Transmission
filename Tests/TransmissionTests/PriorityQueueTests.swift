import Testing
import Foundation
@testable import Transmission

@Suite("PriorityQueue Tests")
struct PriorityQueueTests {

    @Test("MessagePriority ordering")
    func messagePriorityOrdering() {
        // Test that priority enum values are ordered correctly
        let priorities: [MessagePriority] = [.realtime, .high, .normal, .low]

        #expect(MessagePriority.realtime.rawValue == 0)
        #expect(MessagePriority.high.rawValue == 1)
        #expect(MessagePriority.normal.rawValue == 2)
        #expect(MessagePriority.low.rawValue == 3)

        // Verify ordering from highest to lowest
        let sorted = priorities.sorted { $0.rawValue < $1.rawValue }
        #expect(sorted == [.realtime, .high, .normal, .low])
    }

    @Test("CallEnvelope preserves priority")
    func envelopePreservesPriority() {
        let priorities: [MessagePriority] = [.realtime, .high, .normal, .low]

        for priority in priorities {
            let envelope = makeEnvelope(priority: priority)
            #expect(envelope.priority == priority)
        }
    }

    @Test("PriorityMessageQueue initialization")
    func queueInitialization() async {
        let queue = PriorityMessageQueue()
        let count = await queue.count
        #expect(count == 0)
    }

    // Note: Full integration tests with actual message enqueueing/dequeueing
    // require a real RemoteNode which needs NIO channels. These would be
    // tested in a separate integration test suite with a running server.

    private func makeEnvelope(priority: MessagePriority, id: String = "test") -> CallEnvelope {
        CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: id),
            target: "test()",
            genericSubs: [],
            args: [],
            priority: priority
        )
    }
}
