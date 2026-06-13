import Testing
import Foundation
@testable import Transmission

// Helper to create a stub RemoteNode backed only by closures (no NIO).
private func makeNode(id: String, send: @escaping @Sendable (Data) async throws -> Void = { _ in }) -> RemoteNode {
    RemoteNode(
        nodeID: NodeIdentity(id: id),
        send: send,
        close: {}
    )
}

private func makeEnvelope(recipient: String = "actor") -> CallEnvelope {
    CallEnvelope(
        callID: CallID(),
        recipient: ActorIdentity(id: recipient),
        target: "test()",
        genericSubs: [],
        args: [],
        priority: .normal
    )
}

@Suite("PendingCalls per-node cancellation")
struct PendingCallsTests {

    // MARK: - cancelAll only affects the target node

    @Test("cancelAll for disconnected node fails only that node's calls")
    func cancelAllOnlyTargetsCorrectNode() async throws {
        let pendingCalls = PendingCalls()

        let nodeA = makeNode(id: "nodeA") { _ in
            // Simulate a hung send — never resolves so the call stays pending.
            try await Task.sleep(for: .seconds(60))
        }
        let nodeB = makeNode(id: "nodeB") { _ in
            try await Task.sleep(for: .seconds(60))
        }

        let envelopeA = makeEnvelope(recipient: "actorA")
        let envelopeB = makeEnvelope(recipient: "actorB")

        // Start two concurrent calls, one to each node.
        // Both will suspend waiting for a reply (the send closures hang).
        let callATask = Task<Data?, Never> {
            do {
                return try await pendingCalls.send(envelope: envelopeA, via: nodeA, timeout: .seconds(30))
            } catch {
                return nil  // Expected for nodeA when cancelled
            }
        }
        let callBTask = Task<Data?, Never> {
            do {
                return try await pendingCalls.send(envelope: envelopeB, via: nodeB, timeout: .seconds(30))
            } catch {
                return nil
            }
        }

        // Let both tasks register their continuations before we cancel.
        try await Task.sleep(for: .milliseconds(50))

        // Cancel only nodeA.
        await pendingCalls.cancelAll(for: NodeIdentity(id: "nodeA"))

        // callA should fail immediately (continuation resumed with error).
        let resultA = await callATask.value
        #expect(resultA == nil, "Call to nodeA must fail when nodeA is cancelled")

        // callB must still be pending — deliver a synthetic reply so the task
        // can finish and we can confirm it succeeded.
        let replyB = ReplyEnvelope(
            callID: envelopeB.callID,
            sender: nil,
            value: "ok".data(using: .utf8)!
        )
        await pendingCalls.receive(reply: replyB)

        let resultB = await callBTask.value
        #expect(resultB == "ok".data(using: .utf8)!, "Call to nodeB must survive nodeA cancellation")

        callATask.cancel()
        callBTask.cancel()
    }

    // MARK: - cancelAll for unknown node is a no-op

    @Test("cancelAll for unknown node does not cancel any pending call")
    func cancelAllUnknownNodeIsNoop() async throws {
        let pendingCalls = PendingCalls()

        let node = makeNode(id: "realNode") { _ in
            try await Task.sleep(for: .seconds(60))
        }
        let envelope = makeEnvelope()

        let callTask = Task<Data?, Never> {
            do {
                return try await pendingCalls.send(envelope: envelope, via: node, timeout: .seconds(30))
            } catch {
                return nil
            }
        }

        try await Task.sleep(for: .milliseconds(50))

        // Cancel a completely different node ID.
        await pendingCalls.cancelAll(for: NodeIdentity(id: "ghostNode"))

        // The real call must still be pending; deliver its reply.
        let reply = ReplyEnvelope(
            callID: envelope.callID,
            sender: nil,
            value: "alive".data(using: .utf8)!
        )
        await pendingCalls.receive(reply: reply)

        let result = await callTask.value
        #expect(result == "alive".data(using: .utf8)!, "Call must not be affected by cancelAll for an unrelated node")

        callTask.cancel()
    }

    // MARK: - receive cleans up node tracking

    @Test("Successful receive cleans up callNodeIDs so a subsequent cancelAll is a no-op")
    func receiveRemovesNodeTracking() async throws {
        let pendingCalls = PendingCalls()
        let node = makeNode(id: "nodeX")
        let envelope = makeEnvelope()

        // Deliver the reply before the send is dispatched so there is no
        // continuation to resume — the receive is simply a no-op.
        // Instead, fire a send, let it register, then reply immediately.
        let callTask = Task<Data?, Never> {
            do {
                return try await pendingCalls.send(envelope: envelope, via: node, timeout: .seconds(30))
            } catch {
                return nil
            }
        }

        try await Task.sleep(for: .milliseconds(50))

        let reply = ReplyEnvelope(
            callID: envelope.callID,
            sender: nil,
            value: "done".data(using: .utf8)!
        )
        await pendingCalls.receive(reply: reply)

        let result = await callTask.value
        #expect(result == "done".data(using: .utf8)!)

        // After the reply, cancelAll should find no tracked calls for nodeX.
        // This mainly verifies the internal map is cleaned up (no assertion needed
        // beyond confirming this doesn't crash or hang).
        await pendingCalls.cancelAll(for: NodeIdentity(id: "nodeX"))
    }

    // MARK: - cancelAll for same node twice is safe

    @Test("cancelAll called twice for the same node does not crash")
    func cancelAllTwiceIsSafe() async throws {
        let pendingCalls = PendingCalls()
        let node = makeNode(id: "dup") { _ in
            try await Task.sleep(for: .seconds(60))
        }
        let envelope = makeEnvelope()

        let callTask = Task<Data?, Never> {
            do {
                return try await pendingCalls.send(envelope: envelope, via: node, timeout: .seconds(30))
            } catch {
                return nil
            }
        }

        try await Task.sleep(for: .milliseconds(50))

        await pendingCalls.cancelAll(for: NodeIdentity(id: "dup"))
        // Second call must not crash or double-resume a continuation.
        await pendingCalls.cancelAll(for: NodeIdentity(id: "dup"))

        let result = await callTask.value
        #expect(result == nil, "Call must have been cancelled by the first cancelAll")

        callTask.cancel()
    }
}
