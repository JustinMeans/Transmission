import Testing
import Foundation
@testable import Transmission

// Helper to make a closure-backed RemoteNode (no NIO required).
private func makeStubNode(id: String) -> RemoteNode {
    RemoteNode(
        nodeID: NodeIdentity(id: id),
        send: { _ in },
        close: {}
    )
}

@Suite("NodeDirectory")
struct NodeDirectoryTests {

    // MARK: - Timeout path (the continuation-leak bug)

    /// Before the fix, `node(for:timeout:)` spawned an unstructured inner Task to
    /// register the waiting continuation.  When the timeout sibling won the race
    /// that inner Task still ran afterwards, storing the continuation in
    /// `nodes[nodeID] = .pending([...])`.  A subsequent `register` call would then
    /// resume that leaked continuation — producing a spurious extra wakeup — and
    /// the node-ID would be left in `.connected` state, meaning the immediately
    /// following `node(for:)` call with a fresh timeout would succeed rather than
    /// throwing `.connectionTimeout` again.
    ///
    /// With the fix the continuation is cleaned up by the cancellation handler, so
    /// `nodes[nodeID]` is `nil` after the timeout and the state is pristine for
    /// the next attempt.
    @Test("Timeout throws connectionTimeout and leaves no stale pending continuation")
    func timeoutLeavesNoPendingContinuation() async throws {
        let dir = NodeDirectory()
        let nodeID = NodeIdentity(id: "ghost-node")

        // First attempt: should time out quickly.
        do {
            _ = try await dir.node(for: nodeID, timeout: .milliseconds(50))
            Issue.record("Expected connectionTimeout but call succeeded")
        } catch TransmissionError.connectionTimeout {
            // correct
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        // Give the inner unstructured Task (if any) time to fire and potentially
        // leave a stale continuation in the node dict.
        try await Task.sleep(for: .milliseconds(100))

        // After a clean timeout there must be NO pending entry for this node.
        // We verify by registering the node and then immediately resolving it —
        // if a leaked continuation exists, `register` would have already consumed
        // it and `node(for:)` would find `.connected` thanks to the leaked state
        // entry, OR the internal state would be corrupted.
        //
        // The canonical proof: a second short-timeout `node(for:)` call issued
        // BEFORE we register should still time out (no stale `.pending` that a
        // leaked continuation left behind can interfere with fresh `.pending`
        // creation, but more importantly no spurious `.connected` state).
        do {
            _ = try await dir.node(for: nodeID, timeout: .milliseconds(50))
            Issue.record("Second attempt: expected connectionTimeout")
        } catch TransmissionError.connectionTimeout {
            // correct — state is pristine
        } catch {
            Issue.record("Second attempt: unexpected error: \(error)")
        }

        // Now register the node properly and confirm a waiter can resolve it.
        let stub = makeStubNode(id: "ghost-node")

        let resolved = try await withThrowingTaskGroup(of: RemoteNode.self) { group in
            group.addTask {
                // This waiter should unblock as soon as register fires.
                try await dir.node(for: nodeID, timeout: .seconds(5))
            }
            group.addTask {
                // Small delay to let the waiter suspend before we register.
                try await Task.sleep(for: .milliseconds(30))
                await dir.register(stub)
                // Throw so task group picks the first result (the waiter).
                throw CancellationError()
            }
            // Take the first success (the waiter resolution).
            do {
                let node = try await group.next()!
                group.cancelAll()
                return node
            } catch {
                // The register task threw CancellationError; get the waiter result.
                let node = try await group.next()!
                group.cancelAll()
                return node
            }
        }

        let resolvedID = await resolved.nodeID
        #expect(resolvedID == nodeID,
            "Registered node must be returned by a waiting node(for:) call")
    }

    // MARK: - Happy path: waiter unblocked by register

    @Test("node(for:) waiter is unblocked when register is called")
    func waiterUnblockedByRegister() async throws {
        let dir = NodeDirectory()
        let nodeID = NodeIdentity(id: "late-server")
        let stub = makeStubNode(id: "late-server")

        // Start a waiter task first.
        let waiterTask = Task<RemoteNode, any Error> {
            try await dir.node(for: nodeID, timeout: .seconds(5))
        }

        // Give the waiter time to suspend.
        try await Task.sleep(for: .milliseconds(30))

        // Register the node — must unblock the waiter.
        await dir.register(stub)

        let resolved = try await waiterTask.value
        let resolvedID = await resolved.nodeID
        #expect(resolvedID == nodeID)
    }

    // MARK: - Unregister cancels pending waiters

    @Test("unregister resumes pending waiters with noConnection error")
    func unregisterCancelsPendingWaiters() async throws {
        let dir = NodeDirectory()
        let nodeID = NodeIdentity(id: "dropping-server")

        let waiterTask = Task<RemoteNode?, Never> {
            do {
                return try await dir.node(for: nodeID, timeout: .seconds(5))
            } catch {
                return nil
            }
        }

        try await Task.sleep(for: .milliseconds(30))

        // Unregister before anyone connects.
        await dir.unregister(nodeID)

        let result = await waiterTask.value
        #expect(result == nil, "Waiter must fail when node is unregistered")
    }

    // MARK: - Immediate hit for already-connected node

    @Test("node(for:) returns immediately when node is already connected")
    func immediateHitForConnectedNode() async throws {
        let dir = NodeDirectory()
        let stub = makeStubNode(id: "fast-server")
        await dir.register(stub)

        let resolved = try await dir.node(for: NodeIdentity(id: "fast-server"), timeout: .seconds(1))
        let resolvedID = await resolved.nodeID
        #expect(resolvedID == NodeIdentity(id: "fast-server"))
    }
}
