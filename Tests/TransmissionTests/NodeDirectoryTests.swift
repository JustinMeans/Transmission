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

    // MARK: - Multi-waiter cancellation correctness

    /// Regression test for the bug where `removePendingContinuation` cancelled the
    /// FIRST pending continuation regardless of nonce, so when two callers waited
    /// for the same node and the SECOND one timed out, the FIRST one (which still
    /// had time remaining) was wrongly cancelled instead.
    ///
    /// With the fix, each pending waiter is stored as a (nonce, continuation) pair
    /// so the cancellation handler removes exactly the timed-out entry by identity.
    @Test("Only the timed-out waiter is cancelled when multiple waiters share a node")
    func multiWaiterCancellationCancelsOnlyTimedOutEntry() async throws {
        let dir = NodeDirectory()
        let nodeID = NodeIdentity(id: "shared-node")

        // Waiter A: long timeout — must NOT be cancelled.
        let waiterA = Task<RemoteNode?, Never> {
            do {
                return try await dir.node(for: nodeID, timeout: .seconds(10))
            } catch {
                // Any error (including spurious CancellationError from the bug)
                // surfaces as nil so the #expect below can catch it.
                return nil
            }
        }

        // Give waiter A time to register its continuation first.
        try await Task.sleep(for: .milliseconds(30))

        // Waiter B: very short timeout — must time out with connectionTimeout.
        let waiterBError: (any Error)? = await {
            do {
                _ = try await dir.node(for: nodeID, timeout: .milliseconds(50))
                return nil
            } catch {
                return error
            }
        }()

        // Waiter B must have thrown connectionTimeout.
        if let te = waiterBError as? TransmissionError, case .connectionTimeout = te {
            // correct
        } else {
            Issue.record("Waiter B must throw TransmissionError.connectionTimeout, got \(String(describing: waiterBError))")
        }

        // Allow any async cleanup (onCancel Task hop) to settle.
        try await Task.sleep(for: .milliseconds(100))

        // Now register the node. Waiter A must be unblocked.
        let stub = makeStubNode(id: "shared-node")
        await dir.register(stub)

        let resolved = await waiterA.value
        #expect(resolved != nil,
            "Waiter A must not have been cancelled: its timeout had not expired")

        if let node = resolved {
            let resolvedID = await node.nodeID
            #expect(resolvedID == nodeID,
                "Waiter A must resolve to the registered node")
        }

        waiterA.cancel()
    }

    // MARK: - defaultNode happy path

    @Test("defaultNode returns the single registered node")
    func defaultNodeReturnsSingleRegisteredNode() async throws {
        let dir = NodeDirectory()
        let stub = makeStubNode(id: "solo")
        await dir.register(stub)

        let resolved = try await dir.defaultNode(timeout: .seconds(1))
        let resolvedID = await resolved.nodeID
        #expect(resolvedID == NodeIdentity(id: "solo"))
    }

    @Test("defaultNode throws noConnection when directory is empty")
    func defaultNodeThrowsNoConnectionWhenEmpty() async {
        let dir = NodeDirectory()
        do {
            _ = try await dir.defaultNode(timeout: .seconds(1))
            Issue.record("Expected noConnection but call succeeded")
        } catch TransmissionError.noConnection {
            // correct
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("defaultNode throws noConnection after the only node is unregistered")
    func defaultNodeThrowsAfterOnlyNodeUnregisters() async {
        let dir = NodeDirectory()
        let stub = makeStubNode(id: "ephemeral")
        await dir.register(stub)
        await dir.unregister(NodeIdentity(id: "ephemeral"))

        do {
            _ = try await dir.defaultNode(timeout: .milliseconds(50))
            Issue.record("Expected noConnection but call succeeded")
        } catch TransmissionError.noConnection {
            // correct — _defaultNode must be nil after the only node unregisters
        } catch TransmissionError.connectionTimeout {
            Issue.record("Got connectionTimeout; _defaultNode was not cleared by unregister")
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    // MARK: - connectedNodes

    @Test("connectedNodes is empty when directory is empty")
    func connectedNodesEmptyWhenDirectoryEmpty() async {
        let dir = NodeDirectory()
        let nodes = await dir.connectedNodes
        #expect(nodes.isEmpty)
    }

    @Test("connectedNodes returns all registered nodes")
    func connectedNodesReturnsAllRegistered() async {
        let dir = NodeDirectory()
        await dir.register(makeStubNode(id: "alpha"))
        await dir.register(makeStubNode(id: "beta"))
        await dir.register(makeStubNode(id: "gamma"))

        let nodes = await dir.connectedNodes
        let ids = Set(await withTaskGroup(of: NodeIdentity.self) { group in
            for node in nodes {
                group.addTask { await node.nodeID }
            }
            var result: [NodeIdentity] = []
            for await id in group { result.append(id) }
            return result
        })
        #expect(ids.count == 3)
        #expect(ids.contains(NodeIdentity(id: "alpha")))
        #expect(ids.contains(NodeIdentity(id: "beta")))
        #expect(ids.contains(NodeIdentity(id: "gamma")))
    }

    @Test("connectedNodes excludes unregistered nodes")
    func connectedNodesExcludesUnregistered() async {
        let dir = NodeDirectory()
        await dir.register(makeStubNode(id: "keep"))
        await dir.register(makeStubNode(id: "remove"))
        await dir.unregister(NodeIdentity(id: "remove"))

        let nodes = await dir.connectedNodes
        #expect(nodes.count == 1)
        let id = await nodes.first?.nodeID
        #expect(id == NodeIdentity(id: "keep"))
    }
}
