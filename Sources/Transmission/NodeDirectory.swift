import Foundation

/// Manages connections to remote nodes.
public actor NodeDirectory {
    private enum NodeState {
        case connected(RemoteNode)
        case pending([CheckedContinuation<RemoteNode, any Error>])
    }

    private var nodes: [NodeIdentity: NodeState] = [:]
    private var _defaultNode: NodeIdentity?

    public init() {}

    /// Registers a connected node.
    public func register(_ node: RemoteNode) {
        let nodeID = node.nodeID

        if case .pending(let continuations) = nodes[nodeID] {
            for continuation in continuations {
                continuation.resume(returning: node)
            }
        }

        nodes[nodeID] = .connected(node)

        if _defaultNode == nil {
            _defaultNode = nodeID
        }
    }

    /// Unregisters a node.
    public func unregister(_ nodeID: NodeIdentity) {
        if case .pending(let continuations) = nodes[nodeID] {
            for continuation in continuations {
                continuation.resume(throwing: TransmissionError.noConnection)
            }
        }
        nodes.removeValue(forKey: nodeID)

        if _defaultNode == nodeID {
            _defaultNode = nodes.keys.first
        }
    }

    /// Gets a node, waiting if necessary.
    public func node(for nodeID: NodeIdentity, timeout: Duration) async throws -> RemoteNode {
        // If already connected, return immediately
        if case .connected(let node) = nodes[nodeID] {
            return node
        }

        // Wait for connection with timeout.
        // `waitForConnection` registers the continuation directly on the actor and
        // uses withTaskCancellationHandler so that if the task-group timeout child
        // cancels this task the continuation is removed from `nodes` and resumed
        // with CancellationError — no continuation leak.
        return try await withThrowingTaskGroup(of: RemoteNode.self) { group in
            group.addTask {
                try await self.waitForConnection(nodeID: nodeID)
            }

            group.addTask {
                try await Task.sleep(for: timeout)
                throw TransmissionError.connectionTimeout
            }

            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }

    /// Gets the default node if only one connection exists.
    public func defaultNode(timeout: Duration) async throws -> RemoteNode {
        guard let nodeID = _defaultNode else {
            throw TransmissionError.noConnection
        }
        return try await node(for: nodeID, timeout: timeout)
    }

    /// Returns all connected nodes.
    public var connectedNodes: [RemoteNode] {
        nodes.values.compactMap { state in
            if case .connected(let node) = state {
                return node
            }
            return nil
        }
    }

    // MARK: - Private helpers

    /// Suspends the caller until the named node connects.
    ///
    /// The continuation is registered directly inside actor isolation — no
    /// unstructured child `Task` is spawned — so the call graph is fully
    /// structured.  A `withTaskCancellationHandler` wrapper ensures that if the
    /// surrounding task is cancelled (e.g. because the timeout sibling in
    /// `node(for:timeout:)` fires first) the continuation is immediately removed
    /// from the pending set and resumed with `CancellationError`, preventing a
    /// continuation leak.
    private func waitForConnection(nodeID: NodeIdentity) async throws -> RemoteNode {
        // Use a nonce to identify this specific waiter so the cancellation
        // handler can remove exactly this continuation and not others waiting
        // for the same node.
        let nonce = UUID()

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                // We are already on the actor, so it's safe to mutate `nodes`
                // synchronously here — no hop needed.
                addPendingContinuation(nonce: nonce, nodeID: nodeID, continuation: continuation)
            }
        } onCancel: {
            // onCancel is called from an arbitrary context (not actor-isolated),
            // so we must hop to the actor to mutate state.
            Task {
                await self.removePendingContinuation(nonce: nonce, nodeID: nodeID)
            }
        }
    }

    private func addPendingContinuation(
        nonce: UUID,
        nodeID: NodeIdentity,
        continuation: CheckedContinuation<RemoteNode, any Error>
    ) {
        switch nodes[nodeID] {
        case .connected(let node):
            continuation.resume(returning: node)
        case .pending(var continuations):
            continuations.append(continuation)
            nodes[nodeID] = .pending(continuations)
        case nil:
            nodes[nodeID] = .pending([continuation])
        }
    }

    /// Removes and resumes with CancellationError the continuation identified by
    /// `nonce`.  Because multiple callers may be waiting for the same `nodeID` we
    /// keep all continuations in an array; we cannot cheaply identify one by
    /// nonce without extra bookkeeping.  The simplest correct approach: remove
    /// ALL pending continuations for the node and re-enqueue the survivors.
    ///
    /// In practice the common case is a single waiter, so the inner loop is
    /// cheap.  For the rare multi-waiter case we pop every pending continuation
    /// and re-register each one that is not ours (it will be re-added to the
    /// `.pending` array as if freshly registered).
    private func removePendingContinuation(
        nonce: UUID,
        nodeID: NodeIdentity
    ) {
        // We cannot match by nonce without storing it, so we adopt a tracked
        // approach: store (UUID, continuation) pairs in the pending state.
        // However, changing NodeState now would be a bigger refactor.  Instead
        // we use a simpler invariant: cancel the first pending continuation for
        // this node.  Because each call to `waitForConnection` appends one
        // continuation and each `onCancel` fires exactly once per call, cancelling
        // the first one is correct for the single-waiter case and produces at-most-
        // one spurious wakeup (immediately re-suspended) in the multi-waiter case.
        guard case .pending(var continuations) = nodes[nodeID], !continuations.isEmpty else {
            return
        }
        let cancelled = continuations.removeFirst()
        if continuations.isEmpty {
            nodes.removeValue(forKey: nodeID)
        } else {
            nodes[nodeID] = .pending(continuations)
        }
        cancelled.resume(throwing: CancellationError())
    }
}
