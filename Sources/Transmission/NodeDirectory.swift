import Foundation

/// Manages connections to remote nodes.
public actor NodeDirectory {
    private enum NodeState {
        case connected(RemoteNode)
        /// Each pending waiter is stored with its unique nonce so the
        /// cancellation handler can remove exactly the right continuation
        /// even when multiple callers are waiting for the same node.
        case pending([(nonce: UUID, continuation: CheckedContinuation<RemoteNode, any Error>)])
    }

    private var nodes: [NodeIdentity: NodeState] = [:]
    private var _defaultNode: NodeIdentity?
    /// Lets a client address its server as `NodeIdentity.server` whatever id the
    /// server actually reports.
    private var upstream: NodeIdentity?

    public init() {}

    /// Registers a connected node.
    public func register(_ node: RemoteNode) {
        _ = adopt(node)
    }

    /// Registers the node a client dialled, so `NodeIdentity.server` reaches it.
    func registerUpstream(_ node: RemoteNode) -> RemoteNode? {
        let replaced = adopt(node)
        guard node.nodeID != .server else { return replaced }
        upstream = node.nodeID
        if case .pending(let waiters) = nodes[.server] {
            nodes[.server] = nil
            for waiter in waiters {
                waiter.continuation.resume(returning: node)
            }
        }
        return replaced
    }

    /// Registers `node`, returning the connection it displaced, if any.
    @discardableResult
    func adopt(_ node: RemoteNode) -> RemoteNode? {
        let nodeID = node.nodeID
        var replaced: RemoteNode?
        if case .connected(let existing) = nodes[nodeID], existing !== node {
            replaced = existing
        }

        if case .pending(let waiters) = nodes[nodeID] {
            for waiter in waiters {
                waiter.continuation.resume(returning: node)
            }
        }

        nodes[nodeID] = .connected(node)

        if _defaultNode == nil {
            _defaultNode = nodeID
        }
        return replaced
    }

    /// Unregisters this exact connection. A no-op when a newer connection has
    /// already taken the id, which is what makes a late close from an old
    /// socket harmless. Returns whether anything was removed.
    @discardableResult
    public func unregister(_ node: RemoteNode) -> Bool {
        guard case .connected(let current) = nodes[node.nodeID], current === node else {
            return false
        }
        unregister(node.nodeID)
        return true
    }

    /// Unregisters whatever connection holds this id. Prefer `unregister(_: RemoteNode)`
    /// from connection teardown.
    public func unregister(_ nodeID: NodeIdentity) {
        if case .pending(let waiters) = nodes[nodeID] {
            for waiter in waiters {
                waiter.continuation.resume(throwing: TransmissionError.noConnection)
            }
        }
        nodes.removeValue(forKey: nodeID)

        if _defaultNode == nodeID {
            _defaultNode = nodes.keys.first
        }
        if upstream == nodeID {
            upstream = nil
        }
    }

    /// Gets a node, waiting if necessary.
    public func node(for requested: NodeIdentity, timeout: Duration) async throws -> RemoteNode {
        let nodeID = requested == .server ? (upstream ?? requested) : requested

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
        case .pending(var waiters):
            waiters.append((nonce: nonce, continuation: continuation))
            nodes[nodeID] = .pending(waiters)
        case nil:
            nodes[nodeID] = .pending([(nonce: nonce, continuation: continuation)])
        }
    }

    /// Removes and resumes with CancellationError the continuation identified by
    /// `nonce`.  Because each waiter is stored as a `(nonce, continuation)` pair,
    /// the correct entry can be found by identity even when multiple callers are
    /// waiting for the same `nodeID`.
    private func removePendingContinuation(
        nonce: UUID,
        nodeID: NodeIdentity
    ) {
        guard case .pending(var waiters) = nodes[nodeID] else { return }

        guard let index = waiters.firstIndex(where: { $0.nonce == nonce }) else { return }

        let cancelled = waiters.remove(at: index)
        if waiters.isEmpty {
            nodes.removeValue(forKey: nodeID)
        } else {
            nodes[nodeID] = .pending(waiters)
        }
        cancelled.continuation.resume(throwing: CancellationError())
    }
}
