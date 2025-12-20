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

        // Wait for connection with timeout
        return try await withThrowingTaskGroup(of: RemoteNode.self) { group in
            group.addTask {
                try await withCheckedThrowingContinuation { continuation in
                    Task {
                        await self.addPendingContinuation(for: nodeID, continuation: continuation)
                    }
                }
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

    private func addPendingContinuation(
        for nodeID: NodeIdentity,
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
}
