import Foundation

/// Manages pending remote calls awaiting responses.
public actor PendingCalls {
    private var continuations: [CallID: CheckedContinuation<Data, any Error>] = [:]
    private var timeoutTasks: [CallID: Task<Void, Never>] = [:]
    /// Maps each in-flight call to the node it was dispatched to, enabling
    /// per-node cancellation when a connection drops.
    private var callNodeIDs: [CallID: NodeIdentity] = [:]
    /// The exact connection each call went out on, so a late close from a
    /// replaced socket cancels only that socket's calls.
    private var callNodes: [CallID: ObjectIdentifier] = [:]

    public init() {}

    /// Sends a call and waits for the response. Honours task cancellation.
    public func send(envelope: CallEnvelope, via node: RemoteNode, timeout: Duration = .seconds(30)) async throws -> Data {
        let callID = envelope.callID
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                register(callID: callID, node: node, continuation: continuation, timeout: timeout)
                if Task.isCancelled {
                    fail(callID: callID, error: CancellationError())
                    return
                }
                Task {
                    do {
                        try await node.send(.call(envelope))
                    } catch {
                        await self.fail(callID: callID, error: error)
                    }
                }
            }
        } onCancel: {
            Task { await self.fail(callID: callID, error: CancellationError()) }
        }
    }

    /// Receives a reply for a pending call.
    public func receive(reply: ReplyEnvelope) {
        guard let continuation = continuations.removeValue(forKey: reply.callID) else {
            return
        }

        clearTracking(reply.callID)
        continuation.resume(returning: reply.value)
    }

    /// Receives a failure for a pending call.
    public func receive(error: ErrorEnvelope) {
        fail(callID: error.callID, error: error.asError)
    }

    private func clearTracking(_ callID: CallID) {
        timeoutTasks[callID]?.cancel()
        timeoutTasks.removeValue(forKey: callID)
        callNodeIDs.removeValue(forKey: callID)
        callNodes.removeValue(forKey: callID)
    }

    /// Cancels only the pending calls that were dispatched to the given node.
    /// Calls to other nodes are left untouched.
    public func cancelAll(for nodeID: NodeIdentity) {
        let targetCallIDs = callNodeIDs.compactMap { (callID, nid) -> CallID? in
            nid == nodeID ? callID : nil
        }
        for callID in targetCallIDs {
            fail(callID: callID, error: TransmissionError.noConnection)
        }
    }

    /// Cancels only the calls sent over this exact connection.
    public func cancelAll(for node: RemoteNode) {
        let instance = ObjectIdentifier(node)
        let targetCallIDs = callNodes.compactMap { (callID, candidate) -> CallID? in
            candidate == instance ? callID : nil
        }
        for callID in targetCallIDs {
            fail(callID: callID, error: TransmissionError.noConnection)
        }
    }

    private func register(
        callID: CallID,
        node: RemoteNode,
        continuation: CheckedContinuation<Data, any Error>,
        timeout: Duration
    ) {
        continuations[callID] = continuation
        callNodeIDs[callID] = node.nodeID
        callNodes[callID] = ObjectIdentifier(node)

        timeoutTasks[callID] = Task {
            do {
                try await Task.sleep(for: timeout)
                await self.timeout(callID: callID)
            } catch {
                // Cancelled
            }
        }
    }

    private func timeout(callID: CallID) {
        guard let continuation = continuations.removeValue(forKey: callID) else {
            return
        }
        clearTracking(callID)
        continuation.resume(throwing: TransmissionError.callTimeout(callID))
    }

    private func fail(callID: CallID, error: any Error) {
        guard let continuation = continuations.removeValue(forKey: callID) else {
            return
        }
        clearTracking(callID)
        continuation.resume(throwing: error)
    }
}
