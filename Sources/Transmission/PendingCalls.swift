import Foundation

/// Manages pending remote calls awaiting responses.
public actor PendingCalls {
    private var continuations: [CallID: CheckedContinuation<Data, any Error>] = [:]
    private var timeoutTasks: [CallID: Task<Void, Never>] = [:]
    /// Maps each in-flight call to the node it was dispatched to, enabling
    /// per-node cancellation when a connection drops.
    private var callNodeIDs: [CallID: NodeIdentity] = [:]

    public init() {}

    /// Sends a call and waits for the response.
    public func send(envelope: CallEnvelope, via node: RemoteNode, timeout: Duration = .seconds(30)) async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            Task {
                await self.register(callID: envelope.callID, nodeID: node.nodeID, continuation: continuation, timeout: timeout)

                do {
                    try await node.send(.call(envelope))
                } catch {
                    await self.fail(callID: envelope.callID, error: error)
                }
            }
        }
    }

    /// Receives a reply for a pending call.
    public func receive(reply: ReplyEnvelope) {
        guard let continuation = continuations.removeValue(forKey: reply.callID) else {
            return
        }

        timeoutTasks[reply.callID]?.cancel()
        timeoutTasks.removeValue(forKey: reply.callID)
        callNodeIDs.removeValue(forKey: reply.callID)

        continuation.resume(returning: reply.value)
    }

    /// Cancels only the pending calls that were dispatched to the given node.
    /// Calls to other nodes are left untouched.
    public func cancelAll(for nodeID: NodeIdentity) {
        let targetCallIDs = callNodeIDs.compactMap { (callID, nid) -> CallID? in
            nid == nodeID ? callID : nil
        }
        for callID in targetCallIDs {
            if let continuation = continuations.removeValue(forKey: callID) {
                continuation.resume(throwing: TransmissionError.noConnection)
            }
            timeoutTasks[callID]?.cancel()
            timeoutTasks.removeValue(forKey: callID)
            callNodeIDs.removeValue(forKey: callID)
        }
    }

    private func register(
        callID: CallID,
        nodeID: NodeIdentity,
        continuation: CheckedContinuation<Data, any Error>,
        timeout: Duration
    ) {
        continuations[callID] = continuation
        callNodeIDs[callID] = nodeID

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
        timeoutTasks.removeValue(forKey: callID)
        callNodeIDs.removeValue(forKey: callID)
        continuation.resume(throwing: TransmissionError.callTimeout(callID))
    }

    private func fail(callID: CallID, error: any Error) {
        guard let continuation = continuations.removeValue(forKey: callID) else {
            return
        }
        timeoutTasks[callID]?.cancel()
        timeoutTasks.removeValue(forKey: callID)
        callNodeIDs.removeValue(forKey: callID)
        continuation.resume(throwing: error)
    }
}
