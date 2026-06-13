import Distributed
import Foundation
import Logging
import Metrics
import NIO

/// The core distributed actor system for Transmission.
/// Handles actor registration, resolution, and remote method invocation over WebSockets.
public final class TransmissionSystem: DistributedActorSystem, @unchecked Sendable {
    public typealias ActorID = ActorIdentity
    public typealias ResultHandler = TransmissionResultHandler
    public typealias InvocationEncoder = TransmissionEncoder
    public typealias InvocationDecoder = TransmissionDecoder
    public typealias SerializationRequirement = any Codable

    /// The node identity for this system instance.
    public let nodeID: NodeIdentity

    /// Logger for this system.
    public let logger: Logger

    /// Connection timeout for remote calls.
    public var connectionTimeout: Duration = .seconds(30)

    private let lock = NSLock()
    private var actors: [ActorID: any DistributedActor] = [:]
    private var onDemandResolver: ((ActorID) -> (any DistributedActor)?)?
    private var clientManager: ClientManager?

    public let nodes = NodeDirectory()
    public let pendingCalls = PendingCalls()
    public let metrics: TransmissionMetrics

    @TaskLocal static var actorIDHint: ActorID?
    @TaskLocal private static var lockHeld: Bool = false

    /// Creates a new TransmissionSystem for client use.
    public init(id: String? = nil) {
        let nodeID = NodeIdentity(id: id ?? UUID().uuidString)
        self.nodeID = nodeID
        self.logger = Logger(label: "transmission.\(nodeID.id)")
        self.metrics = TransmissionMetrics(nodeID: nodeID)
    }

    /// Creates a new TransmissionSystem configured as a server.
    public static func server(id: String = "server") -> TransmissionSystem {
        TransmissionSystem(id: id)
    }

    // MARK: - Actor Lifecycle

    public func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act?
    where Act: DistributedActor, Act.ID == ActorID {
        let actor: (any DistributedActor)? = withLock {
            if let existing = actors[id] {
                return existing
            }
            return onDemandResolver?(id)
        }

        guard let resolved = actor else {
            return nil
        }

        guard let typed = resolved as? Act else {
            throw TransmissionError.typeMismatch(expected: "\(Act.self)", got: "\(type(of: resolved))")
        }

        return typed
    }

    public func assignID<Act>(_ actorType: Act.Type) -> ActorID
    where Act: DistributedActor, Act.ID == ActorID {
        if let hint = Self.actorIDHint {
            return hint.withNode(nodeID)
        }
        return ActorIdentity.random(for: actorType).withNode(nodeID)
    }

    public func actorReady<Act>(_ actor: Act)
    where Act: DistributedActor, Act.ID == ActorID {
        withLock {
            actors[actor.id] = actor
        }
        logger.debug("Actor ready: \(actor.id)")
    }

    public func resignID(_ id: ActorID) {
        _ = withLock {
            actors.removeValue(forKey: id)
        }
        logger.debug("Actor resigned: \(id)")
    }

    // MARK: - Actor Creation

    /// Creates a local actor with a specific ID.
    @discardableResult
    public func makeLocalActor<Act: DistributedActor>(
        id: ActorID,
        _ factory: () -> Act
    ) -> Act where Act.ID == ActorID {
        Self.$actorIDHint.withValue(id.withNode(nodeID)) {
            factory()
        }
    }

    /// Registers a handler for on-demand actor resolution.
    public func registerOnDemandResolver(_ resolver: @escaping (ActorID) -> (any DistributedActor)?) {
        withLock {
            onDemandResolver = resolver
        }
    }

    func setClientManager(_ client: ClientManager) {
        withLock {
            clientManager = client
        }
    }

    /// Looks up a registered actor by ID without type constraints.
    /// Returns nil if no actor is registered with the given ID.
    public func lookupActor(id: ActorID) -> (any DistributedActor)? {
        withLock {
            if let existing = actors[id] {
                return existing
            }
            return onDemandResolver?(id)
        }
    }

    /// Handles an incoming call envelope and returns the reply data.
    /// This provides a clean integration point for custom transport layers.
    /// - Parameters:
    ///   - call: The incoming call envelope
    ///   - format: Serialization format for the reply (defaults to binary for optimal performance)
    ///   - sendReply: Callback to send the reply data
    public func handleCall(_ call: CallEnvelope, format: SerializationFormat = .binary, sendReply: @escaping @Sendable (Data) async throws -> Void) async {
        do {
            guard let actor = lookupActor(id: call.recipient) else {
                throw TransmissionError.actorNotFound(call.recipient)
            }

            var decoder = TransmissionDecoder(envelope: call, system: self)
            let resultBox = ResultBox()
            let handler = TransmissionResultHandler(callID: call.callID, resultBox: resultBox)

            do {
                try await executeDistributedTarget(
                    on: actor,
                    target: RemoteCallTarget(call.target),
                    invocationDecoder: &decoder,
                    handler: handler
                )
            } catch {
                logger.debug("Target execution error: \(error)")
            }

            let reply = ReplyEnvelope(
                callID: call.callID,
                sender: call.recipient,
                value: resultBox.value
            )

            let wireEnvelope = WireEnvelope.reply(reply)
            let replyData: Data
            switch format {
            case .binary:
                replyData = wireEnvelope.encodeCompact()
            case .json:
                replyData = try JSONEncoder().encode(wireEnvelope)
            }
            try await sendReply(replyData)
        } catch {
            logger.error("Failed to handle call: \(error)")
        }
    }

    // MARK: - Remote Calls

    public func remoteCall<Act, Err, Res>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type,
        returning: Res.Type
    ) async throws -> Res
    where Act: DistributedActor,
          Act.ID == ActorID,
          Err: Error,
          Res: Codable {

        let callID = CallID()
        let envelope = CallEnvelope(
            callID: callID,
            recipient: actor.id,
            target: target.identifier,
            genericSubs: invocation.genericSubs,
            args: invocation.args,
            priority: invocation.priority
        )

        let data = try await sendCall(envelope: envelope, to: actor.id)
        metrics.recordCall(target: target.identifier)

        let decoder = JSONDecoder()
        decoder.userInfo[.transmissionSystem] = self
        return try decoder.decode(Res.self, from: data)
    }

    public func remoteCallVoid<Act, Err>(
        on actor: Act,
        target: RemoteCallTarget,
        invocation: inout InvocationEncoder,
        throwing: Err.Type
    ) async throws
    where Act: DistributedActor,
          Act.ID == ActorID,
          Err: Error {

        let callID = CallID()
        let envelope = CallEnvelope(
            callID: callID,
            recipient: actor.id,
            target: target.identifier,
            genericSubs: invocation.genericSubs,
            args: invocation.args,
            priority: invocation.priority
        )

        _ = try await sendCall(envelope: envelope, to: actor.id)
        metrics.recordCall(target: target.identifier)
    }

    private func sendCall(envelope: CallEnvelope, to actorID: ActorID) async throws -> Data {
        guard let targetNode = actorID.node else {
            throw TransmissionError.noNodeForActor(actorID)
        }

        let node = try await nodes.node(for: targetNode, timeout: connectionTimeout)
        return try await pendingCalls.send(envelope: envelope, via: node)
    }

    public func makeInvocationEncoder() -> InvocationEncoder {
        TransmissionEncoder(system: self)
    }

    // MARK: - Node Lifecycle

    /// Unregisters a node and immediately cancels any pending calls that were
    /// dispatched to it. Callers should use this instead of calling
    /// `nodes.unregister` directly so that in-flight calls fail fast rather
    /// than waiting for their full timeout period.
    public func nodeDidDisconnect(_ nodeID: NodeIdentity) async {
        await nodes.unregister(nodeID)
        await pendingCalls.cancelAll(for: nodeID)
    }

    // MARK: - Message Handling

    /// Decodes and delivers an incoming message from a remote node.
    /// Automatically detects binary vs JSON format based on the data.
    public func decodeAndDeliver(data: Data, from node: RemoteNode) async {
        do {
            let envelope: WireEnvelope

            // Detect format: binary format starts with type byte (0, 1, or 2)
            // JSON format starts with '{' (0x7B) for object
            if let firstByte = data.first, firstByte <= 2 {
                envelope = try WireEnvelope.decodeCompact(from: data)
            } else {
                let decoder = JSONDecoder()
                decoder.userInfo[.transmissionSystem] = self
                envelope = try decoder.decode(WireEnvelope.self, from: data)
            }

            switch envelope {
            case .call(let call):
                await handleCall(call, from: node)
            case .reply(let reply):
                await handleReply(reply)
            case .close:
                await node.close()
            }
        } catch {
            logger.error("Failed to decode message: \(error)")
        }
    }

    private func handleCall(_ call: CallEnvelope, from node: RemoteNode) async {
        do {
            guard let actor = withLock({ actors[call.recipient] }) else {
                throw TransmissionError.actorNotFound(call.recipient)
            }

            var decoder = TransmissionDecoder(
                envelope: call,
                system: self
            )

            let resultBox = ResultBox()
            let handler = TransmissionResultHandler(callID: call.callID, resultBox: resultBox)

            do {
                try await executeDistributedTarget(
                    on: actor,
                    target: RemoteCallTarget(call.target),
                    invocationDecoder: &decoder,
                    handler: handler
                )
            } catch {
                logger.debug("Target execution error: \(error)")
            }

            let reply = ReplyEnvelope(
                callID: call.callID,
                sender: call.recipient,
                value: resultBox.value
            )

            try await node.send(.reply(reply))
        } catch {
            logger.error("Call handling failed: \(error)")
        }
    }

    private func handleReply(_ reply: ReplyEnvelope) async {
        await pendingCalls.receive(reply: reply)
    }

    private func withLock<T>(_ body: () -> T) -> T {
        if Self.lockHeld {
            return body()
        }
        return Self.$lockHeld.withValue(true) {
            lock.lock()
            defer { lock.unlock() }
            return body()
        }
    }
}

// MARK: - CodingUserInfoKey Extension

extension CodingUserInfoKey {
    public static let transmissionSystem = CodingUserInfoKey(rawValue: "transmission.system")!
}
