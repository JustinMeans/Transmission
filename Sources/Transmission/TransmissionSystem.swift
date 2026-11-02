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

    /// The single frame limit: applied by the client and server upgraders, the
    /// message reassembler, the Vapor route and the sender.
    public static let defaultMaxFrameSize = 64 << 20

    private let lock = NSLock()
    private var settings = Settings()
    private var actors: [ActorID: ActorEntry] = [:]
    private var onDemandResolver: ((ActorID) -> (any DistributedActor)?)?
    private var clientManager: ClientManager?
    private var clientLoop: Task<Void, Never>?

    private struct Settings {
        var connectionTimeout: Duration = .seconds(30)
        var callTimeout: Duration = .seconds(30)
        var maxFrameSize = TransmissionSystem.defaultMaxFrameSize
        var serverHeartbeat: HeartbeatPolicy? = .standard
        var dispatchLimits = DispatchLimits()
    }

    /// `weak` is what lets a per-session actor deinit and resign. `pinned` is
    /// only set by `makeLocalActor`, whose result is `@discardableResult`:
    /// callers already rely on the system keeping those alive.
    private struct ActorEntry {
        weak var actor: (any DistributedActor)?
        var pinned: (any DistributedActor)?
    }

    public let nodes = NodeDirectory()
    public let pendingCalls = PendingCalls()
    public let metrics: TransmissionMetrics

    @TaskLocal static var actorIDHint: ActorID?

    /// How long a call waits for a connection to its node.
    public var connectionTimeout: Duration {
        get { lock.withLock { settings.connectionTimeout } }
        set { lock.withLock { settings.connectionTimeout = newValue } }
    }

    /// How long a call waits for its reply once sent.
    public var callTimeout: Duration {
        get { lock.withLock { settings.callTimeout } }
        set { lock.withLock { settings.callTimeout = newValue } }
    }

    /// Set before connecting or serving; open connections keep their limit.
    public var maxFrameSize: Int {
        get { lock.withLock { settings.maxFrameSize } }
        set { lock.withLock { settings.maxFrameSize = newValue } }
    }

    /// Keepalive for inbound connections. `nil` disables it. Set before serving.
    public var serverHeartbeat: HeartbeatPolicy? {
        get { lock.withLock { settings.serverHeartbeat } }
        set { lock.withLock { settings.serverHeartbeat = newValue } }
    }

    /// Per-connection dispatch limits. Set before connecting or serving.
    public var dispatchLimits: DispatchLimits {
        get { lock.withLock { settings.dispatchLimits } }
        set { lock.withLock { settings.dispatchLimits = newValue } }
    }

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

    deinit {
        clientLoop?.cancel()
    }

    // MARK: - Actor Lifecycle

    public func resolve<Act>(id: ActorID, as actorType: Act.Type) throws -> Act?
    where Act: DistributedActor, Act.ID == ActorID {
        guard let resolved = lookupActor(id: id) else {
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
        lock.withLock {
            actors[actor.id] = ActorEntry(actor: actor)
        }
        logger.debug("Actor ready: \(actor.id)")
    }

    public func resignID(_ id: ActorID) {
        // Runs from the actor's deinit, when the weak reference already reads nil.
        // A live entry under this id is a replacement and must stay.
        lock.withLock {
            if let entry = actors[id], entry.actor == nil {
                actors[id] = nil
            }
        }
        logger.debug("Actor resigned: \(id)")
    }

    // MARK: - Actor Creation

    /// Creates a local actor with a specific ID. The system keeps it alive until
    /// `releaseLocalActor(id:)`, so the result may be discarded. Actors created
    /// with plain `Actor(actorSystem:)` are held weakly instead.
    @discardableResult
    public func makeLocalActor<Act: DistributedActor>(
        id: ActorID,
        _ factory: () -> Act
    ) -> Act where Act.ID == ActorID {
        let actor = Self.$actorIDHint.withValue(id.withNode(nodeID)) {
            factory()
        }
        lock.withLock {
            actors[actor.id]?.pinned = actor
        }
        return actor
    }

    /// Stops keeping a `makeLocalActor` actor alive. It resigns once nothing else
    /// references it.
    public func releaseLocalActor(id: ActorID) {
        // Taken out and dropped outside the lock: this can be the last reference,
        // and the actor's deinit calls back into `resignID`.
        let released = lock.withLock { () -> (any DistributedActor)? in
            let pinned = actors[id]?.pinned
            actors[id]?.pinned = nil
            return pinned
        }
        _ = released
    }

    var liveActorCount: Int {
        lock.withLock { actors.values.filter { $0.actor != nil }.count }
    }

    /// Registers a handler for on-demand actor resolution. It runs without any
    /// lock held, so it may create actors, and may run twice for one id if two
    /// calls race: it should return the same actor for the same id.
    public func registerOnDemandResolver(_ resolver: @escaping (ActorID) -> (any DistributedActor)?) {
        lock.withLock {
            onDemandResolver = resolver
        }
    }

    func sharedClientManager() -> ClientManager {
        lock.withLock {
            if let clientManager { return clientManager }
            let created = ClientManager(system: self)
            clientManager = created
            return created
        }
    }

    func clientManagerIfAny() -> ClientManager? {
        lock.withLock { clientManager }
    }

    func setClientLoop(_ task: Task<Void, Never>) {
        lock.withLock { clientLoop = task }
    }

    /// Looks up a registered actor by ID without type constraints.
    /// Returns nil if no actor is registered with the given ID.
    public func lookupActor(id: ActorID) -> (any DistributedActor)? {
        let (existing, resolver) = lock.withLock { (actors[id]?.actor, onDemandResolver) }
        return existing ?? resolver?(id)
    }

    /// Handles an incoming call envelope and returns the reply data.
    /// This provides a clean integration point for custom transport layers.
    /// - Parameters:
    ///   - call: The incoming call envelope
    ///   - format: Serialization format for the reply (defaults to binary for optimal performance)
    ///   - sendReply: Callback to send the reply data
    public func handleCall(_ call: CallEnvelope, format: SerializationFormat = .binary, sendReply: @escaping @Sendable (Data) async throws -> Void) async {
        do {
            let reply = await dispatch(call, from: nil)
            let replyData: Data
            switch format {
            case .binary:
                replyData = reply.encodeCompact()
            case .json:
                replyData = try JSONEncoder().encode(reply)
            }
            try await sendReply(replyData)
        } catch {
            logger.error("Failed to handle call: \(error)")
        }
    }

    /// Runs one call and builds its answer. Every failure becomes an error
    /// envelope: there is no path that answers a failed call with empty success.
    func dispatch(_ call: CallEnvelope, from node: RemoteNode?) async -> WireEnvelope {
        func failure(_ code: RemoteCallError.Code, _ message: String) -> WireEnvelope {
            .error(ErrorEnvelope(
                callID: call.callID, sender: call.recipient, code: code,
                errorType: nil, message: message, payload: nil))
        }

        guard let actor = lookupActor(id: call.recipient) else {
            return failure(.actorNotFound, "No actor \(call.recipient.id)")
        }

        let resultBox = ResultBox()
        let handler = TransmissionResultHandler(callID: call.callID, resultBox: resultBox)
        do {
            try await RemoteNode.$current.withValue(node) {
                var decoder = TransmissionDecoder(envelope: call, system: self)
                try await executeDistributedTarget(
                    on: actor,
                    target: RemoteCallTarget(call.target),
                    invocationDecoder: &decoder,
                    handler: handler
                )
            }
        } catch {
            logger.debug("Target execution error: \(error)")
            if error is ExecuteDistributedTargetError {
                return failure(.targetNotFound, "No distributed target \(call.target)")
            }
            if let thrown = resultBox.failure {
                return thrown.envelope(for: call)
            }
            if resultBox.encodingFailed {
                return failure(.resultEncodingFailed, "Result could not be encoded")
            }
            return failure(.badArguments, "Arguments could not be decoded for \(call.target)")
        }

        if let thrown = resultBox.failure {
            return thrown.envelope(for: call)
        }
        return .reply(ReplyEnvelope(callID: call.callID, sender: call.recipient, value: resultBox.value))
    }

    /// Writes a dispatched call's answer. An answer too large for the frame limit
    /// becomes an error reply rather than a dead connection.
    func send(_ answer: WireEnvelope, for call: CallEnvelope, to node: RemoteNode) async {
        do {
            do {
                try await node.send(answer)
            } catch TransmissionError.encodingFailed(let reason) {
                try await node.send(.error(ErrorEnvelope(
                    callID: call.callID, sender: call.recipient, code: .frameTooLarge,
                    errorType: nil, message: reason, payload: nil)))
            }
        } catch {
            logger.debug("Could not send reply: \(error)")
        }
    }

    func makeInboundDispatcher(for node: RemoteNode) -> InboundDispatcher {
        InboundDispatcher(system: self, node: node, limits: dispatchLimits)
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

        let data = try await sendCall(envelope: envelope, to: actor.id, throwing: Err.self)
        metrics.recordCall(target: target.identifier)

        let decoder = JSONDecoder()
        decoder.userInfo[.transmissionSystem] = self
        decoder.userInfo[.actorSystemKey] = self
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

        _ = try await sendCall(envelope: envelope, to: actor.id, throwing: Err.self)
        metrics.recordCall(target: target.identifier)
    }

    private func sendCall<Err: Error>(
        envelope: CallEnvelope,
        to actorID: ActorID,
        throwing: Err.Type
    ) async throws -> Data {
        guard let targetNode = actorID.node else {
            throw TransmissionError.noNodeForActor(actorID)
        }

        let started = ContinuousClock.now
        defer { metrics.recordCallDuration(started.duration(to: .now), target: envelope.target) }
        do {
            let node = try await nodes.node(for: targetNode, timeout: connectionTimeout)
            return try await pendingCalls.send(envelope: envelope, via: node, timeout: callTimeout)
        } catch let error as RemoteCallError {
            metrics.recordError(error)
            // A `throws(SomeError)` method gets its own type back when it can be decoded.
            if let payload = error.payload,
               let decodable = Err.self as? any Decodable.Type,
               let typed = try? JSONDecoder().decode(decodable, from: payload) as? Err {
                throw typed
            }
            throw error
        } catch {
            metrics.recordError(error)
            throw error
        }
    }

    public func makeInvocationEncoder() -> InvocationEncoder {
        TransmissionEncoder(system: self)
    }

    // MARK: - Node Lifecycle

    /// Unregisters a node and immediately cancels any pending calls that were
    /// dispatched to it. Callers should use this instead of calling
    /// `nodes.unregister` directly so that in-flight calls fail fast rather
    /// than waiting for their full timeout period.
    ///
    /// Connection teardown should use `nodeDidDisconnect(_: RemoteNode)`: this
    /// overload acts on whichever connection holds the id right now.
    public func nodeDidDisconnect(_ nodeID: NodeIdentity) async {
        await nodes.unregister(nodeID)
        await pendingCalls.cancelAll(for: nodeID)
    }

    /// Tears down one exact connection. A close that arrives after a newer socket
    /// took the id cancels the old socket's calls and leaves the new socket alone.
    public func nodeDidDisconnect(_ node: RemoteNode) async {
        await nodes.unregister(node)
        await pendingCalls.cancelAll(for: node)
    }

    /// Registers an inbound connection. Calls in flight on any socket it replaces
    /// can never be answered, so they fail now rather than at their timeout.
    public func adopt(_ node: RemoteNode) async {
        if let replaced = await nodes.adopt(node) {
            await pendingCalls.cancelAll(for: replaced)
        }
    }

    func adoptUpstream(_ node: RemoteNode) async {
        if let replaced = await nodes.registerUpstream(node) {
            await pendingCalls.cancelAll(for: replaced)
        }
    }

    // MARK: - Message Handling

    /// Decodes and handles one message, waiting for any call it carries to finish.
    /// Transports use `InboundDispatcher` instead, which never waits; this remains
    /// for callers driving the system by hand.
    public func decodeAndDeliver(data: Data, from node: RemoteNode) async {
        do {
            switch try decodeEnvelope(data) {
            case .call(let call):
                let answer = await dispatch(call, from: node)
                await send(answer, for: call, to: node)
            case .reply(let reply):
                await pendingCalls.receive(reply: reply)
            case .error(let failure):
                await pendingCalls.receive(error: failure)
            case .close:
                await node.close()
            }
        } catch {
            logger.error("Failed to decode message: \(error)")
        }
    }

    /// Automatically detects binary vs JSON format based on the data.
    func decodeEnvelope(_ data: Data) throws -> WireEnvelope {
        if WireEnvelope.looksCompact(data) {
            return try WireEnvelope.decodeCompact(from: data)
        }
        let decoder = JSONDecoder()
        decoder.userInfo[.transmissionSystem] = self
        decoder.userInfo[.actorSystemKey] = self
        return try decoder.decode(WireEnvelope.self, from: data)
    }
}

// MARK: - CodingUserInfoKey Extension

extension CodingUserInfoKey {
    public static let transmissionSystem = CodingUserInfoKey(rawValue: "transmission.system")!
}
