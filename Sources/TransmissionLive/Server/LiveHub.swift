import Foundation
import Logging

/// Who a connection belongs to.
public struct LiveIdentity: Sendable, Hashable {
    public let subject: String
    public let expiresAt: Date?

    public init(subject: String, expiresAt: Date? = nil) {
        self.subject = subject
        self.expiresAt = expiresAt
    }
}

/// The access rules. Every `SUB` passes through `authorize`.
public protocol LiveAuthorizer: Sendable {
    /// Validates a token from the upgrade or from `AUTH_REFRESH`.
    func authenticate(token: String) async throws -> LiveIdentity
    func authorize(_ identity: LiveIdentity, topic: TopicKey) async -> Bool
    /// Called periodically, so a revocation ends a live session.
    func revalidate(_ identity: LiveIdentity) async -> Bool
}

public struct LiveHubConfiguration: Sendable {
    public var heartbeat: Duration = .seconds(25)
    /// A single write outstanding this long closes the connection as a slow consumer.
    public var stallLimit: Duration = .seconds(30)
    /// Bytes behind, summed across a connection's subscriptions, that close it.
    public var maxQueuedBytes = 4 << 20
    /// Deltas a log subscription may lag by before the oldest are dropped with a `GAP`.
    public var mailboxCapacity = 1024
    public var maxFrame = 1 << 20
    public var maxSubscriptionsPerConnection = 256
    /// How often connections check stalls and heartbeats.
    public var tickInterval: Duration = .seconds(1)
    public var revalidateInterval: Duration = .seconds(60)
    /// Warn this long before an identity expires.
    public var expiryWarning: Duration = .seconds(60)

    public init() {}
}

public enum LiveHubError: Error, Equatable {
    case unknownTopic(String)
    case topicAlreadyHosted(String)
    case payloadTooLarge(Int)
}

/// Owns the topic registry and subscriber refcounts. A publish here never waits
/// for a client: it appends to the ring and rings each connection's doorbell.
public actor LiveHub {
    public nonisolated let configuration: LiveHubConfiguration
    nonisolated let authorizer: any LiveAuthorizer
    nonisolated let logger: Logger

    private let bus: any LiveBus
    private var busAttached = false
    private var topics: [TopicKey: LiveTopicInstance] = [:]
    private var subscribers: [TopicKey: [ObjectIdentifier: Subscriber]] = [:]
    private var connections: [ObjectIdentifier: LiveConnection] = [:]
    private var byClient: [ClientKey: LiveConnection] = [:]
    private var onDemandChange: (@Sendable (TopicKey, Bool) -> Void)?
    public nonisolated let epoch = TopicRing.makeEpoch()

    private struct Subscriber {
        let connection: LiveConnection
        var count: Int
    }

    private struct ClientKey: Hashable {
        let subject: String?
        let clientID: String
    }

    public init(
        configuration: LiveHubConfiguration = .init(),
        authorizer: any LiveAuthorizer,
        bus: any LiveBus = InProcessLiveBus(),
        logger: Logger = Logger(label: "transmission.live")
    ) {
        self.configuration = configuration
        self.authorizer = authorizer
        self.bus = bus
        self.logger = logger
    }

    /// Reports the first subscriber arriving and the last leaving, which is what
    /// a demand-driven sampler keys off.
    public func setDemandHandler(_ handler: @escaping @Sendable (TopicKey, Bool) -> Void) {
        onDemandChange = handler
    }

    @discardableResult
    public func host<State, Delta>(
        _ topic: Topic<State, Delta>,
        initial: State,
        limits: TopicRing.Limits? = nil
    ) async throws -> LiveTopicInstance {
        guard topics[topic.key] == nil else {
            throw LiveHubError.topicAlreadyHosted(topic.key.description)
        }
        await attachBus()
        let defaults: TopicRing.Limits = topic.kind == .log || topic.kind == .event ? .logs : .metrics
        let instance = LiveTopicInstance(
            topic: topic, initial: initial, limits: limits ?? defaults, maxFrame: configuration.maxFrame)
        topics[topic.key] = instance
        return instance
    }

    private func attachBus() async {
        guard !busAttached else { return }
        busAttached = true
        await bus.subscribe { [weak self] message in
            await self?.deliver(message)
        }
    }

    /// Encodes once here; everything downstream shares these bytes.
    public func publish<State, Delta>(_ topic: Topic<State, Delta>, _ delta: Delta, sampleTime: Date = Date()) async throws {
        guard topics[topic.key] != nil else {
            throw LiveHubError.unknownTopic(topic.key.description)
        }
        let payload = try JSONEncoder().encode(delta)
        guard payload.count <= configuration.maxFrame - 64 else {
            throw LiveHubError.payloadTooLarge(payload.count)
        }
        try await bus.publish(LiveBusMessage(
            key: topic.key,
            sampleTimeMs: UInt64(max(sampleTime.timeIntervalSince1970, 0) * 1000),
            payload: payload))
    }

    private func deliver(_ message: LiveBusMessage) {
        guard let instance = topics[message.key] else { return }
        do {
            try instance.ingest(payload: message.payload, sampleTimeMs: message.sampleTimeMs)
        } catch {
            logger.error("Dropped delta for \(message.key): \(error)")
            return
        }
        for subscriber in subscribers[message.key, default: [:]].values {
            subscriber.connection.topicAdvanced()
        }
    }

    public func subscriberCount(_ key: TopicKey) -> Int {
        subscribers[key, default: [:]].values.reduce(0) { $0 + $1.count }
    }

    public var connectionCount: Int { connections.count }

    /// Closes every connection, for server shutdown. Clients treat this like any
    /// other drop and resume on the next instance, which is why a deploy is routine.
    public func closeAll(code: LiveCloseCode = .normal) {
        for connection in connections.values {
            connection.close(code: code)
        }
    }

    public func topic(_ key: TopicKey) -> LiveTopicInstance? { topics[key] }

    // MARK: Connections

    func add(_ connection: LiveConnection) {
        connections[ObjectIdentifier(connection)] = connection
    }

    func remove(_ connection: LiveConnection, clientID: String?, subject: String?) {
        connections[ObjectIdentifier(connection)] = nil
        for key in subscribers.keys {
            removeSubscriber(connection, from: key, removingAll: true)
        }
        if let clientID {
            let key = ClientKey(subject: subject, clientID: clientID)
            // By instance: a late close from an old socket must not evict its replacement.
            if byClient[key] === connection {
                byClient[key] = nil
            }
        }
    }

    /// Records `connection` as this client's current one, returning the one it replaces.
    func claim(clientID: String, subject: String?, connection: LiveConnection) -> LiveConnection? {
        let key = ClientKey(subject: subject, clientID: clientID)
        let previous = byClient[key]
        byClient[key] = connection
        return previous === connection ? nil : previous
    }

    func adverts() -> [TopicAdvert] {
        var byName: [String: TopicAdvert] = [:]
        for instance in topics.values {
            let schema = UInt64(instance.key.schema)
            if var existing = byName[instance.key.name] {
                existing.schemaMin = min(existing.schemaMin, schema)
                existing.schemaMax = max(existing.schemaMax, schema)
                byName[instance.key.name] = existing
            } else {
                byName[instance.key.name] = TopicAdvert(
                    name: instance.key.name, schemaMin: schema, schemaMax: schema, kind: instance.kind)
            }
        }
        return byName.values.sorted { $0.name < $1.name }
    }

    enum SubscribeResult {
        case ok(LiveTopicInstance)
        case refused(LiveErrorCode, String)
    }

    func subscribe(_ connection: LiveConnection, key: TopicKey, identity: LiveIdentity?) async -> SubscribeResult {
        guard let instance = topics[key] else {
            if topics.keys.contains(where: { $0.name == key.name && $0.params == key.params }) {
                return .refused(.unsupportedSchema, "Schema \(key.schema) of \(key.name) is not published")
            }
            return .refused(.unknownTopic, "Unknown topic \(key.name)")
        }
        guard let identity else {
            return .refused(.forbidden, "Not authenticated")
        }
        guard await authorizer.authorize(identity, topic: key) else {
            return .refused(.forbidden, "Not permitted")
        }
        // The hub is reentrant across that await: the connection may be gone.
        guard connections[ObjectIdentifier(connection)] != nil else {
            return .refused(.forbidden, "Connection closed")
        }
        let id = ObjectIdentifier(connection)
        var entries = subscribers[key, default: [:]]
        let wasEmpty = entries.isEmpty
        entries[id, default: Subscriber(connection: connection, count: 0)].count += 1
        subscribers[key] = entries
        if wasEmpty { onDemandChange?(key, true) }
        return .ok(instance)
    }

    func unsubscribe(_ connection: LiveConnection, key: TopicKey) {
        removeSubscriber(connection, from: key, removingAll: false)
    }

    private func removeSubscriber(_ connection: LiveConnection, from key: TopicKey, removingAll: Bool) {
        let id = ObjectIdentifier(connection)
        guard var entries = subscribers[key], var entry = entries[id] else { return }
        entry.count -= 1
        if removingAll || entry.count == 0 {
            entries[id] = nil
        } else {
            entries[id] = entry
        }
        if entries.isEmpty {
            subscribers[key] = nil
            onDemandChange?(key, false)
        } else {
            subscribers[key] = entries
        }
    }
}
