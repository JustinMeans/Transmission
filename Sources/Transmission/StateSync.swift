import Foundation
import Distributed

public protocol Syncable: Codable, Sendable, Equatable {
    var version: Int { get }
}

extension Syncable {
    public var version: Int { 0 }
}

public struct SyncedState<T: Codable & Sendable>: Sendable {
    public var value: T
    public private(set) var lastSyncedAt: Date?
    /// True whenever the local value has changed since the last successful sync.
    /// Initialized to true because a freshly-created SyncedState has never been
    /// synced to any peer (lastSyncedAt == nil), so the initial value must be
    /// treated as pending sync just like any subsequent update.
    public private(set) var isDirty: Bool = true

    public init(_ value: T) {
        self.value = value
    }

    public mutating func update(_ newValue: T) {
        self.value = newValue
        self.isDirty = true
    }

    public mutating func markSynced() {
        self.lastSyncedAt = Date()
        self.isDirty = false
    }
}

public protocol StateHolder: DistributedActor where ActorSystem == TransmissionSystem {
    associatedtype State: Codable & Sendable
    func get() async throws -> State
    func set(_ newState: State) async throws
}

extension Encodable {
    public func transmissionEncode() throws -> Data {
        try JSONEncoder().encode(self)
    }
}

extension Decodable {
    public static func transmissionDecode(from data: Data) throws -> Self {
        try JSONDecoder().decode(Self.self, from: data)
    }
}

public struct StateBatch<T: Codable & Sendable>: Codable, Sendable {
    public let items: [T]
    public let timestamp: Date

    public init(items: [T], timestamp: Date = Date()) {
        self.items = items
        self.timestamp = timestamp
    }
}

public struct StateDelta<T: Codable & Sendable>: Codable, Sendable {
    public enum Operation: String, Codable, Sendable {
        case set
        case delete
        case merge
    }

    public let operation: Operation
    public let path: String
    public let value: T?
    public let timestamp: Date

    public init(operation: Operation, path: String, value: T? = nil, timestamp: Date = Date()) {
        self.operation = operation
        self.path = path
        self.value = value
        self.timestamp = timestamp
    }
}

public protocol StateBroadcaster: DistributedActor where ActorSystem == TransmissionSystem {
    associatedtype State: Codable & Sendable
    func getState() async throws -> State
    func subscribe(nodeID: String) async throws -> State
    func unsubscribe(nodeID: String) async throws
}
