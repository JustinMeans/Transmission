import Foundation

/// How a topic behaves when a consumer falls behind.
public enum TopicKind: UInt8, Sendable, Codable, Equatable {
    /// Latest wins. Intermediates may vanish.
    case state = 0
    /// Chart points. Downsampled to the subscriber's rate.
    case series = 1
    /// Append only. Overflow drops the oldest and tells the client.
    case log = 2
    /// A notification that something was persisted. Clients fetch by cursor.
    case event = 3
}

/// Identifies one topic instance. Each schema is its own instance, so a server
/// can publish two during a migration.
public struct TopicKey: Hashable, Sendable, CustomStringConvertible {
    public let name: String
    public let params: [String: String]
    public let schema: Int

    public init(name: String, params: [String: String] = [:], schema: Int) {
        self.name = name
        self.params = params
        self.schema = schema
    }

    public var description: String {
        let query = params.keys.sorted().map { "\($0)=\(params[$0]!)" }.joined(separator: "&")
        return "\(name)?\(query)#\(schema)"
    }
}

/// A typed topic. Client and server share this definition, so `apply` is the
/// same code at both ends.
public struct Topic<State: Codable & Sendable, Delta: Codable & Sendable>: Sendable {
    public let name: String
    public let schema: Int
    public let kind: TopicKind
    public let params: [String: String]
    public let apply: @Sendable (inout State, Delta) -> Void
    /// Folds a later delta into an earlier one. Without it, a state topic sends a
    /// snapshot on overflow and a series topic keeps only the latest point.
    public let merge: (@Sendable (inout Delta, Delta) -> Void)?

    public init(
        name: String,
        schema: Int = 1,
        kind: TopicKind,
        params: [String: String] = [:],
        merge: (@Sendable (inout Delta, Delta) -> Void)? = nil,
        apply: @escaping @Sendable (inout State, Delta) -> Void
    ) {
        self.name = name
        self.schema = schema
        self.kind = kind
        self.params = params
        self.merge = merge
        self.apply = apply
    }

    public var key: TopicKey {
        TopicKey(name: name, params: params, schema: schema)
    }
}
