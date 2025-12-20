import Foundation

/// Uniquely identifies a distributed actor within the Transmission system.
public struct ActorIdentity: Hashable, Sendable, Codable, CustomStringConvertible {
    /// The unique identifier string.
    public let id: String

    /// The node this actor resides on, if known.
    public let node: NodeIdentity?

    /// The actor type name, for debugging and on-demand creation.
    public let typeName: String?

    public init(id: String, node: NodeIdentity? = nil, typeName: String? = nil) {
        self.id = id
        self.node = node
        self.typeName = typeName
    }

    /// Creates an identity with a specific ID string.
    public init(_ id: String) {
        self.id = id
        self.node = nil
        self.typeName = nil
    }

    /// Creates a random identity.
    public static func random() -> ActorIdentity {
        ActorIdentity(id: UUID().uuidString)
    }

    /// Creates a random identity with type information.
    public static func random<T>(for type: T.Type) -> ActorIdentity {
        ActorIdentity(
            id: UUID().uuidString,
            typeName: String(reflecting: type)
        )
    }

    /// Returns a new identity with the specified node.
    public func withNode(_ node: NodeIdentity?) -> ActorIdentity {
        ActorIdentity(id: id, node: node, typeName: typeName)
    }

    /// Checks if this identity has a type prefix matching the given type.
    public func hasType<T>(for type: T.Type) -> Bool {
        guard let typeName else { return false }
        return typeName.contains(String(describing: type))
    }

    // MARK: - Hashable

    public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }

    public static func == (lhs: ActorIdentity, rhs: ActorIdentity) -> Bool {
        lhs.id == rhs.id
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        if let node {
            return "\(node.id)/\(id)"
        }
        return id
    }
}

// MARK: - ExpressibleByStringLiteral

extension ActorIdentity: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(id: value)
    }
}
