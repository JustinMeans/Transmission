import Foundation

/// Identifies a node (client or server instance) in the Transmission network.
public struct NodeIdentity: Hashable, Sendable, Codable, CustomStringConvertible {
    /// The unique identifier for this node.
    public let id: String

    public init(id: String) {
        self.id = id
    }

    /// The standard server node identity.
    public static let server = NodeIdentity(id: "server")

    /// Creates a random node identity.
    public static func random() -> NodeIdentity {
        NodeIdentity(id: UUID().uuidString)
    }

    public var description: String { id }
}

// MARK: - ExpressibleByStringLiteral

extension NodeIdentity: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(id: value)
    }
}
