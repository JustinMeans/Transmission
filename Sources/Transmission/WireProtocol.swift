import Foundation

/// Unique identifier for a remote call.
public struct CallID: Hashable, Sendable, Codable, CustomStringConvertible {
    public let value: UUID

    public init() {
        self.value = UUID()
    }

    public init(_ uuid: UUID) {
        self.value = uuid
    }

    public var description: String { value.uuidString }
}

/// Message priority for routing through priority queues.
/// Lower raw values indicate higher priority (realtime=0 sorts first).
/// Comparable: low < normal < high < realtime (higher priority compares greater).
public enum MessagePriority: Int, Sendable, Codable, Comparable {
    case realtime = 0
    case high = 1
    case normal = 2
    case low = 3

    public static func < (lhs: MessagePriority, rhs: MessagePriority) -> Bool {
        // Invert comparison: lower raw value = higher priority = compares greater
        lhs.rawValue > rhs.rawValue
    }
}

/// Top-level wire envelope for all messages.
public enum WireEnvelope: Sendable, Codable {
    case call(CallEnvelope)
    case reply(ReplyEnvelope)
    case close
}

/// Envelope for a remote method call.
public struct CallEnvelope: Sendable, Codable {
    public let callID: CallID
    public let recipient: ActorIdentity
    public let target: String
    public let genericSubs: [String]
    public let args: [Data]
    public let priority: MessagePriority

    public init(callID: CallID, recipient: ActorIdentity, target: String, genericSubs: [String], args: [Data], priority: MessagePriority) {
        self.callID = callID
        self.recipient = recipient
        self.target = target
        self.genericSubs = genericSubs
        self.args = args
        self.priority = priority
    }
}

/// Envelope for a call reply.
public struct ReplyEnvelope: Sendable, Codable {
    public let callID: CallID
    public let sender: ActorIdentity?
    public let value: Data

    public init(callID: CallID, sender: ActorIdentity?, value: Data) {
        self.callID = callID
        self.sender = sender
        self.value = value
    }
}
