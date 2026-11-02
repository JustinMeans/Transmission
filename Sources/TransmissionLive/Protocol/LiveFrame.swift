import Foundation

public enum LiveProtocol {
    public static let version: UInt64 = 1
    public static let subprotocol = "tx-live.1"
}

/// WebSocket close codes. See Docs/LiveProtocol.md.
public struct LiveCloseCode: RawRepresentable, Hashable, Sendable {
    public let rawValue: UInt16
    public init(rawValue: UInt16) { self.rawValue = rawValue }

    public static let normal = LiveCloseCode(rawValue: 1000)
    public static let slowConsumer = LiveCloseCode(rawValue: 4000)
    public static let updateRequired = LiveCloseCode(rawValue: 4001)
    public static let authFailed = LiveCloseCode(rawValue: 4002)
    public static let superseded = LiveCloseCode(rawValue: 4003)
    public static let protocolError = LiveCloseCode(rawValue: 4004)
    public static let deadPeer = LiveCloseCode(rawValue: 4005)
    public static let authExpired = LiveCloseCode(rawValue: 4006)
}

public enum LiveErrorCode: UInt64, Sendable, Equatable {
    case unspecified = 0
    case unknownTopic = 1
    case unsupportedSchema = 2
    case forbidden = 3
    case updateRequired = 4
    case tooManySubscriptions = 5
    case unknownAlias = 6
    case malformed = 7
    case authFailed = 8
}

public enum LiveGapReason: UInt8, Sendable, Equatable {
    case overflow = 0
    case outsideRing = 1
}

public struct TopicAdvert: Sendable, Equatable {
    public var name: String
    public var schemaMin: UInt64
    public var schemaMax: UInt64
    public var kind: TopicKind

    public init(name: String, schemaMin: UInt64, schemaMax: UInt64, kind: TopicKind) {
        self.name = name
        self.schemaMin = schemaMin
        self.schemaMax = schemaMax
        self.kind = kind
    }
}

public struct LiveCursor: Sendable, Equatable, Hashable {
    public var epoch: UInt64
    public var seq: UInt64

    public init(epoch: UInt64, seq: UInt64) {
        self.epoch = epoch
        self.seq = seq
    }
}

/// Every frame in protocol version 1.
public enum LiveFrame: Sendable, Equatable {
    case hello(protoMin: UInt64, protoMax: UInt64, build: String, clientID: String)
    case welcome(proto: UInt64, epoch: UInt64, serverTimeMs: UInt64, heartbeatMs: UInt64, maxFrame: UInt64, topics: [TopicAdvert])
    case sub(subID: UInt64, topic: String, params: [String: String], schema: UInt64, maxMilliHz: UInt64, resume: LiveCursor?)
    case unsub(subID: UInt64)
    case pause(subID: UInt64)
    case resume(subID: UInt64)
    case subAck(subID: UInt64, epoch: UInt64, headSeq: UInt64, schema: UInt64, replay: Bool)
    case snapshot(subID: UInt64, epoch: UInt64, seq: UInt64, schema: UInt64, part: UInt64, last: Bool, payload: Data)
    /// `span` is the number of sequences this delta covers, normally 1.
    case delta(subID: UInt64, seq: UInt64, sampleTimeMs: UInt64, span: UInt64, payload: Data)
    case gap(subID: UInt64, fromSeq: UInt64, toSeq: UInt64, droppedCount: UInt64, reason: LiveGapReason)
    case ack(subID: UInt64, seq: UInt64)
    case heartbeat(serverTimeMs: UInt64)
    case authRefresh(token: String)
    case authExpiring(secondsLeft: UInt64)
    case error(subID: UInt64, code: LiveErrorCode, retryable: Bool, message: String)
    case call(alias: String, callID: UInt64, payload: Data)
    case reply(callID: UInt64, isError: Bool, payload: Data)
    /// A frame type this build does not know. Receivers ignore it.
    case unknown(type: UInt8)
}
