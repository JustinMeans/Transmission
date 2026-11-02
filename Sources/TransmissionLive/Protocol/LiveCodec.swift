import Foundation
import NIOCore

public enum LiveCodecError: Error, Equatable, Sendable {
    case truncated
    case varintOverflow
    case invalidUTF8
    case invalidValue(String)
}

enum FrameType {
    static let hello: UInt8 = 0x01
    static let welcome: UInt8 = 0x02
    static let sub: UInt8 = 0x03
    static let unsub: UInt8 = 0x04
    static let pause: UInt8 = 0x05
    static let resume: UInt8 = 0x06
    static let subAck: UInt8 = 0x07
    static let snapshot: UInt8 = 0x10
    static let delta: UInt8 = 0x11
    static let gap: UInt8 = 0x12
    static let ack: UInt8 = 0x13
    static let heartbeat: UInt8 = 0x20
    static let authRefresh: UInt8 = 0x21
    static let authExpiring: UInt8 = 0x22
    static let error: UInt8 = 0x30
    static let call: UInt8 = 0x31
    static let reply: UInt8 = 0x32
}

enum FrameFlag {
    static let resume: UInt8 = 0x01
    static let last: UInt8 = 0x01
    static let span: UInt8 = 0x01
    static let isError: UInt8 = 0x01
}

struct LiveWriter {
    private(set) var bytes: [UInt8] = []

    init(capacity: Int = 32) {
        bytes.reserveCapacity(capacity)
    }

    mutating func u8(_ value: UInt8) {
        bytes.append(value)
    }

    mutating func varint(_ value: UInt64) {
        var remaining = value
        while remaining >= 0x80 {
            bytes.append(UInt8(remaining & 0x7F) | 0x80)
            remaining >>= 7
        }
        bytes.append(UInt8(remaining))
    }

    mutating func string(_ value: String) {
        let utf8 = Array(value.utf8)
        varint(UInt64(utf8.count))
        bytes.append(contentsOf: utf8)
    }

    mutating func payload(_ value: Data) {
        varint(UInt64(value.count))
        bytes.append(contentsOf: value)
    }
}

struct LiveReader {
    private let bytes: [UInt8]
    private var offset = 0

    init(_ bytes: [UInt8]) {
        self.bytes = bytes
    }

    var remaining: Int { bytes.count - offset }

    mutating func u8() throws -> UInt8 {
        guard offset < bytes.count else { throw LiveCodecError.truncated }
        defer { offset += 1 }
        return bytes[offset]
    }

    mutating func varint() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        while true {
            let byte = try u8()
            if shift == 63, byte > 1 { throw LiveCodecError.varintOverflow }
            result |= UInt64(byte & 0x7F) << shift
            if byte & 0x80 == 0 { return result }
            shift += 7
            if shift > 63 { throw LiveCodecError.varintOverflow }
        }
    }

    private mutating func length() throws -> Int {
        let raw = try varint()
        guard raw <= UInt64(remaining) else { throw LiveCodecError.truncated }
        return Int(raw)
    }

    mutating func string() throws -> String {
        let count = try length()
        defer { offset += count }
        guard let value = String(validating: bytes[offset..<(offset + count)], as: UTF8.self) else {
            throw LiveCodecError.invalidUTF8
        }
        return value
    }

    mutating func payload() throws -> Data {
        let count = try length()
        defer { offset += count }
        return Data(bytes[offset..<(offset + count)])
    }

    /// A count of items that each take at least `minimumSize` bytes.
    mutating func count(minimumSize: Int) throws -> Int {
        let raw = try varint()
        guard raw <= UInt64(remaining / max(minimumSize, 1)) else { throw LiveCodecError.truncated }
        return Int(raw)
    }
}

extension String {
    /// `String(validating:as:)` is newer than the deployment targets.
    fileprivate init?(validating bytes: ArraySlice<UInt8>, as: UTF8.Type) {
        guard let decoded = String(bytes: bytes, encoding: .utf8) else { return nil }
        self = decoded
    }
}

/// A frame split into a per-subscriber prefix and a body encoded once.
///
/// The header holds `subID`, which differs per subscriber, so a whole frame can
/// never be shared. Everything after the header can: `body` is one `ByteBuffer`
/// referenced by every subscriber's queue, and only the few prefix bytes are
/// per connection.
public struct LiveWireFrame: Sendable {
    public let prefix: [UInt8]
    public let body: ByteBuffer

    public init(type: UInt8, flags: UInt8 = 0, subID: UInt64 = 0, body: ByteBuffer) {
        var writer = LiveWriter(capacity: 8)
        writer.u8(type)
        writer.u8(flags)
        writer.varint(subID)
        self.prefix = writer.bytes
        self.body = body
    }

    public var byteCount: Int { prefix.count + body.readableBytes }

    /// The whole message, for a transport that cannot gather two buffers.
    public var assembled: [UInt8] {
        var bytes = [UInt8]()
        bytes.reserveCapacity(byteCount)
        bytes.append(contentsOf: prefix)
        bytes.append(contentsOf: body.readableBytesView)
        return bytes
    }
}

public enum LiveCodec {
    public static func encode(_ frame: LiveFrame) -> Data {
        Data(encodeBytes(frame))
    }

    static func encodeBytes(_ frame: LiveFrame) -> [UInt8] {
        let (type, flags, subID, body) = parts(frame)
        var writer = LiveWriter(capacity: body.count + 8)
        writer.u8(type)
        writer.u8(flags)
        writer.varint(subID)
        return writer.bytes + body
    }

    static func wireFrame(_ frame: LiveFrame) -> LiveWireFrame {
        let (type, flags, subID, body) = parts(frame)
        return LiveWireFrame(type: type, flags: flags, subID: subID, body: ByteBuffer(bytes: body))
    }

    private static func parts(_ frame: LiveFrame) -> (UInt8, UInt8, UInt64, [UInt8]) {
        var w = LiveWriter()
        switch frame {
        case let .hello(protoMin, protoMax, build, clientID):
            w.varint(protoMin)
            w.varint(protoMax)
            w.string(build)
            w.string(clientID)
            return (FrameType.hello, 0, 0, w.bytes)
        case let .welcome(proto, epoch, serverTimeMs, heartbeatMs, maxFrame, topics):
            w.varint(proto)
            w.varint(epoch)
            w.varint(serverTimeMs)
            w.varint(heartbeatMs)
            w.varint(maxFrame)
            w.varint(UInt64(topics.count))
            for topic in topics {
                w.string(topic.name)
                w.varint(topic.schemaMin)
                w.varint(topic.schemaMax)
                w.u8(topic.kind.rawValue)
            }
            return (FrameType.welcome, 0, 0, w.bytes)
        case let .sub(subID, topic, params, schema, maxMilliHz, resume):
            w.string(topic)
            w.varint(UInt64(params.count))
            for key in params.keys.sorted() {
                w.string(key)
                w.string(params[key]!)
            }
            w.varint(schema)
            w.varint(maxMilliHz)
            if let resume {
                w.varint(resume.epoch)
                w.varint(resume.seq)
            }
            return (FrameType.sub, resume == nil ? 0 : FrameFlag.resume, subID, w.bytes)
        case let .unsub(subID):
            return (FrameType.unsub, 0, subID, [])
        case let .pause(subID):
            return (FrameType.pause, 0, subID, [])
        case let .resume(subID):
            return (FrameType.resume, 0, subID, [])
        case let .subAck(subID, epoch, headSeq, schema, replay):
            w.varint(epoch)
            w.varint(headSeq)
            w.varint(schema)
            w.u8(replay ? 1 : 0)
            return (FrameType.subAck, 0, subID, w.bytes)
        case let .snapshot(subID, epoch, seq, schema, part, last, payload):
            w.varint(epoch)
            w.varint(seq)
            w.varint(schema)
            w.varint(part)
            w.payload(payload)
            return (FrameType.snapshot, last ? FrameFlag.last : 0, subID, w.bytes)
        case let .delta(subID, seq, sampleTimeMs, span, payload):
            w.varint(seq)
            w.varint(sampleTimeMs)
            if span != 1 { w.varint(span) }
            w.payload(payload)
            return (FrameType.delta, span != 1 ? FrameFlag.span : 0, subID, w.bytes)
        case let .gap(subID, fromSeq, toSeq, droppedCount, reason):
            w.varint(fromSeq)
            w.varint(toSeq)
            w.varint(droppedCount)
            w.u8(reason.rawValue)
            return (FrameType.gap, 0, subID, w.bytes)
        case let .ack(subID, seq):
            w.varint(seq)
            return (FrameType.ack, 0, subID, w.bytes)
        case let .heartbeat(serverTimeMs):
            w.varint(serverTimeMs)
            return (FrameType.heartbeat, 0, 0, w.bytes)
        case let .authRefresh(token):
            w.string(token)
            return (FrameType.authRefresh, 0, 0, w.bytes)
        case let .authExpiring(secondsLeft):
            w.varint(secondsLeft)
            return (FrameType.authExpiring, 0, 0, w.bytes)
        case let .error(subID, code, retryable, message):
            w.varint(code.rawValue)
            w.u8(retryable ? 1 : 0)
            w.string(message)
            return (FrameType.error, 0, subID, w.bytes)
        case let .call(alias, callID, payload):
            w.string(alias)
            w.varint(callID)
            w.payload(payload)
            return (FrameType.call, 0, 0, w.bytes)
        case let .reply(callID, isError, payload):
            w.varint(callID)
            w.payload(payload)
            return (FrameType.reply, isError ? FrameFlag.isError : 0, 0, w.bytes)
        case let .unknown(type):
            return (type, 0, 0, [])
        }
    }

    /// Decodes one message. Unknown frame types become `.unknown`, and bytes
    /// after a known frame's fields are ignored: both are required for evolution.
    public static func decode(_ data: Data) throws -> LiveFrame {
        try decode(Array(data))
    }

    public static func decode(_ bytes: [UInt8]) throws -> LiveFrame {
        var r = LiveReader(bytes)
        let type = try r.u8()
        let flags = try r.u8()
        let subID = try r.varint()
        switch type {
        case FrameType.hello:
            return .hello(protoMin: try r.varint(), protoMax: try r.varint(), build: try r.string(), clientID: try r.string())
        case FrameType.welcome:
            let proto = try r.varint()
            let epoch = try r.varint()
            let serverTimeMs = try r.varint()
            let heartbeatMs = try r.varint()
            let maxFrame = try r.varint()
            let count = try r.count(minimumSize: 4)
            var topics: [TopicAdvert] = []
            topics.reserveCapacity(count)
            for _ in 0..<count {
                let name = try r.string()
                let schemaMin = try r.varint()
                let schemaMax = try r.varint()
                let kind = TopicKind(rawValue: try r.u8()) ?? .state
                topics.append(TopicAdvert(name: name, schemaMin: schemaMin, schemaMax: schemaMax, kind: kind))
            }
            return .welcome(proto: proto, epoch: epoch, serverTimeMs: serverTimeMs, heartbeatMs: heartbeatMs, maxFrame: maxFrame, topics: topics)
        case FrameType.sub:
            let topic = try r.string()
            let count = try r.count(minimumSize: 2)
            var params: [String: String] = [:]
            for _ in 0..<count {
                let key = try r.string()
                params[key] = try r.string()
            }
            let schema = try r.varint()
            let maxMilliHz = try r.varint()
            var resume: LiveCursor?
            if flags & FrameFlag.resume != 0 {
                resume = LiveCursor(epoch: try r.varint(), seq: try r.varint())
            }
            return .sub(subID: subID, topic: topic, params: params, schema: schema, maxMilliHz: maxMilliHz, resume: resume)
        case FrameType.unsub:
            return .unsub(subID: subID)
        case FrameType.pause:
            return .pause(subID: subID)
        case FrameType.resume:
            return .resume(subID: subID)
        case FrameType.subAck:
            let epoch = try r.varint()
            let headSeq = try r.varint()
            let schema = try r.varint()
            return .subAck(subID: subID, epoch: epoch, headSeq: headSeq, schema: schema, replay: try r.u8() == 1)
        case FrameType.snapshot:
            let epoch = try r.varint()
            let seq = try r.varint()
            let schema = try r.varint()
            let part = try r.varint()
            return .snapshot(subID: subID, epoch: epoch, seq: seq, schema: schema, part: part, last: flags & FrameFlag.last != 0, payload: try r.payload())
        case FrameType.delta:
            let seq = try r.varint()
            let sampleTimeMs = try r.varint()
            let span = flags & FrameFlag.span != 0 ? try r.varint() : 1
            return .delta(subID: subID, seq: seq, sampleTimeMs: sampleTimeMs, span: span, payload: try r.payload())
        case FrameType.gap:
            let fromSeq = try r.varint()
            let toSeq = try r.varint()
            let droppedCount = try r.varint()
            return .gap(subID: subID, fromSeq: fromSeq, toSeq: toSeq, droppedCount: droppedCount, reason: LiveGapReason(rawValue: try r.u8()) ?? .overflow)
        case FrameType.ack:
            return .ack(subID: subID, seq: try r.varint())
        case FrameType.heartbeat:
            return .heartbeat(serverTimeMs: try r.varint())
        case FrameType.authRefresh:
            return .authRefresh(token: try r.string())
        case FrameType.authExpiring:
            return .authExpiring(secondsLeft: try r.varint())
        case FrameType.error:
            let code = LiveErrorCode(rawValue: try r.varint()) ?? .unspecified
            let retryable = try r.u8() == 1
            return .error(subID: subID, code: code, retryable: retryable, message: try r.string())
        case FrameType.call:
            return .call(alias: try r.string(), callID: try r.varint(), payload: try r.payload())
        case FrameType.reply:
            return .reply(callID: try r.varint(), isError: flags & FrameFlag.isError != 0, payload: try r.payload())
        default:
            return .unknown(type: type)
        }
    }
}
