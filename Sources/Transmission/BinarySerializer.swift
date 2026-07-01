import Foundation

public enum SerializationFormat: Sendable {
    case json
    case binary
}

public protocol BinarySerializable: Sendable {
    func serialize() -> Data
    static func deserialize(from data: Data) throws -> Self
}

public struct CompactEncoder: Sendable {
    private var buffer: [UInt8] = []

    public init() {}

    public mutating func writeUInt8(_ value: UInt8) {
        buffer.append(value)
    }

    public mutating func writeVarint(_ value: UInt64) {
        var v = value
        while v >= 0x80 {
            buffer.append(UInt8(v & 0x7F) | 0x80)
            v >>= 7
        }
        buffer.append(UInt8(v))
    }

    public mutating func writeVarintSigned(_ value: Int64) {
        let unsigned = UInt64(bitPattern: (value << 1) ^ (value >> 63))
        writeVarint(unsigned)
    }

    public mutating func writeBytes(_ data: Data) {
        writeVarint(UInt64(data.count))
        buffer.append(contentsOf: data)
    }

    public mutating func writeString(_ string: String) {
        let data = Data(string.utf8)
        writeBytes(data)
    }

    public mutating func writeUUID(_ uuid: UUID) {
        let (b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15) = uuid.uuid
        buffer.append(contentsOf: [b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15])
    }

    public mutating func writeOptionalString(_ string: String?) {
        if let s = string {
            writeUInt8(1)
            writeString(s)
        } else {
            writeUInt8(0)
        }
    }

    public var data: Data {
        Data(buffer)
    }
}

public struct CompactDecoder: Sendable {
    private let buffer: [UInt8]
    private var offset: Int = 0

    public init(_ data: Data) {
        self.buffer = Array(data)
    }

    public var bytesRemaining: Int {
        buffer.count - offset
    }

    public mutating func readUInt8() throws -> UInt8 {
        guard offset < buffer.count else {
            throw TransmissionError.decodingFailed("Unexpected end of data")
        }
        let value = buffer[offset]
        offset += 1
        return value
    }

    public mutating func readVarint() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        while true {
            guard offset < buffer.count else {
                throw TransmissionError.decodingFailed("Incomplete varint")
            }
            let byte = buffer[offset]
            offset += 1
            // At shift=63 only bit 63 is valid for a UInt64. Reject any 7-bit
            // value > 1 at this position — it would silently overflow (or trap
            // in a debug build) rather than being detected as out-of-range.
            if shift == 63, byte & 0x7F > 1 {
                throw TransmissionError.decodingFailed("Varint overflow: value exceeds UInt64.max")
            }
            result |= UInt64(byte & 0x7F) << shift
            if byte & 0x80 == 0 {
                break
            }
            shift += 7
            if shift >= 64 {
                throw TransmissionError.decodingFailed("Varint overflow")
            }
        }
        return result
    }

    public mutating func readVarintSigned() throws -> Int64 {
        let unsigned = try readVarint()
        let signed = Int64(bitPattern: (unsigned >> 1) ^ (0 &- (unsigned & 1)))
        return signed
    }

    public mutating func readBytes() throws -> Data {
        let rawLength = try readVarint()
        guard rawLength <= UInt64(Int.max) else {
            throw TransmissionError.decodingFailed("Data length varint overflows Int: \(rawLength)")
        }
        let length = Int(rawLength)
        // Compare as `length <= buffer.count - offset` to avoid integer overflow
        // when `length` is large (e.g. Int.max) and `offset` is non-zero.
        guard length <= buffer.count - offset else {
            throw TransmissionError.decodingFailed("Insufficient bytes for data")
        }
        let data = Data(buffer[offset..<(offset + length)])
        offset += length
        return data
    }

    public mutating func readString() throws -> String {
        let data = try readBytes()
        guard let string = String(data: data, encoding: .utf8) else {
            throw TransmissionError.decodingFailed("Invalid UTF-8 string")
        }
        return string
    }

    public mutating func readUUID() throws -> UUID {
        guard offset + 16 <= buffer.count else {
            throw TransmissionError.decodingFailed("Insufficient bytes for UUID")
        }
        let bytes = Array(buffer[offset..<(offset + 16)])
        offset += 16
        let uuid = UUID(uuid: (bytes[0], bytes[1], bytes[2], bytes[3],
                               bytes[4], bytes[5], bytes[6], bytes[7],
                               bytes[8], bytes[9], bytes[10], bytes[11],
                               bytes[12], bytes[13], bytes[14], bytes[15]))
        return uuid
    }

    public mutating func readOptionalString() throws -> String? {
        let hasValue = try readUInt8()
        switch hasValue {
        case 0:
            return nil
        case 1:
            return try readString()
        default:
            throw TransmissionError.decodingFailed("Invalid optional presence byte: \(hasValue); expected 0 or 1")
        }
    }
}

private enum WireEnvelopeType: UInt8 {
    case call = 0
    case reply = 1
    case close = 2
}

extension WireEnvelope {

    public func encodeCompact() -> Data {
        var encoder = CompactEncoder()

        switch self {
        case .call(let call):
            encoder.writeUInt8(WireEnvelopeType.call.rawValue)
            encoder.writeUUID(call.callID.value)
            encoder.writeString(call.recipient.id)
            encoder.writeOptionalString(call.recipient.node?.id)
            encoder.writeString(call.target)
            encoder.writeUInt8(UInt8(call.priority.rawValue))

            encoder.writeVarint(UInt64(call.genericSubs.count))
            for sub in call.genericSubs {
                encoder.writeString(sub)
            }

            encoder.writeVarint(UInt64(call.args.count))
            for arg in call.args {
                encoder.writeBytes(arg)
            }

        case .reply(let reply):
            encoder.writeUInt8(WireEnvelopeType.reply.rawValue)
            encoder.writeUUID(reply.callID.value)

            if let sender = reply.sender {
                encoder.writeUInt8(1)
                encoder.writeString(sender.id)
                encoder.writeOptionalString(sender.node?.id)
            } else {
                encoder.writeUInt8(0)
            }

            encoder.writeBytes(reply.value)

        case .close:
            encoder.writeUInt8(WireEnvelopeType.close.rawValue)
        }

        return encoder.data
    }

    public static func decodeCompact(from data: Data) throws -> WireEnvelope {
        var decoder = CompactDecoder(data)

        let typeRaw = try decoder.readUInt8()
        guard let type = WireEnvelopeType(rawValue: typeRaw) else {
            throw TransmissionError.decodingFailed("Invalid envelope type: \(typeRaw)")
        }

        switch type {
        case .call:
            let callID = CallID(try decoder.readUUID())
            let actorID = try decoder.readString()
            let nodeID = try decoder.readOptionalString()

            let node = nodeID.map { NodeIdentity(id: $0) }
            let recipient = ActorIdentity(id: actorID, node: node)

            let target = try decoder.readString()
            let priorityRaw = try decoder.readUInt8()
            let priority = MessagePriority(rawValue: Int(priorityRaw)) ?? .normal

            let rawSubsCount = try decoder.readVarint()
            guard rawSubsCount <= UInt64(Int.max) else {
                throw TransmissionError.decodingFailed("Generic substitution count overflows Int: \(rawSubsCount)")
            }
            let subsCount = Int(rawSubsCount)
            // Each substitution is at minimum a 1-byte varint length (an empty
            // string). A declared count larger than the bytes left in the buffer
            // is therefore provably unsatisfiable. Reject it before reserving
            // capacity so a tiny adversarial frame cannot drive a multi-gigabyte
            // allocation (OOM/DoS) on the inbound transport path.
            guard subsCount <= decoder.bytesRemaining else {
                throw TransmissionError.decodingFailed("Generic substitution count \(subsCount) exceeds remaining bytes \(decoder.bytesRemaining)")
            }
            var genericSubs: [String] = []
            genericSubs.reserveCapacity(subsCount)
            for _ in 0..<subsCount {
                genericSubs.append(try decoder.readString())
            }

            let rawArgsCount = try decoder.readVarint()
            guard rawArgsCount <= UInt64(Int.max) else {
                throw TransmissionError.decodingFailed("Argument count overflows Int: \(rawArgsCount)")
            }
            let argsCount = Int(rawArgsCount)
            // Each argument is at minimum a 1-byte varint length (empty Data).
            // Same bound as genericSubs: reject any count larger than the bytes
            // left so reserveCapacity cannot be coerced into a huge allocation.
            guard argsCount <= decoder.bytesRemaining else {
                throw TransmissionError.decodingFailed("Argument count \(argsCount) exceeds remaining bytes \(decoder.bytesRemaining)")
            }
            var args: [Data] = []
            args.reserveCapacity(argsCount)
            for _ in 0..<argsCount {
                args.append(try decoder.readBytes())
            }

            let envelope = CallEnvelope(
                callID: callID,
                recipient: recipient,
                target: target,
                genericSubs: genericSubs,
                args: args,
                priority: priority
            )
            // A `.call` is fully length-described by its own fields: once the args
            // array closes the structure there must be nothing left. Any residual
            // bytes mean the frame is malformed (corruption) or that a second,
            // smuggled payload has been appended after a structurally-complete
            // envelope. Reject it rather than silently discarding the tail — a
            // permissive decoder lets an attacker hide bytes the framing layer
            // believes it has accounted for (frame-ambiguity / integrity evasion).
            guard decoder.bytesRemaining == 0 else {
                throw TransmissionError.decodingFailed("Trailing \(decoder.bytesRemaining) byte(s) after call envelope")
            }
            return .call(envelope)

        case .reply:
            let callID = CallID(try decoder.readUUID())
            let hasSender = try decoder.readUInt8() == 1

            var sender: ActorIdentity?
            if hasSender {
                let actorID = try decoder.readString()
                let nodeID = try decoder.readOptionalString()
                let node = nodeID.map { NodeIdentity(id: $0) }
                sender = ActorIdentity(id: actorID, node: node)
            }

            let value = try decoder.readBytes()
            let envelope = ReplyEnvelope(callID: callID, sender: sender, value: value)
            // Same strict-consumption contract as `.call`: the reply's length-
            // prefixed `value` closes the structure, so residual bytes indicate a
            // malformed or smuggled frame and must be rejected, not ignored.
            guard decoder.bytesRemaining == 0 else {
                throw TransmissionError.decodingFailed("Trailing \(decoder.bytesRemaining) byte(s) after reply envelope")
            }
            return .reply(envelope)

        case .close:
            return .close
        }
    }
}

public struct BinaryEncoder: Sendable {
    public init() {}

    public func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = []
        return try encoder.encode(value)
    }

    public func encodeBinary<T: BinarySerializable>(_ value: T) -> Data {
        value.serialize()
    }
}

public struct BinaryDecoder: Sendable {
    public init() {}

    public func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let decoder = JSONDecoder()
        return try decoder.decode(type, from: data)
    }

    public func decodeBinary<T: BinarySerializable>(_ type: T.Type, from data: Data) throws -> T {
        try type.deserialize(from: data)
    }
}

extension Data: BinarySerializable {
    public func serialize() -> Data { self }
    public static func deserialize(from data: Data) throws -> Data { data }
}

extension String: BinarySerializable {
    public func serialize() -> Data {
        data(using: .utf8) ?? Data()
    }

    public static func deserialize(from data: Data) throws -> String {
        guard let string = String(data: data, encoding: .utf8) else {
            throw TransmissionError.decodingFailed("Invalid UTF-8 string")
        }
        return string
    }
}
