import Foundation

public enum SerializationFormat: Sendable {
    case json
    case binary
}

public protocol BinarySerializable: Sendable {
    func serialize() -> Data
    static func deserialize(from data: Data) throws -> Self
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
