import Testing
import Foundation
@testable import Transmission

@Suite("BinarySerializer Tests")
struct BinarySerializerTests {

    @Test("BinaryEncoder encodes Codable types")
    func encodesCodable() throws {
        let encoder = BinaryEncoder()

        struct TestData: Codable, Equatable {
            let name: String
            let value: Int
        }

        let original = TestData(name: "test", value: 42)
        let data = try encoder.encode(original)

        let decoder = BinaryDecoder()
        let decoded = try decoder.decode(TestData.self, from: data)

        #expect(decoded == original)
    }

    @Test("String BinarySerializable round-trip")
    func stringRoundTrip() throws {
        let original = "Hello, Transmission!"
        let data = original.serialize()
        let decoded = try String.deserialize(from: data)

        #expect(decoded == original)
    }

    @Test("Data BinarySerializable round-trip")
    func dataRoundTrip() throws {
        let original = Data([0x01, 0x02, 0x03, 0x04])
        let serialized = original.serialize()
        let decoded = try Data.deserialize(from: serialized)

        #expect(decoded == original)
    }

    @Test("Empty string serialization")
    func emptyStringSerialization() throws {
        let original = ""
        let data = original.serialize()
        let decoded = try String.deserialize(from: data)

        #expect(decoded == original)
    }

    @Test("Unicode string serialization")
    func unicodeStringSerialization() throws {
        let original = "Hello 🌍 世界 مرحبا"
        let data = original.serialize()
        let decoded = try String.deserialize(from: data)

        #expect(decoded == original)
    }

    @Test("Complex nested structure encoding")
    func complexStructureEncoding() throws {
        struct Inner: Codable, Equatable {
            let id: Int
            let name: String
        }

        struct Outer: Codable, Equatable {
            let items: [Inner]
            let metadata: [String: String]
        }

        let encoder = BinaryEncoder()
        let decoder = BinaryDecoder()

        let original = Outer(
            items: [
                Inner(id: 1, name: "First"),
                Inner(id: 2, name: "Second")
            ],
            metadata: ["version": "1.0", "author": "Test"]
        )

        let data = try encoder.encode(original)
        let decoded = try decoder.decode(Outer.self, from: data)

        #expect(decoded == original)
    }

    @Test("Array encoding")
    func arrayEncoding() throws {
        let encoder = BinaryEncoder()
        let decoder = BinaryDecoder()

        let original = [1, 2, 3, 4, 5]
        let data = try encoder.encode(original)
        let decoded = try decoder.decode([Int].self, from: data)

        #expect(decoded == original)
    }

    @Test("Optional encoding")
    func optionalEncoding() throws {
        let encoder = BinaryEncoder()
        let decoder = BinaryDecoder()

        struct WithOptional: Codable, Equatable {
            let required: String
            let optional: Int?
        }

        let withValue = WithOptional(required: "test", optional: 42)
        let withoutValue = WithOptional(required: "test", optional: nil)

        let data1 = try encoder.encode(withValue)
        let data2 = try encoder.encode(withoutValue)

        let decoded1 = try decoder.decode(WithOptional.self, from: data1)
        let decoded2 = try decoder.decode(WithOptional.self, from: data2)

        #expect(decoded1 == withValue)
        #expect(decoded2 == withoutValue)
    }
}
