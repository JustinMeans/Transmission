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
        let original = "Hello World"
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

@Suite("Compact Binary Protocol Tests")
struct CompactBinaryProtocolTests {

    @Test("Varint encoding small values")
    func varintSmallValues() throws {
        var encoder = CompactEncoder()
        encoder.writeVarint(0)
        encoder.writeVarint(1)
        encoder.writeVarint(127)

        var decoder = CompactDecoder(encoder.data)
        #expect(try decoder.readVarint() == 0)
        #expect(try decoder.readVarint() == 1)
        #expect(try decoder.readVarint() == 127)
        #expect(encoder.data.count == 3)
    }

    @Test("Varint encoding multi-byte values")
    func varintMultiByteValues() throws {
        var encoder = CompactEncoder()
        encoder.writeVarint(128)
        encoder.writeVarint(16383)
        encoder.writeVarint(16384)
        encoder.writeVarint(UInt64.max)

        var decoder = CompactDecoder(encoder.data)
        #expect(try decoder.readVarint() == 128)
        #expect(try decoder.readVarint() == 16383)
        #expect(try decoder.readVarint() == 16384)
        #expect(try decoder.readVarint() == UInt64.max)
    }

    @Test("Signed varint encoding")
    func signedVarintEncoding() throws {
        var encoder = CompactEncoder()
        encoder.writeVarintSigned(0)
        encoder.writeVarintSigned(1)
        encoder.writeVarintSigned(-1)
        encoder.writeVarintSigned(100)
        encoder.writeVarintSigned(-100)
        encoder.writeVarintSigned(Int64.max)
        encoder.writeVarintSigned(Int64.min)

        var decoder = CompactDecoder(encoder.data)
        #expect(try decoder.readVarintSigned() == 0)
        #expect(try decoder.readVarintSigned() == 1)
        #expect(try decoder.readVarintSigned() == -1)
        #expect(try decoder.readVarintSigned() == 100)
        #expect(try decoder.readVarintSigned() == -100)
        #expect(try decoder.readVarintSigned() == Int64.max)
        #expect(try decoder.readVarintSigned() == Int64.min)
    }

    @Test("UUID encoding uses raw bytes")
    func uuidRawBytes() throws {
        let uuid = UUID()
        var encoder = CompactEncoder()
        encoder.writeUUID(uuid)

        #expect(encoder.data.count == 16)

        var decoder = CompactDecoder(encoder.data)
        let decoded = try decoder.readUUID()
        #expect(decoded == uuid)
    }

    @Test("String encoding with length prefix")
    func stringLengthPrefix() throws {
        let testString = "Hello, Transmission!"
        var encoder = CompactEncoder()
        encoder.writeString(testString)

        var decoder = CompactDecoder(encoder.data)
        let decoded = try decoder.readString()
        #expect(decoded == testString)
    }

    @Test("CallEnvelope compact round-trip")
    func callEnvelopeRoundTrip() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "test-actor", node: NodeIdentity(id: "test-node")),
            target: "doSomething(with:)",
            genericSubs: ["Swift.String", "Swift.Int"],
            args: [Data([1, 2, 3]), Data([4, 5, 6])],
            priority: .high
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .call(let decodedCall) = decoded {
            #expect(decodedCall.callID == envelope.callID)
            #expect(decodedCall.recipient.id == envelope.recipient.id)
            #expect(decodedCall.recipient.node?.id == envelope.recipient.node?.id)
            #expect(decodedCall.target == envelope.target)
            #expect(decodedCall.genericSubs == envelope.genericSubs)
            #expect(decodedCall.args == envelope.args)
            #expect(decodedCall.priority == envelope.priority)
        } else {
            Issue.record("Expected call envelope")
        }
    }

    @Test("ReplyEnvelope compact round-trip")
    func replyEnvelopeRoundTrip() throws {
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: ActorIdentity(id: "responder", node: NodeIdentity(id: "server")),
            value: Data([0xDE, 0xAD, 0xBE, 0xEF])
        )

        let wireEnvelope = WireEnvelope.reply(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .reply(let decodedReply) = decoded {
            #expect(decodedReply.callID == envelope.callID)
            #expect(decodedReply.sender?.id == envelope.sender?.id)
            #expect(decodedReply.sender?.node?.id == envelope.sender?.node?.id)
            #expect(decodedReply.value == envelope.value)
        } else {
            Issue.record("Expected reply envelope")
        }
    }

    @Test("Close envelope compact round-trip")
    func closeEnvelopeRoundTrip() throws {
        let encoded = WireEnvelope.close.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .close = decoded {
            #expect(encoded.count == 1)
        } else {
            Issue.record("Expected close envelope")
        }
    }

    @Test("Compact encoding is smaller than JSON")
    func compactSmallerThanJSON() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment()",
            genericSubs: [],
            args: [],
            priority: .normal
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let compactData = wireEnvelope.encodeCompact()
        let jsonData = try JSONEncoder().encode(wireEnvelope)

        #expect(compactData.count < jsonData.count)
    }

    @Test("Empty args and genericSubs")
    func emptyArgsAndGenericSubs() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "actor"),
            target: "method()",
            genericSubs: [],
            args: [],
            priority: .normal
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .call(let decodedCall) = decoded {
            #expect(decodedCall.genericSubs.isEmpty)
            #expect(decodedCall.args.isEmpty)
        } else {
            Issue.record("Expected call envelope")
        }
    }

    @Test("Reply with nil sender")
    func replyNilSender() throws {
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: nil,
            value: Data([1, 2, 3])
        )

        let wireEnvelope = WireEnvelope.reply(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .reply(let decodedReply) = decoded {
            #expect(decodedReply.sender == nil)
        } else {
            Issue.record("Expected reply envelope")
        }
    }

    @Test("Call with nil node")
    func callNilNode() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "actor"),
            target: "method()",
            genericSubs: [],
            args: [],
            priority: .realtime
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .call(let decodedCall) = decoded {
            #expect(decodedCall.recipient.node == nil)
            #expect(decodedCall.priority == .realtime)
        } else {
            Issue.record("Expected call envelope")
        }
    }

    @Test("All priority levels")
    func allPriorityLevels() throws {
        let priorities: [MessagePriority] = [.realtime, .high, .normal, .low]

        for priority in priorities {
            let envelope = CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "actor"),
                target: "method()",
                genericSubs: [],
                args: [],
                priority: priority
            )

            let wireEnvelope = WireEnvelope.call(envelope)
            let encoded = wireEnvelope.encodeCompact()
            let decoded = try WireEnvelope.decodeCompact(from: encoded)

            if case .call(let decodedCall) = decoded {
                #expect(decodedCall.priority == priority)
            } else {
                Issue.record("Expected call envelope for priority \(priority)")
            }
        }
    }

    @Test("Large payload encoding")
    func largePayloadEncoding() throws {
        let largeData = Data(repeating: 0xAB, count: 100_000)
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: nil,
            value: largeData
        )

        let wireEnvelope = WireEnvelope.reply(envelope)
        let encoded = wireEnvelope.encodeCompact()
        let decoded = try WireEnvelope.decodeCompact(from: encoded)

        if case .reply(let decodedReply) = decoded {
            #expect(decodedReply.value == largeData)
        } else {
            Issue.record("Expected reply envelope")
        }
    }
}

@Suite("Wire Protocol Benchmarks")
struct WireProtocolBenchmarks {

    static let iterations = 10_000

    // MARK: - Size Benchmarks

    @Test("Size comparison: minimal call envelope")
    func sizeComparisonMinimalCall() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "c", node: NodeIdentity(id: "s")),
            target: "m()",
            genericSubs: [],
            args: [],
            priority: .normal
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let compactSize = wireEnvelope.encodeCompact().count
        let jsonSize = try JSONEncoder().encode(wireEnvelope).count

        let ratio = Double(jsonSize) / Double(compactSize)
        print("Minimal call - Compact: \(compactSize) bytes, JSON: \(jsonSize) bytes, Ratio: \(String(format: "%.2f", ratio))x")

        #expect(compactSize < jsonSize)
        #expect(ratio > 2.0)
    }

    @Test("Size comparison: typical call envelope")
    func sizeComparisonTypicalCall() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment(by:)",
            genericSubs: [],
            args: [try JSONEncoder().encode(42)],
            priority: .normal
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let compactSize = wireEnvelope.encodeCompact().count
        let jsonSize = try JSONEncoder().encode(wireEnvelope).count

        let ratio = Double(jsonSize) / Double(compactSize)
        print("Typical call - Compact: \(compactSize) bytes, JSON: \(jsonSize) bytes, Ratio: \(String(format: "%.2f", ratio))x")

        #expect(compactSize < jsonSize)
    }

    @Test("Size comparison: complex call envelope")
    func sizeComparisonComplexCall() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "data-processor-service", node: NodeIdentity(id: "worker-node-001")),
            target: "processData(input:options:callback:)",
            genericSubs: ["Swift.String", "Swift.Int", "Swift.Array<Swift.UInt8>"],
            args: [
                try JSONEncoder().encode(["key1": "value1", "key2": "value2"]),
                try JSONEncoder().encode([1, 2, 3, 4, 5]),
                Data(repeating: 0x00, count: 1000)
            ],
            priority: .high
        )

        let wireEnvelope = WireEnvelope.call(envelope)
        let compactSize = wireEnvelope.encodeCompact().count
        let jsonSize = try JSONEncoder().encode(wireEnvelope).count

        let ratio = Double(jsonSize) / Double(compactSize)
        print("Complex call - Compact: \(compactSize) bytes, JSON: \(jsonSize) bytes, Ratio: \(String(format: "%.2f", ratio))x")

        #expect(compactSize < jsonSize)
    }

    @Test("Size comparison: reply envelope")
    func sizeComparisonReply() throws {
        struct Response: Codable {
            let result: String
            let count: Int
        }
        let responseData = try JSONEncoder().encode(Response(result: "success", count: 42))
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            value: responseData
        )

        let wireEnvelope = WireEnvelope.reply(envelope)
        let compactSize = wireEnvelope.encodeCompact().count
        let jsonSize = try JSONEncoder().encode(wireEnvelope).count

        let ratio = Double(jsonSize) / Double(compactSize)
        print("Reply - Compact: \(compactSize) bytes, JSON: \(jsonSize) bytes, Ratio: \(String(format: "%.2f", ratio))x")

        #expect(compactSize < jsonSize)
    }

    // MARK: - Throughput Benchmarks

    @Test("Encoding throughput: compact vs JSON")
    func encodingThroughput() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment()",
            genericSubs: [],
            args: [],
            priority: .normal
        )
        let wireEnvelope = WireEnvelope.call(envelope)

        let jsonEncoder = JSONEncoder()

        let compactStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            _ = wireEnvelope.encodeCompact()
        }
        let compactDuration = Date().timeIntervalSinceReferenceDate - compactStart

        let jsonStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            _ = try jsonEncoder.encode(wireEnvelope)
        }
        let jsonDuration = Date().timeIntervalSinceReferenceDate - jsonStart

        let speedup = jsonDuration / compactDuration
        print("Encoding \(Self.iterations) envelopes - Compact: \(String(format: "%.3f", compactDuration))s, JSON: \(String(format: "%.3f", jsonDuration))s, Speedup: \(String(format: "%.2f", speedup))x")

        #expect(compactDuration < jsonDuration)
    }

    @Test("Decoding throughput: compact vs JSON")
    func decodingThroughput() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment()",
            genericSubs: [],
            args: [],
            priority: .normal
        )
        let wireEnvelope = WireEnvelope.call(envelope)

        let compactData = wireEnvelope.encodeCompact()
        let jsonData = try JSONEncoder().encode(wireEnvelope)

        let compactStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            _ = try WireEnvelope.decodeCompact(from: compactData)
        }
        let compactDuration = Date().timeIntervalSinceReferenceDate - compactStart

        let jsonDecoder = JSONDecoder()
        let jsonStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            _ = try jsonDecoder.decode(WireEnvelope.self, from: jsonData)
        }
        let jsonDuration = Date().timeIntervalSinceReferenceDate - jsonStart

        let speedup = jsonDuration / compactDuration
        print("Decoding \(Self.iterations) envelopes - Compact: \(String(format: "%.3f", compactDuration))s, JSON: \(String(format: "%.3f", jsonDuration))s, Speedup: \(String(format: "%.2f", speedup))x")

        #expect(compactDuration < jsonDuration)
    }

    @Test("Round-trip throughput: compact vs JSON")
    func roundTripThroughput() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment(by:limit:)",
            genericSubs: ["Swift.Int"],
            args: [try JSONEncoder().encode(10), try JSONEncoder().encode(100)],
            priority: .high
        )
        let wireEnvelope = WireEnvelope.call(envelope)

        let jsonEncoder = JSONEncoder()
        let jsonDecoder = JSONDecoder()

        let compactStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            let data = wireEnvelope.encodeCompact()
            _ = try WireEnvelope.decodeCompact(from: data)
        }
        let compactDuration = Date().timeIntervalSinceReferenceDate - compactStart

        let jsonStart = Date().timeIntervalSinceReferenceDate
        for _ in 0..<Self.iterations {
            let data = try jsonEncoder.encode(wireEnvelope)
            _ = try jsonDecoder.decode(WireEnvelope.self, from: data)
        }
        let jsonDuration = Date().timeIntervalSinceReferenceDate - jsonStart

        let speedup = jsonDuration / compactDuration
        print("Round-trip \(Self.iterations) envelopes - Compact: \(String(format: "%.3f", compactDuration))s, JSON: \(String(format: "%.3f", jsonDuration))s, Speedup: \(String(format: "%.2f", speedup))x")

        #expect(compactDuration < jsonDuration)
    }

    // MARK: - Latency Benchmarks

    @Test("Single operation latency")
    func singleOperationLatency() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "counter", node: NodeIdentity(id: "mainframe")),
            target: "increment()",
            genericSubs: [],
            args: [],
            priority: .realtime
        )
        let wireEnvelope = WireEnvelope.call(envelope)

        var compactEncodeTimes: [Double] = []
        var compactDecodeTimes: [Double] = []
        var jsonEncodeTimes: [Double] = []
        var jsonDecodeTimes: [Double] = []

        let jsonEncoder = JSONEncoder()
        let jsonDecoder = JSONDecoder()

        for _ in 0..<1000 {
            let start1 = Date().timeIntervalSinceReferenceDate
            let compactData = wireEnvelope.encodeCompact()
            compactEncodeTimes.append(Date().timeIntervalSinceReferenceDate - start1)

            let start2 = Date().timeIntervalSinceReferenceDate
            _ = try WireEnvelope.decodeCompact(from: compactData)
            compactDecodeTimes.append(Date().timeIntervalSinceReferenceDate - start2)

            let start3 = Date().timeIntervalSinceReferenceDate
            let jsonData = try jsonEncoder.encode(wireEnvelope)
            jsonEncodeTimes.append(Date().timeIntervalSinceReferenceDate - start3)

            let start4 = Date().timeIntervalSinceReferenceDate
            _ = try jsonDecoder.decode(WireEnvelope.self, from: jsonData)
            jsonDecodeTimes.append(Date().timeIntervalSinceReferenceDate - start4)
        }

        let compactEncodeAvg = compactEncodeTimes.reduce(0, +) / Double(compactEncodeTimes.count) * 1_000_000
        let compactDecodeAvg = compactDecodeTimes.reduce(0, +) / Double(compactDecodeTimes.count) * 1_000_000
        let jsonEncodeAvg = jsonEncodeTimes.reduce(0, +) / Double(jsonEncodeTimes.count) * 1_000_000
        let jsonDecodeAvg = jsonDecodeTimes.reduce(0, +) / Double(jsonDecodeTimes.count) * 1_000_000

        print("Average latency (microseconds):")
        print("  Compact encode: \(String(format: "%.2f", compactEncodeAvg))us")
        print("  Compact decode: \(String(format: "%.2f", compactDecodeAvg))us")
        print("  JSON encode: \(String(format: "%.2f", jsonEncodeAvg))us")
        print("  JSON decode: \(String(format: "%.2f", jsonDecodeAvg))us")

        #expect(compactEncodeAvg < jsonEncodeAvg)
        #expect(compactDecodeAvg < jsonDecodeAvg)
    }

    // MARK: - Memory Efficiency

    @Test("Memory efficiency with large payloads")
    func memoryEfficiencyLargePayloads() throws {
        let payloadSizes = [100, 1_000, 10_000, 100_000]

        for size in payloadSizes {
            let largeData = Data(repeating: 0xAB, count: size)
            let envelope = ReplyEnvelope(
                callID: CallID(),
                sender: nil,
                value: largeData
            )
            let wireEnvelope = WireEnvelope.reply(envelope)

            let compactSize = wireEnvelope.encodeCompact().count
            let jsonSize = try JSONEncoder().encode(wireEnvelope).count

            let overhead = compactSize - size
            let jsonOverhead = jsonSize - size

            print("Payload \(size) bytes - Compact overhead: \(overhead) bytes, JSON overhead: \(jsonOverhead) bytes")

            #expect(overhead < jsonOverhead)
        }
    }

    // MARK: - UUID Encoding Efficiency

    @Test("UUID encoding is exactly 16 bytes")
    func uuidEncodingSize() throws {
        var encoder = CompactEncoder()
        encoder.writeUUID(UUID())

        #expect(encoder.data.count == 16)

        let uuidString = UUID().uuidString
        let jsonUUIDSize = uuidString.count
        print("UUID - Compact: 16 bytes, JSON string: \(jsonUUIDSize) bytes (with quotes: \(jsonUUIDSize + 2))")
    }

    // MARK: - Varint Efficiency

    @Test("Varint encoding efficiency for common values")
    func varintEfficiency() {
        let testValues: [(UInt64, Int)] = [
            (0, 1),
            (1, 1),
            (127, 1),
            (128, 2),
            (16383, 2),
            (16384, 3),
            (2097151, 3),
            (268435455, 4),
            (UInt64.max, 10)
        ]

        for (value, expectedBytes) in testValues {
            var encoder = CompactEncoder()
            encoder.writeVarint(value)
            let actualBytes = encoder.data.count

            #expect(actualBytes == expectedBytes, "Value \(value) should be \(expectedBytes) bytes, got \(actualBytes)")
        }
    }
}
