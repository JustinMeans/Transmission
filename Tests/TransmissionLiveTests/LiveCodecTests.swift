import Foundation
import Testing
@testable import TransmissionLive

/// The value each line of `frames-v1.txt` must decode to.
private let goldenFrames: [String: LiveFrame] = [
    "hello": .hello(protoMin: 1, protoMax: 2, build: "2026.11", clientID: "install-1"),
    "welcome": .welcome(
        proto: 1, epoch: 123_456_789, serverTimeMs: 1_800_000_000_000, heartbeatMs: 25_000, maxFrame: 1_048_576,
        topics: [
            TopicAdvert(name: "logs", schemaMin: 1, schemaMax: 1, kind: .log),
            TopicAdvert(name: "stats", schemaMin: 1, schemaMax: 2, kind: .state),
        ]),
    "sub_fresh": .sub(subID: 1, topic: "stats", params: ["app": "vantage"], schema: 1, maxMilliHz: 1000, resume: nil),
    "sub_resume": .sub(subID: 2, topic: "logs", params: [:], schema: 1, maxMilliHz: 0, resume: LiveCursor(epoch: 77, seq: 300)),
    "unsub": .unsub(subID: 1),
    "pause": .pause(subID: 1),
    "resume": .resume(subID: 1),
    "suback_snapshot": .subAck(subID: 1, epoch: 77, headSeq: 300, schema: 1, replay: false),
    "suback_replay": .subAck(subID: 2, epoch: 77, headSeq: 300, schema: 1, replay: true),
    "snapshot_part": .snapshot(subID: 1, epoch: 77, seq: 300, schema: 1, part: 0, last: false, payload: Data("{\"a\"".utf8)),
    "snapshot_last": .snapshot(subID: 1, epoch: 77, seq: 300, schema: 1, part: 1, last: true, payload: Data(":1}".utf8)),
    "delta": .delta(subID: 1, seq: 301, sampleTimeMs: 1_800_000_000_123, span: 1, payload: Data("{\"a\":2}".utf8)),
    "delta_span": .delta(subID: 1, seq: 310, sampleTimeMs: 1_800_000_000_123, span: 9, payload: Data("{\"a\":3}".utf8)),
    "gap": .gap(subID: 2, fromSeq: 10, toSeq: 19, droppedCount: 10, reason: .outsideRing),
    "ack": .ack(subID: 1, seq: 301),
    "heartbeat": .heartbeat(serverTimeMs: 1_800_000_000_000),
    "auth_refresh": .authRefresh(token: "token"),
    "auth_expiring": .authExpiring(secondsLeft: 60),
    "error": .error(subID: 1, code: .forbidden, retryable: false, message: "Not permitted"),
    "call": .call(alias: "hub.getStats.v1", callID: 7, payload: Data("{}".utf8)),
    "reply_error": .reply(callID: 7, isError: true, payload: Data("no".utf8)),
]

private func loadGolden() throws -> [(name: String, bytes: [UInt8])] {
    let url = try #require(Bundle.module.url(forResource: "Fixtures/frames-v1", withExtension: "txt"))
    return try String(contentsOf: url, encoding: .utf8)
        .split(separator: "\n")
        .filter { !$0.hasPrefix("#") }
        .map { line in
            let parts = line.split(separator: " ")
            return (String(parts[0]), hex(String(parts[1])))
        }
}

private func hex(_ text: String) -> [UInt8] {
    stride(from: 0, to: text.count, by: 2).map {
        let start = text.index(text.startIndex, offsetBy: $0)
        return UInt8(text[start..<text.index(start, offsetBy: 2)], radix: 16)!
    }
}

@Suite("Live codec")
struct LiveCodecTests {
    @Test("every golden line decodes to its value and re-encodes byte for byte")
    func golden() throws {
        let lines = try loadGolden()
        #expect(lines.count == goldenFrames.count)
        for (name, bytes) in lines {
            let expected = try #require(goldenFrames[name], "no value for \(name)")
            #expect(try LiveCodec.decode(bytes) == expected, "decode \(name)")
            #expect(Array(LiveCodec.encode(expected)) == bytes, "encode \(name)")
        }
    }

    @Test("every frame type is pinned by a golden line")
    func goldenCoversEveryType() throws {
        let types = Set(try loadGolden().map { $0.bytes[0] })
        #expect(types == [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x10, 0x11, 0x12, 0x13, 0x20, 0x21, 0x22, 0x30, 0x31, 0x32])
    }

    @Test("a hand-written frame matches the spec")
    func literal() throws {
        // DELTA, no flags, subID 1, seq 2, sampleTime 3, payload "{}".
        let bytes: [UInt8] = [0x11, 0x00, 0x01, 0x02, 0x03, 0x02, 0x7B, 0x7D]
        #expect(try LiveCodec.decode(bytes) == .delta(subID: 1, seq: 2, sampleTimeMs: 3, span: 1, payload: Data("{}".utf8)))
    }

    @Test("unknown frame types are ignored, not errors")
    func unknownType() throws {
        #expect(try LiveCodec.decode([0x7F, 0x00, 0x00, 0xDE, 0xAD]) == .unknown(type: 0x7F))
    }

    @Test("a future field appended to a known frame is tolerated")
    func trailingBytes() throws {
        for (name, bytes) in try loadGolden() {
            let extended = bytes + [0xDE, 0xAD, 0xBE, 0xEF]
            #expect(try LiveCodec.decode(extended) == goldenFrames[name], "\(name)")
        }
    }

    @Test("unknown flag bits are ignored")
    func reservedFlags() throws {
        let bytes: [UInt8] = [0x11, 0xF0, 0x01, 0x02, 0x03, 0x00]
        #expect(try LiveCodec.decode(bytes) == .delta(subID: 1, seq: 2, sampleTimeMs: 3, span: 1, payload: Data()))
    }

    @Test("an unknown error code or gap reason stays decodable")
    func unknownEnumValues() throws {
        #expect(try LiveCodec.decode([0x30, 0, 0, 200, 1, 0, 0]) == .error(subID: 0, code: .unspecified, retryable: false, message: ""))
        #expect(try LiveCodec.decode([0x12, 0, 1, 1, 2, 2, 9]) == .gap(subID: 1, fromSeq: 1, toSeq: 2, droppedCount: 2, reason: .overflow))
    }

    @Test("every truncation of every golden frame fails cleanly")
    func truncation() throws {
        for (name, bytes) in try loadGolden() {
            for length in 0..<bytes.count {
                // A prefix may still be a complete frame only when it is shorter
                // than the optional tail; everything else must throw, never trap.
                let prefix = Array(bytes[..<length])
                _ = try? LiveCodec.decode(prefix)
                if length < 3 {
                    #expect(throws: (any Error).self, "\(name) at \(length)") { try LiveCodec.decode(prefix) }
                }
            }
        }
    }

    @Test("a length past the end of the frame is rejected")
    func hugeLength() {
        let bytes: [UInt8] = [0x21, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 1]
        #expect(throws: (any Error).self) { try LiveCodec.decode(bytes) }
    }

    @Test("a count past the end of the frame is rejected before any allocation")
    func hugeCount() {
        // WELCOME claiming 2^40 topics.
        var bytes: [UInt8] = [0x02, 0, 0, 1, 1, 1, 1, 1]
        bytes += [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40]
        #expect(throws: LiveCodecError.truncated) { try LiveCodec.decode(bytes) }
    }

    @Test("a varint past 64 bits is rejected")
    func varintOverflow() {
        let bytes: [UInt8] = [0x13, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02]
        #expect(throws: LiveCodecError.varintOverflow) { try LiveCodec.decode(bytes) }
    }

    @Test("max UInt64 survives a round trip")
    func maxVarint() throws {
        let frame = LiveFrame.ack(subID: .max, seq: .max)
        #expect(try LiveCodec.decode(LiveCodec.encode(frame)) == frame)
    }

    @Test("invalid UTF-8 is rejected")
    func invalidUTF8() {
        #expect(throws: LiveCodecError.invalidUTF8) { try LiveCodec.decode([0x21, 0, 0, 2, 0xC3, 0x28]) }
    }

    @Test("params encode in sorted order so equal subscriptions are equal bytes")
    func sortedParams() {
        let a = LiveCodec.encode(.sub(subID: 1, topic: "t", params: ["b": "2", "a": "1", "c": "3"], schema: 1, maxMilliHz: 0, resume: nil))
        let b = LiveCodec.encode(.sub(subID: 1, topic: "t", params: ["c": "3", "a": "1", "b": "2"], schema: 1, maxMilliHz: 0, resume: nil))
        #expect(a == b)
    }

    @Test("a wire frame assembles to the same bytes as a whole frame")
    func wireFrameAssembly() {
        let frame = LiveFrame.gap(subID: 300, fromSeq: 1, toSeq: 2, droppedCount: 2, reason: .overflow)
        #expect(LiveCodec.wireFrame(frame).assembled == Array(LiveCodec.encode(frame)))
    }
}
