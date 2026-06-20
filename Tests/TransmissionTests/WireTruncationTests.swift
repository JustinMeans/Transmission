import Testing
import Foundation
@testable import Transmission

/// Truncation and malformed-frame robustness for the compact wire format.
///
/// `WireEnvelope.decodeCompact(from:)` runs directly on raw bytes pulled off the
/// WebSocket transport. A partial frame (an incomplete payload delivered by the
/// socket layer) or an adversarial short frame MUST surface as a thrown
/// `TransmissionError.decodingFailed` — never a trap, an out-of-bounds access, or
/// a silently mis-decoded envelope.
///
/// The existing `CompactBinaryProtocolTests` only ever feed well-formed bytes, so
/// the truncation contract is unpinned. These tests encode valid envelopes, then
/// decode every strict prefix of the encoded bytes and assert that no prefix
/// causes a crash. A prefix is acceptable if it throws, or if it happens to be a
/// self-consistent shorter frame (only possible for `.close`, whose body is one
/// byte). For `.call` and `.reply` every strict prefix must throw.
@Suite("Wire Truncation Robustness")
struct WireTruncationTests {

    /// Decoding any strict prefix of a valid `.call` envelope must throw, never trap.
    @Test("Truncated call envelope at every prefix throws, never traps")
    func truncatedCallThrowsAtEveryPrefix() {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "data-service", node: NodeIdentity(id: "worker-001")),
            target: "process(input:options:)",
            genericSubs: ["Swift.String", "Swift.Int"],
            args: [Data([0x01, 0x02, 0x03, 0x04]), Data(repeating: 0xAB, count: 32)],
            priority: .high
        )
        let full = WireEnvelope.call(envelope).encodeCompact()

        // Every strict prefix (0 ..< full.count) is incomplete and must throw.
        for prefixLength in 0..<full.count {
            let truncated = full.prefix(prefixLength)
            #expect(throws: (any Error).self,
                    "Call prefix of length \(prefixLength) must throw, not trap") {
                _ = try WireEnvelope.decodeCompact(from: Data(truncated))
            }
        }

        // Sanity: the untruncated bytes still decode cleanly.
        #expect(throws: Never.self) {
            _ = try WireEnvelope.decodeCompact(from: full)
        }
    }

    /// Decoding any strict prefix of a valid `.reply` envelope must throw, never trap.
    @Test("Truncated reply envelope at every prefix throws, never traps")
    func truncatedReplyThrowsAtEveryPrefix() {
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: ActorIdentity(id: "responder", node: NodeIdentity(id: "server")),
            value: Data(repeating: 0xDE, count: 64)
        )
        let full = WireEnvelope.reply(envelope).encodeCompact()

        for prefixLength in 0..<full.count {
            let truncated = full.prefix(prefixLength)
            #expect(throws: (any Error).self,
                    "Reply prefix of length \(prefixLength) must throw, not trap") {
                _ = try WireEnvelope.decodeCompact(from: Data(truncated))
            }
        }

        #expect(throws: Never.self) {
            _ = try WireEnvelope.decodeCompact(from: full)
        }
    }

    /// Empty data has no type byte and must throw rather than trap on the first read.
    @Test("Empty payload throws")
    func emptyPayloadThrows() {
        #expect(throws: (any Error).self) {
            _ = try WireEnvelope.decodeCompact(from: Data())
        }
    }

    /// A frame whose only byte is an unknown envelope-type tag must throw.
    @Test("Unknown envelope type byte throws")
    func unknownEnvelopeTypeThrows() {
        // 0, 1, 2 are call/reply/close; 3+ are undefined.
        for badType: UInt8 in [3, 7, 42, 0xFF] {
            #expect(throws: (any Error).self,
                    "Envelope type \(badType) must be rejected") {
                _ = try WireEnvelope.decodeCompact(from: Data([badType]))
            }
        }
    }

    /// A call frame that declares a string length longer than the bytes that follow
    /// must throw "insufficient bytes", not read past the buffer.
    @Test("Call with overstated string length throws insufficient bytes")
    func overstatedStringLengthThrows() {
        var encoder = CompactEncoder()
        encoder.writeUInt8(0)            // envelope type: call
        encoder.writeUUID(UUID())        // call ID (16 bytes)
        // Claim a 200-byte actor-ID string but supply only 3 bytes.
        encoder.writeVarint(200)
        var raw = encoder.data
        raw.append(contentsOf: [0x61, 0x62, 0x63]) // "abc"

        #expect(throws: (any Error).self,
                "Overstated string length must throw, not over-read") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A reply frame whose declared value length overruns the buffer must throw.
    @Test("Reply with overstated value length throws insufficient bytes")
    func overstatedValueLengthThrows() {
        var encoder = CompactEncoder()
        encoder.writeUInt8(1)            // envelope type: reply
        encoder.writeUUID(UUID())        // call ID
        encoder.writeUInt8(0)            // sender absent
        // Claim a 1000-byte value but supply only 2 bytes.
        encoder.writeVarint(1000)
        var raw = encoder.data
        raw.append(contentsOf: [0xFF, 0xFE])

        #expect(throws: (any Error).self,
                "Overstated reply value length must throw, not over-read") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A call frame truncated exactly mid-UUID (after the type byte, before the
    /// full 16 UUID bytes) must throw — exercises `readUUID`'s bounds check.
    @Test("Call truncated mid-UUID throws")
    func callTruncatedMidUUIDThrows() {
        var encoder = CompactEncoder()
        encoder.writeUInt8(0)            // call type
        encoder.writeUUID(UUID())        // 16 UUID bytes
        let full = encoder.data          // 17 bytes total

        // Keep the type byte plus only 8 of the 16 UUID bytes.
        let truncated = full.prefix(1 + 8)
        #expect(throws: (any Error).self,
                "Mid-UUID truncation must throw") {
            _ = try WireEnvelope.decodeCompact(from: Data(truncated))
        }
    }

    /// `.close` is a single byte; a `.close` frame with trailing garbage still
    /// decodes to `.close` (the decoder reads exactly one byte). This pins that the
    /// decoder does not require the buffer to be fully consumed for close.
    @Test("Close frame with trailing bytes still decodes to close")
    func closeWithTrailingBytesDecodes() throws {
        var raw = WireEnvelope.close.encodeCompact() // single 0x02 byte
        raw.append(contentsOf: [0x00, 0x00, 0x00])   // trailing garbage

        let decoded = try WireEnvelope.decodeCompact(from: raw)
        guard case .close = decoded else {
            Issue.record("Expected close envelope")
            return
        }
    }
}
