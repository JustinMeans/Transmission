import Testing
import Foundation
@testable import Transmission

/// Trailing-byte (strict-consumption) robustness for the compact wire format.
///
/// `WireEnvelope.decodeCompact(from:)` runs directly on the payload of a single
/// inbound WebSocket frame: the whole payload is meant to be exactly one envelope.
/// A `.call` and a `.reply` are fully length-described by their own fields, so a
/// structurally-complete envelope followed by extra bytes is malformed — either
/// corruption, or a second payload smuggled after the framing layer believes it
/// has accounted for the message.
///
/// Prior cycles pinned the *under-read* directions: truncation / byte over-read
/// (`WireTruncationTests`), oversized collection counts (`WireCountBoundTests`),
/// and reassembled-message size limits (`FrameAccumulatorTests`). None of them
/// pin the *over-read* direction. A permissive decoder that silently discards the
/// tail enables frame-ambiguity / integrity-evasion attacks and masks corruption.
///
/// Contract pinned here: any residual bytes after a structurally-complete `.call`
/// or `.reply` MUST throw `TransmissionError.decodingFailed` rather than decode
/// successfully and drop the tail. `.close` is intentionally exempt (it is a
/// single-byte tag whose trailing-byte tolerance is pinned by
/// `WireTruncationTests.closeWithTrailingBytesDecodes`).
@Suite("Wire Trailing Byte Strict Consumption")
struct WireTrailingByteTests {

    private func sampleCall() -> WireEnvelope {
        .call(CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "data-service", node: NodeIdentity(id: "worker-001")),
            target: "process(input:options:)",
            genericSubs: ["Swift.String", "Swift.Int"],
            args: [Data([0x01, 0x02, 0x03, 0x04]), Data(repeating: 0xAB, count: 16)],
            priority: .high
        ))
    }

    private func sampleReply() -> WireEnvelope {
        .reply(ReplyEnvelope(
            callID: CallID(),
            sender: ActorIdentity(id: "responder", node: NodeIdentity(id: "server")),
            value: Data(repeating: 0xDE, count: 24)
        ))
    }

    /// A structurally-complete `.call` with a single trailing byte must throw.
    @Test("Call with one trailing byte throws")
    func callWithOneTrailingByteThrows() {
        var raw = sampleCall().encodeCompact()
        raw.append(0x00)

        #expect(throws: (any Error).self,
                "A single trailing byte after a call envelope must be rejected") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A `.call` with a multi-byte smuggled tail (e.g. a second appended envelope)
    /// must throw, not silently decode only the first envelope.
    @Test("Call with smuggled trailing envelope throws")
    func callWithSmuggledTrailingEnvelopeThrows() {
        var raw = sampleCall().encodeCompact()
        // Append an entire second envelope as the smuggled tail.
        raw.append(sampleReply().encodeCompact())

        #expect(throws: (any Error).self,
                "A second smuggled envelope appended to a call must be rejected") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A structurally-complete `.reply` with trailing bytes must throw.
    @Test("Reply with trailing bytes throws")
    func replyWithTrailingBytesThrows() {
        var raw = sampleReply().encodeCompact()
        raw.append(contentsOf: [0xFF, 0xFE, 0xFD])

        #expect(throws: (any Error).self,
                "Trailing bytes after a reply envelope must be rejected") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A `.reply` with a large random trailing blob must throw, never silently
    /// drop it.
    @Test("Reply with large trailing blob throws")
    func replyWithLargeTrailingBlobThrows() {
        var raw = sampleReply().encodeCompact()
        raw.append(Data(repeating: 0x5A, count: 4096))

        #expect(throws: (any Error).self,
                "A large trailing blob after a reply must be rejected") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// The guard must be exact, not over-tight: an envelope with NO trailing bytes
    /// (`bytesRemaining == 0`) must still decode cleanly. Pins that the strict
    /// check does not regress the happy path for both framed envelope types.
    @Test("Exactly-sized call and reply still decode")
    func exactlySizedEnvelopesDecode() throws {
        let call = sampleCall()
        let decodedCall = try WireEnvelope.decodeCompact(from: call.encodeCompact())
        guard case .call(let c) = decodedCall else {
            Issue.record("Expected a call envelope")
            return
        }
        #expect(c.target == "process(input:options:)")
        #expect(c.genericSubs == ["Swift.String", "Swift.Int"])

        let reply = sampleReply()
        let decodedReply = try WireEnvelope.decodeCompact(from: reply.encodeCompact())
        guard case .reply(let r) = decodedReply else {
            Issue.record("Expected a reply envelope")
            return
        }
        #expect(r.value == Data(repeating: 0xDE, count: 24))
    }

    /// A `.call` with empty collections (no subs, no args) and one trailing byte
    /// must still throw — exercises the guard on the minimal-structure path where
    /// the args array closes the envelope immediately.
    @Test("Minimal call with trailing byte throws")
    func minimalCallWithTrailingByteThrows() {
        let minimal = WireEnvelope.call(CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "svc"),
            target: "ping()",
            genericSubs: [],
            args: [],
            priority: .normal
        ))
        var raw = minimal.encodeCompact()
        raw.append(0x42)

        #expect(throws: (any Error).self,
                "A trailing byte after a minimal call must be rejected") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// `.close` retains its single-byte-tag trailing-byte tolerance (pinned here so
    /// the strict-consumption change is documented as intentionally NOT applying to
    /// `.close`, matching `WireTruncationTests.closeWithTrailingBytesDecodes`).
    @Test("Close with trailing bytes still decodes (unchanged)")
    func closeWithTrailingBytesStillDecodes() throws {
        var raw = WireEnvelope.close.encodeCompact()
        raw.append(contentsOf: [0x00, 0x00, 0x00])

        let decoded = try WireEnvelope.decodeCompact(from: raw)
        guard case .close = decoded else {
            Issue.record("Expected close envelope")
            return
        }
    }
}
