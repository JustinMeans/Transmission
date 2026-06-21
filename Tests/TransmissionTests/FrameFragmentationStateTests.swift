import Testing
import Foundation
import NIO
import NIOWebSocket
@testable import Transmission

/// Tests for the WebSocket fragmentation state machine enforced by
/// `FrameAccumulator.feed(_:)`.
///
/// The earlier `FrameAccumulator` only bounded the reassembled message size; it
/// concatenated every frame's bytes regardless of opcode or fragmentation state.
/// That silently accepted two RFC 6455 (section 5.4) framing violations:
///
/// 1. An ORPHAN CONTINUATION — a `.continuation` frame with no message in
///    progress. Its bytes were treated as a complete standalone message, letting a
///    peer forge a delivery from a bare continuation frame.
/// 2. An INTERLEAVED MESSAGE START — a new `.text` / `.binary` frame arriving while
///    a fragmented message was still open. Its bytes were fused onto the
///    unfinished message (message-boundary confusion / cross-message injection).
///
/// `feed(_:)` now validates the opcode against the in-progress state before
/// touching the buffer, rejects both violations, and resets cleanly so the next
/// legitimate message on the connection is unaffected. These tests pin that
/// contract and confirm the legitimate single-frame and fragmented paths still
/// work (non-regression with `FrameAccumulatorTests`).
@Suite("Frame Fragmentation State")
struct FrameFragmentationStateTests {

    private func makeFrame(bytes: [UInt8], opcode: WebSocketOpcode, fin: Bool) -> WebSocketFrame {
        var buffer = ByteBuffer()
        buffer.writeBytes(bytes)
        return WebSocketFrame(fin: fin, opcode: opcode, data: buffer)
    }

    // MARK: - Orphan continuation (no message in progress)

    @Test("A continuation frame with no message in progress is rejected")
    func orphanContinuationRejected() {
        var acc = FrameAccumulator()
        let orphan = makeFrame(bytes: [0x01, 0x02], opcode: .continuation, fin: true)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(orphan)
        }
    }

    @Test("A non-fin continuation frame with no message in progress is rejected")
    func orphanNonFinContinuationRejected() {
        var acc = FrameAccumulator()
        let orphan = makeFrame(bytes: [0xFF], opcode: .continuation, fin: false)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(orphan)
        }
    }

    @Test("After an orphan continuation rejection, a fresh single message still decodes")
    func recoversAfterOrphanContinuation() throws {
        var acc = FrameAccumulator()
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0xAA], opcode: .continuation, fin: true))
        }
        // A legitimate standalone message must be delivered cleanly with no stale bytes.
        let recovered = try acc.feed(makeFrame(bytes: [0x42], opcode: .binary, fin: true))
        #expect(recovered == Data([0x42]))
    }

    // MARK: - Interleaved new message while fragmenting

    @Test("A new binary frame while a fragmented message is open is rejected")
    func interleavedBinaryRejected() throws {
        var acc = FrameAccumulator()
        // Open a fragmented message.
        #expect(try acc.feed(makeFrame(bytes: [0x01, 0x02], opcode: .binary, fin: false)) == nil)
        // A second data-start frame must not fuse onto the open message.
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0x03], opcode: .binary, fin: true))
        }
    }

    @Test("A new text frame while a fragmented message is open is rejected")
    func interleavedTextRejected() throws {
        var acc = FrameAccumulator()
        #expect(try acc.feed(makeFrame(bytes: [0x10], opcode: .text, fin: false)) == nil)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0x20], opcode: .text, fin: true))
        }
    }

    @Test("After an interleave rejection, the open message's bytes do not leak into the next")
    func interleaveRejectionDiscardsPartialMessage() throws {
        var acc = FrameAccumulator()
        // Open a fragment carrying 0xDE, 0xAD.
        #expect(try acc.feed(makeFrame(bytes: [0xDE, 0xAD], opcode: .binary, fin: false)) == nil)
        // Interleaved new message start: rejected, partial buffer must be discarded.
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0xBE], opcode: .binary, fin: true))
        }
        // The next legitimate message must not carry 0xDE / 0xAD.
        let next = try acc.feed(makeFrame(bytes: [0xEF], opcode: .binary, fin: true))
        #expect(next == Data([0xEF]),
                "Bytes from a message abandoned on interleave must not contaminate the next")
    }

    // MARK: - Control opcodes never feed reassembly

    @Test("A control opcode fed into the accumulator is rejected, not reassembled")
    func controlOpcodeRejected() {
        var acc = FrameAccumulator()
        let ping = makeFrame(bytes: [0x00], opcode: .ping, fin: true)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(ping)
        }
    }

    @Test("A control frame fed mid-message resets state without delivering its bytes")
    func controlFrameMidMessageResets() throws {
        var acc = FrameAccumulator()
        // Open a fragmented message.
        #expect(try acc.feed(makeFrame(bytes: [0x01], opcode: .binary, fin: false)) == nil)
        // A control frame must not be reassembled and must reset the open message.
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0x99], opcode: .pong, fin: true))
        }
        // After reset, a fresh single message decodes with no leaked 0x01 / 0x99.
        let next = try acc.feed(makeFrame(bytes: [0x07], opcode: .binary, fin: true))
        #expect(next == Data([0x07]))
    }

    // MARK: - Legitimate paths still work (non-regression)

    @Test("A standalone binary message still decodes")
    func legitimateSingleFrameStillWorks() throws {
        var acc = FrameAccumulator()
        let result = try acc.feed(makeFrame(bytes: [0xCA, 0xFE], opcode: .binary, fin: true))
        #expect(result == Data([0xCA, 0xFE]))
    }

    @Test("A correctly fragmented binary + continuation message still assembles")
    func legitimateFragmentedStillWorks() throws {
        var acc = FrameAccumulator()
        #expect(try acc.feed(makeFrame(bytes: [0xAA], opcode: .binary, fin: false)) == nil)
        #expect(try acc.feed(makeFrame(bytes: [0xBB], opcode: .continuation, fin: false)) == nil)
        let result = try acc.feed(makeFrame(bytes: [0xCC], opcode: .continuation, fin: true))
        #expect(result == Data([0xAA, 0xBB, 0xCC]))
    }

    @Test("Two consecutive correctly fragmented messages each assemble independently")
    func twoFragmentedMessagesInSequence() throws {
        var acc = FrameAccumulator()
        // Message 1.
        #expect(try acc.feed(makeFrame(bytes: [0x01], opcode: .binary, fin: false)) == nil)
        let m1 = try acc.feed(makeFrame(bytes: [0x02], opcode: .continuation, fin: true))
        #expect(m1 == Data([0x01, 0x02]))
        // Message 2: state must be clean enough to open and complete a new fragmentation.
        #expect(try acc.feed(makeFrame(bytes: [0x03], opcode: .binary, fin: false)) == nil)
        let m2 = try acc.feed(makeFrame(bytes: [0x04], opcode: .continuation, fin: true))
        #expect(m2 == Data([0x03, 0x04]))
    }
}
