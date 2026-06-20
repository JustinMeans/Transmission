import Testing
import Foundation
import NIO
import NIOWebSocket
@testable import Transmission

/// Tests for FrameAccumulator, which reassembles WebSocket message fragments.
///
/// Before the fix, processFrames matched only .binary and .text opcodes. Continuation
/// frames fell through to `default: break`, silently discarding their data and leaving
/// the partial buffer unflushed. The next unrelated message would then have the stale
/// bytes prepended, corrupting delivery.
///
/// The fix introduces FrameAccumulator which handles .continuation frames identically
/// to the initial .binary/.text fragment: accumulate bytes, flush when fin == true.
@Suite("FrameAccumulator Tests")
struct FrameAccumulatorTests {

    // MARK: - Helpers

    private func makeFrame(bytes: [UInt8], opcode: WebSocketOpcode, fin: Bool) -> WebSocketFrame {
        var buffer = ByteBuffer()
        buffer.writeBytes(bytes)
        return WebSocketFrame(fin: fin, opcode: opcode, data: buffer)
    }

    // MARK: - Non-fragmented (single frame, fin == true)

    @Test("Single binary frame with fin=true returns assembled data immediately")
    func singleBinaryFrame() throws {
        var acc = FrameAccumulator()
        let frame = makeFrame(bytes: [0x01, 0x02, 0x03], opcode: .binary, fin: true)
        let result = try acc.feed(frame)
        #expect(result == Data([0x01, 0x02, 0x03]))
    }

    @Test("Single text frame with fin=true returns assembled data immediately")
    func singleTextFrame() throws {
        var acc = FrameAccumulator()
        let frame = makeFrame(bytes: Array("hello".utf8), opcode: .text, fin: true)
        let result = try acc.feed(frame)
        #expect(result == Data("hello".utf8))
    }

    @Test("Empty single frame returns empty Data (not nil)")
    func emptyPayloadFrame() throws {
        var acc = FrameAccumulator()
        let frame = makeFrame(bytes: [], opcode: .binary, fin: true)
        let result = try acc.feed(frame)
        #expect(result == Data())
    }

    // MARK: - Fragmented messages (the bug site)

    @Test("Fragmented message: binary(fin=false) + continuation(fin=true) assembles correctly")
    func twoFragmentMessage() throws {
        var acc = FrameAccumulator()

        // First fragment: opcode .binary, fin == false — should return nil (not complete)
        let first = makeFrame(bytes: [0xAA, 0xBB], opcode: .binary, fin: false)
        let midResult = try acc.feed(first)
        #expect(midResult == nil, "Intermediate frame must not flush the buffer")

        // Final fragment: opcode .continuation, fin == true — must return assembled data
        let last = makeFrame(bytes: [0xCC, 0xDD], opcode: .continuation, fin: true)
        let finalResult = try acc.feed(last)
        #expect(finalResult == Data([0xAA, 0xBB, 0xCC, 0xDD]),
                "Continuation frame data must be appended and flushed when fin == true")
    }

    @Test("Three-fragment message assembles in order")
    func threeFragmentMessage() throws {
        var acc = FrameAccumulator()

        let f1 = makeFrame(bytes: [0x01], opcode: .binary, fin: false)
        let f2 = makeFrame(bytes: [0x02], opcode: .continuation, fin: false)
        let f3 = makeFrame(bytes: [0x03], opcode: .continuation, fin: true)

        #expect(try acc.feed(f1) == nil)
        #expect(try acc.feed(f2) == nil)
        let result = try acc.feed(f3)
        #expect(result == Data([0x01, 0x02, 0x03]))
    }

    @Test("Continuation with fin=false does not flush")
    func continuationWithoutFin() throws {
        var acc = FrameAccumulator()
        let f1 = makeFrame(bytes: [0xDE], opcode: .binary, fin: false)
        let f2 = makeFrame(bytes: [0xAD], opcode: .continuation, fin: false)

        #expect(try acc.feed(f1) == nil)
        #expect(try acc.feed(f2) == nil, "Continuation without fin must not flush")
    }

    // MARK: - Buffer is cleared after flush (no stale data leaks into next message)

    @Test("Buffer resets after a complete message; next message starts clean")
    func bufferClearsAfterFlush() throws {
        var acc = FrameAccumulator()

        // Message 1: single frame
        let msg1 = makeFrame(bytes: [0x11], opcode: .binary, fin: true)
        let result1 = try acc.feed(msg1)
        #expect(result1 == Data([0x11]))

        // Message 2: must NOT contain bytes from message 1
        let msg2 = makeFrame(bytes: [0x22], opcode: .binary, fin: true)
        let result2 = try acc.feed(msg2)
        #expect(result2 == Data([0x22]),
                "Stale bytes from the previous message must not contaminate the next")
    }

    @Test("Buffer resets after fragmented message; next message starts clean")
    func bufferClearsAfterFragmentedFlush() throws {
        var acc = FrameAccumulator()

        // Fragmented message 1
        #expect(try acc.feed(makeFrame(bytes: [0xAA], opcode: .binary, fin: false)) == nil)
        let result1 = try acc.feed(makeFrame(bytes: [0xBB], opcode: .continuation, fin: true))
        #expect(result1 == Data([0xAA, 0xBB]))

        // Independent message 2: must not include 0xAA or 0xBB
        let result2 = try acc.feed(makeFrame(bytes: [0xFF], opcode: .binary, fin: true))
        #expect(result2 == Data([0xFF]),
                "Previously accumulated bytes must be cleared after fin frame flushes")
    }

    // MARK: - Maximum message size cap (fragmentation-flood DoS guard)

    @Test("A message at exactly the cap is accepted")
    func messageExactlyAtCapIsAccepted() throws {
        var acc = FrameAccumulator(maxMessageSize: 4)
        let frame = makeFrame(bytes: [0x01, 0x02, 0x03, 0x04], opcode: .binary, fin: true)
        let result = try acc.feed(frame)
        #expect(result == Data([0x01, 0x02, 0x03, 0x04]),
                "A payload exactly equal to the cap must be delivered, not rejected")
    }

    @Test("A single frame larger than the cap is rejected")
    func singleOversizeFrameIsRejected() {
        var acc = FrameAccumulator(maxMessageSize: 3)
        let frame = makeFrame(bytes: [0x01, 0x02, 0x03, 0x04], opcode: .binary, fin: true)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(frame)
        }
    }

    @Test("Fragmented accumulation that crosses the cap is rejected")
    func fragmentedAccumulationCrossingCapIsRejected() throws {
        var acc = FrameAccumulator(maxMessageSize: 5)

        // 3 bytes so far: under the cap, returns nil.
        let f1 = makeFrame(bytes: [0x01, 0x02, 0x03], opcode: .binary, fin: false)
        #expect(try acc.feed(f1) == nil)

        // Adding 3 more would total 6 > 5: must throw before materializing the bytes.
        let f2 = makeFrame(bytes: [0x04, 0x05, 0x06], opcode: .continuation, fin: false)
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(f2)
        }
    }

    @Test("A continuation flood cannot grow the buffer without bound")
    func continuationFloodIsBounded() {
        // Simulate an attacker streaming many small non-fin continuation frames.
        var acc = FrameAccumulator(maxMessageSize: 16)
        let chunk: [UInt8] = [0xAB, 0xCD, 0xEF, 0x10] // 4 bytes per frame

        // First fragment opens the message.
        #expect(throws: Never.self) {
            _ = try acc.feed(makeFrame(bytes: chunk, opcode: .binary, fin: false))   // 4
            _ = try acc.feed(makeFrame(bytes: chunk, opcode: .continuation, fin: false)) // 8
            _ = try acc.feed(makeFrame(bytes: chunk, opcode: .continuation, fin: false)) // 12
            _ = try acc.feed(makeFrame(bytes: chunk, opcode: .continuation, fin: false)) // 16 (at cap)
        }

        // The 5th frame (would be 20 bytes) must be rejected.
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: chunk, opcode: .continuation, fin: false))
        }
    }

    @Test("Accumulator resets after a cap rejection; the next message starts clean")
    func accumulatorResetsAfterRejection() throws {
        var acc = FrameAccumulator(maxMessageSize: 4)

        // Open with 3 bytes (under cap).
        #expect(try acc.feed(makeFrame(bytes: [0x01, 0x02, 0x03], opcode: .binary, fin: false)) == nil)

        // This overflows the cap and is rejected; the partial buffer must be discarded.
        #expect(throws: TransmissionError.self) {
            _ = try acc.feed(makeFrame(bytes: [0x04, 0x05], opcode: .continuation, fin: false))
        }

        // A fresh, independent message must NOT carry the stale 0x01,0x02,0x03 bytes.
        let recovered = try acc.feed(makeFrame(bytes: [0xFF], opcode: .binary, fin: true))
        #expect(recovered == Data([0xFF]),
                "Bytes accumulated before a cap rejection must not contaminate the next message")
    }

    @Test("Default cap is 16 MiB")
    func defaultCapValue() {
        #expect(FrameAccumulator.defaultMaxMessageSize == 16 * 1024 * 1024)
    }
}
