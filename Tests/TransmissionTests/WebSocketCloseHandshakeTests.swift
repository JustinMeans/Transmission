/// WebSocketCloseHandshakeTests.swift
///
/// Tests for the RFC 6455 section 5.5.1 Close-frame echo logic introduced to fix
/// the missing close-echo bug in ServerManager and ClientManager.
///
/// BEFORE THE FIX: both processFrames implementations responded to a received
/// Close frame with a bare `return`, never writing any outbound Close frame.
/// This violated RFC 6455 section 5.5.1 ("the other peer sends a Close frame in
/// response") and left the initiating endpoint in a half-closed limbo, unable to
/// confirm the close was acknowledged.
///
/// AFTER THE FIX: webSocketCloseEchoPayload(from:) derives the correct echo
/// payload, and webSocketCloseEchoFrame(for:) assembles the outbound Close frame.
/// Both are pure/value-type helpers, independently testable without a live server.

import Testing
import NIO
import NIOWebSocket
@testable import Transmission

@Suite("WebSocketCloseHandshake")
struct WebSocketCloseHandshakeTests {

    // MARK: - Helpers

    private func makeCloseFrame(payload: [UInt8]) -> WebSocketFrame {
        var buf = ByteBuffer()
        buf.writeBytes(payload)
        return WebSocketFrame(fin: true, opcode: .connectionClose, data: buf)
    }

    // MARK: - Echo payload: empty received payload

    @Test("Empty received payload echoes empty payload")
    func emptyPayloadEchoesEmpty() {
        let echo = webSocketCloseEchoPayload(from: [])
        #expect(echo.isEmpty, "A codeless Close must be echoed with an empty payload, not a 1002")
    }

    // MARK: - Echo payload: valid status codes

    @Test("Valid code 1000 (normal closure) is echoed verbatim")
    func validCode1000EchoedVerbatim() {
        // 1000 = 0x03E8
        let payload: [UInt8] = [0x03, 0xE8]
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == payload, "A valid Close payload must be echoed verbatim, not replaced")
    }

    @Test("Valid code 1001 (going away) with reason phrase is echoed verbatim")
    func validCode1001WithReasonEchoedVerbatim() {
        // 1001 = 0x03E9, followed by ASCII "bye"
        let payload: [UInt8] = [0x03, 0xE9, 0x62, 0x79, 0x65] // 1001 + "bye"
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == payload)
    }

    @Test("Application-defined code 4000 is echoed verbatim")
    func applicationCode4000EchoedVerbatim() {
        // 4000 = 0x0FA0
        let payload: [UInt8] = [0x0F, 0xA0]
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == payload)
    }

    @Test("Library code 3000 is echoed verbatim")
    func libraryCode3000EchoedVerbatim() {
        // 3000 = 0x0BB8
        let payload: [UInt8] = [0x0B, 0xB8]
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == payload)
    }

    // MARK: - Echo payload: malformed inputs yield 1002 Protocol Error

    // These are the cases that previously had NO response at all (the bug).
    // Now they produce a 1002 echo rather than silence.

    private let protocolError1002: [UInt8] = [0x03, 0xEA] // 1002 big-endian

    @Test("BEFORE FIX SCENARIO: truncated 1-byte payload yields 1002 instead of no response")
    func truncatedPayloadYields1002() {
        // A single byte cannot encode the mandatory 2-byte status code.
        let echo = webSocketCloseEchoPayload(from: [0x03])
        #expect(echo == protocolError1002,
                "A 1-byte Close payload is malformed; must respond with 1002, not silence")
    }

    @Test("BEFORE FIX SCENARIO: payload > 125 bytes yields 1002 instead of no response")
    func oversizePayloadYields1002() {
        // Control frames MUST NOT exceed 125 bytes (RFC 6455 section 5.5).
        let oversize = [UInt8](repeating: 0x41, count: 126)
        let echo = webSocketCloseEchoPayload(from: oversize)
        #expect(echo == protocolError1002,
                "An oversize Close payload is a protocol error; must respond with 1002, not silence")
    }

    @Test("BEFORE FIX SCENARIO: reserved status code 1004 yields 1002 instead of no response")
    func reservedCode1004Yields1002() {
        // 1004 is explicitly reserved by RFC 6455 section 7.4.1.
        let payload: [UInt8] = [0x03, 0xEC] // 1004 big-endian
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == protocolError1002,
                "Reserved status code 1004 must not be reflected; must respond with 1002")
    }

    @Test("BEFORE FIX SCENARIO: reserved status code 1005 yields 1002 instead of no response")
    func reservedCode1005Yields1002() {
        // 1005 = "no status received" pseudo-code; MUST NOT appear in a Close frame.
        let payload: [UInt8] = [0x03, 0xED] // 1005 big-endian
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == protocolError1002,
                "Pseudo-code 1005 must not be reflected; must respond with 1002")
    }

    @Test("BEFORE FIX SCENARIO: reserved status code 1006 yields 1002 instead of no response")
    func reservedCode1006Yields1002() {
        // 1006 = "abnormal closure"; MUST NOT be set on the wire.
        let payload: [UInt8] = [0x03, 0xEE] // 1006 big-endian
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == protocolError1002,
                "Pseudo-code 1006 must not be reflected; must respond with 1002")
    }

    @Test("BEFORE FIX SCENARIO: status code 999 (below valid range) yields 1002 instead of no response")
    func belowRangeCodeYields1002() {
        // Codes < 1000 are undefined and MUST NOT appear on the wire.
        let payload: [UInt8] = [0x03, 0xE7] // 999 big-endian
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == protocolError1002,
                "Status code 999 is below the valid range; must respond with 1002")
    }

    @Test("BEFORE FIX SCENARIO: non-UTF-8 reason phrase yields 1002 instead of no response")
    func nonUTF8ReasonYields1002() {
        // 1000 + invalid UTF-8 byte sequence 0xFF 0xFE.
        let payload: [UInt8] = [0x03, 0xE8, 0xFF, 0xFE]
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == protocolError1002,
                "A non-UTF-8 reason phrase is a protocol error; must respond with 1002")
    }

    // MARK: - webSocketCloseEchoFrame produces correct WebSocketFrame

    @Test("Echo frame has connectionClose opcode and fin=true")
    func echoFrameOpcodeAndFin() {
        let frame = makeCloseFrame(payload: [0x03, 0xE8]) // code 1000
        let echo = webSocketCloseEchoFrame(for: frame)
        #expect(echo.opcode == .connectionClose)
        #expect(echo.fin == true)
    }

    @Test("Echo frame payload matches valid received payload")
    func echoFramePayloadMatchesValid() {
        let payload: [UInt8] = [0x03, 0xE8] // 1000
        let frame = makeCloseFrame(payload: payload)
        let echo = webSocketCloseEchoFrame(for: frame)
        var data = echo.data
        let bytes = data.readBytes(length: data.readableBytes) ?? []
        #expect(bytes == payload)
    }

    @Test("Echo frame for empty received payload contains empty data")
    func echoFrameForEmptyPayloadIsEmpty() {
        let frame = makeCloseFrame(payload: [])
        let echo = webSocketCloseEchoFrame(for: frame)
        #expect(echo.data.readableBytes == 0)
    }

    @Test("Echo frame for malformed payload contains 1002")
    func echoFrameForMalformedContains1002() {
        let frame = makeCloseFrame(payload: [0x03, 0xED]) // 1005 — reserved
        let echo = webSocketCloseEchoFrame(for: frame)
        var data = echo.data
        let bytes = data.readBytes(length: data.readableBytes) ?? []
        #expect(bytes == protocolError1002)
    }

    // MARK: - Boundary: exactly 125-byte payload is valid (not rejected as oversize)

    @Test("Exactly 125-byte Close payload (at the control-frame cap) is echoed verbatim")
    func exactly125BytePayloadIsAccepted() {
        // 2 bytes status code 1000, then 123 bytes of 'x' — total exactly 125.
        var payload: [UInt8] = [0x03, 0xE8]
        payload += [UInt8](repeating: 0x78, count: 123) // 'x' * 123 = valid ASCII/UTF-8
        #expect(payload.count == 125)
        let echo = webSocketCloseEchoPayload(from: payload)
        #expect(echo == payload, "A 125-byte payload is within the cap and must be echoed verbatim")
    }
}
