import Testing
import Foundation
@testable import Transmission

/// Tests for the RFC 6455 section 5.5.1 Close-frame parser and the IANA WebSocket
/// Close Code registry validation in `WebSocketCloseFrame`.
///
/// The parser extracts the big-endian 2-byte status code from a Close frame's
/// application-data payload, enforces the control-frame 125-byte limit, rejects
/// reserved/undefined close codes, treats an empty payload as a valid codeless
/// close, and decodes the optional UTF-8 reason phrase. These tests pin every one
/// of those rules with known-reference codes (valid AND invalid), boundary payload
/// lengths, an empty payload, and an oversized payload.
@Suite("WebSocket Close Frame")
struct WebSocketCloseFrameTests {

    /// Build a Close-frame payload from a status code (big-endian) plus optional reason.
    private func payload(code: UInt16, reason: String = "") -> [UInt8] {
        var bytes: [UInt8] = [UInt8(code >> 8), UInt8(code & 0xFF)]
        bytes.append(contentsOf: Array(reason.utf8))
        return bytes
    }

    // MARK: - Valid status codes (IANA registry)

    @Test("All assigned protocol close codes 1000...1003 and 1007...1011 are valid",
          arguments: [1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011] as [UInt16])
    func assignedProtocolCodesValid(code: UInt16) throws {
        #expect(WebSocketCloseFrame.isValidCloseCode(code))
        let frame = try WebSocketCloseFrame.parse(payload: payload(code: code))
        #expect(frame.code == code)
        #expect(frame.reason.isEmpty)
    }

    @Test("Library/application range 3000...4999 boundaries are valid",
          arguments: [3000, 3999, 4000, 4500, 4999] as [UInt16])
    func libraryAndPrivateRangesValid(code: UInt16) throws {
        #expect(WebSocketCloseFrame.isValidCloseCode(code))
        let frame = try WebSocketCloseFrame.parse(payload: payload(code: code))
        #expect(frame.code == code)
    }

    // MARK: - Invalid / reserved status codes

    @Test("Reserved and out-of-range close codes are rejected",
          arguments: [0, 999, 1004, 1005, 1006, 1012, 1015, 1999, 2999, 5000, 65535] as [UInt16])
    func reservedAndOutOfRangeCodesInvalid(code: UInt16) {
        #expect(!WebSocketCloseFrame.isValidCloseCode(code))
        #expect(throws: TransmissionError.self) {
            _ = try WebSocketCloseFrame.parse(payload: payload(code: code))
        }
    }

    @Test("Specifically reserved codes 1004/1005/1006/1015 are individually rejected")
    func individuallyReservedCodes() {
        for code: UInt16 in [1004, 1005, 1006, 1015] {
            #expect(!WebSocketCloseFrame.isValidCloseCode(code),
                    "Reserved code \(code) must not be accepted")
        }
    }

    @Test("Boundary codes just outside valid ranges are rejected")
    func boundaryCodesOutsideRanges() {
        // Just below 1000, the 1003/1004 boundary, the 1006/1007 boundary, the
        // 1011/1012 boundary, the 2999/3000 boundary, and the 4999/5000 boundary.
        #expect(!WebSocketCloseFrame.isValidCloseCode(999))
        #expect(WebSocketCloseFrame.isValidCloseCode(1003))
        #expect(!WebSocketCloseFrame.isValidCloseCode(1004))
        #expect(WebSocketCloseFrame.isValidCloseCode(1007))
        #expect(WebSocketCloseFrame.isValidCloseCode(1011))
        #expect(!WebSocketCloseFrame.isValidCloseCode(1012))
        #expect(!WebSocketCloseFrame.isValidCloseCode(2999))
        #expect(WebSocketCloseFrame.isValidCloseCode(3000))
        #expect(WebSocketCloseFrame.isValidCloseCode(4999))
        #expect(!WebSocketCloseFrame.isValidCloseCode(5000))
    }

    // MARK: - Empty payload (codeless close)

    @Test("An empty Close payload is valid and yields a nil code")
    func emptyPayloadIsCodeless() throws {
        let frame = try WebSocketCloseFrame.parse(payload: [])
        #expect(frame.code == nil)
        #expect(frame.reason.isEmpty)
    }

    // MARK: - Truncated status code

    @Test("A single-byte payload cannot hold the 2-byte code and is rejected")
    func truncatedStatusCodeRejected() {
        #expect(throws: TransmissionError.self) {
            _ = try WebSocketCloseFrame.parse(payload: [0x03])
        }
    }

    // MARK: - Reason phrase decoding

    @Test("A valid code with a UTF-8 reason phrase parses both fields")
    func codeWithReasonPhrase() throws {
        let frame = try WebSocketCloseFrame.parse(payload: payload(code: 1000, reason: "bye"))
        #expect(frame.code == 1000)
        #expect(frame.reason == "bye")
    }

    @Test("A multi-byte UTF-8 reason phrase round-trips correctly")
    func multiByteUTF8Reason() throws {
        // "café" exercises a 2-byte UTF-8 sequence.
        let frame = try WebSocketCloseFrame.parse(payload: payload(code: 1001, reason: "café"))
        #expect(frame.code == 1001)
        #expect(frame.reason == "café")
    }

    @Test("An invalid UTF-8 reason phrase is rejected")
    func invalidUTF8ReasonRejected() {
        // 0xFF is never a valid UTF-8 lead byte.
        let bytes: [UInt8] = [0x03, 0xE8, 0xFF]  // code 1000 + invalid byte
        #expect(throws: TransmissionError.self) {
            _ = try WebSocketCloseFrame.parse(payload: bytes)
        }
    }

    // MARK: - Control-frame length boundary

    @Test("A payload at the 125-byte control-frame limit is accepted")
    func payloadAtControlFrameLimitAccepted() throws {
        // 2 code bytes + 123 reason bytes == 125 (the maximum control-frame payload).
        let reason = String(repeating: "x", count: webSocketControlFramePayloadMaxLength - 2)
        let bytes = payload(code: 1000, reason: reason)
        #expect(bytes.count == webSocketControlFramePayloadMaxLength)
        let frame = try WebSocketCloseFrame.parse(payload: bytes)
        #expect(frame.code == 1000)
        #expect(frame.reason.count == webSocketControlFramePayloadMaxLength - 2)
    }

    @Test("A payload one byte over the 125-byte limit is rejected")
    func oversizedPayloadRejected() {
        // 2 code bytes + 124 reason bytes == 126 (one over the limit).
        let reason = String(repeating: "x", count: webSocketControlFramePayloadMaxLength - 1)
        let bytes = payload(code: 1000, reason: reason)
        #expect(bytes.count == webSocketControlFramePayloadMaxLength + 1)
        #expect(throws: TransmissionError.self) {
            _ = try WebSocketCloseFrame.parse(payload: bytes)
        }
    }

    // MARK: - Big-endian decoding

    @Test("The status code is decoded in network byte order (big-endian)")
    func bigEndianDecoding() throws {
        // 0x03E8 == 1000. A naive little-endian read would yield 0xE803 == 59395.
        let frame = try WebSocketCloseFrame.parse(payload: [0x03, 0xE8])
        #expect(frame.code == 1000)
    }
}
