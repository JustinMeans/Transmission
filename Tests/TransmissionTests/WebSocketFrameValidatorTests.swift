import Testing
import Foundation
@testable import Transmission

/// Tests for the RFC 6455 §5.2 / §5.5 frame-level validator in `WebSocketFrameValidator`.
///
/// RFC 6455 §5.2 mandates:
///   - RSV1, RSV2, RSV3 MUST all be zero (absent an extension).
///   - Opcodes 0x3–0x7 and 0x0B–0x0F are reserved and MUST NOT appear on the wire.
///
/// RFC 6455 §5.5 adds:
///   - Control frames (opcode ≥ 0x08) MUST NOT be fragmented (FIN MUST be set).
///   - Control frames MUST carry at most 125 bytes of application data.
///
/// Each test targets exactly one rule or boundary value so that a regression
/// points directly to the violated invariant.
@Suite("WebSocket Frame Validator")
struct WebSocketFrameValidatorTests {

    // MARK: - Helpers

    /// Builds a `WebSocketFrameFlags` with sensible defaults (valid unless overridden).
    private func flags(
        fin: Bool = true,
        rsv: UInt8 = 0,
        opcode: UInt8 = 0x1,       // text frame
        payloadLength: UInt64 = 0
    ) -> WebSocketFrameFlags {
        WebSocketFrameFlags(fin: fin, reservedBits: rsv, opcode: opcode, payloadLength: payloadLength)
    }

    // MARK: - RSV bits

    @Test("A frame with all RSV bits clear is not rejected for RSV reasons")
    func allRSVBitsClearAccepted() {
        let result = WebSocketFrameValidator.validate(flags(rsv: 0))
        #expect(result == .valid)
    }

    @Test("RSV1 set without extension is a protocol error",
          arguments: [0b100, 0b010, 0b001, 0b111, 0b110, 0b101, 0b011] as [UInt8])
    func anyNonZeroRSVIsViolation(rsv: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(rsv: rsv))
        if case .invalid(let v) = result, case .reservedBitsSet(let bits) = v {
            #expect(bits == rsv & 0x07)
        } else {
            Issue.record("Expected .invalid(.reservedBitsSet) for rsv=\(rsv), got \(result)")
        }
    }

    @Test("byte0 convenience init decodes RSV bits correctly")
    func byte0DecodesRSV() {
        // byte0 = 0b0_101_0001: FIN=0, RSV=0b101, opcode=text(1)
        let f = WebSocketFrameFlags(byte0: 0b0101_0001, byte1: 0x00)
        #expect(f.fin == false)
        #expect(f.reservedBits == 0b101)
        #expect(f.opcode == 0x1)
    }

    // MARK: - Opcode validity

    @Test("All defined data-frame opcodes are accepted",
          arguments: [0x0, 0x1, 0x2] as [UInt8])
    func definedDataFrameOpcodes(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(opcode: opcode))
        #expect(result == .valid)
    }

    @Test("All defined control-frame opcodes are accepted",
          arguments: [0x8, 0x9, 0xA] as [UInt8])
    func definedControlFrameOpcodes(opcode: UInt8) {
        // Control frames require FIN=1 and payload ≤ 125; use defaults that satisfy both.
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: opcode, payloadLength: 0))
        #expect(result == .valid)
    }

    @Test("Reserved data-frame opcodes 0x3–0x7 are rejected",
          arguments: [0x3, 0x4, 0x5, 0x6, 0x7] as [UInt8])
    func reservedDataOpcodes(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(opcode: opcode))
        if case .invalid(let v) = result, case .reservedOpcode(let o) = v {
            #expect(o == opcode)
        } else {
            Issue.record("Expected .invalid(.reservedOpcode) for opcode 0x\(String(opcode, radix: 16))")
        }
    }

    @Test("Reserved control-frame opcodes 0x0B–0x0F are rejected",
          arguments: [0xB, 0xC, 0xD, 0xE, 0xF] as [UInt8])
    func reservedControlOpcodes(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(opcode: opcode))
        if case .invalid(let v) = result, case .reservedOpcode(let o) = v {
            #expect(o == opcode)
        } else {
            Issue.record("Expected .invalid(.reservedOpcode) for opcode 0x\(String(opcode, radix: 16))")
        }
    }

    @Test("isKnownOpcode matches validate for all 16 opcode values")
    func isKnownOpcodeConsistencyWithValidate() {
        for opcode: UInt8 in 0x0...0xF {
            let known = WebSocketFrameValidator.isKnownOpcode(opcode)
            // For non-control frames, validate solely on opcode knowledge.
            // For control frames use FIN=1, payload=0 to isolate the opcode check.
            let f = WebSocketFrameFlags(fin: true, reservedBits: 0, opcode: opcode, payloadLength: 0)
            let result = WebSocketFrameValidator.validate(f)
            if known {
                #expect(result == .valid,
                        "opcode 0x\(String(opcode, radix: 16)) is known but validate rejected it")
            } else {
                if case .invalid(let v) = result, case .reservedOpcode = v {
                    // expected
                } else {
                    Issue.record("opcode 0x\(String(opcode, radix: 16)) is unknown but not rejected for .reservedOpcode")
                }
            }
        }
    }

    // MARK: - isControlOpcode helper

    @Test("isControlOpcode returns true for opcodes 0x8, 0x9, 0xA",
          arguments: [0x8, 0x9, 0xA] as [UInt8])
    func isControlOpcodeTrue(opcode: UInt8) {
        #expect(WebSocketFrameValidator.isControlOpcode(opcode))
    }

    @Test("isControlOpcode returns false for data-frame opcodes",
          arguments: [0x0, 0x1, 0x2] as [UInt8])
    func isControlOpcodeFalse(opcode: UInt8) {
        #expect(!WebSocketFrameValidator.isControlOpcode(opcode))
    }

    // MARK: - Control-frame fragmentation

    @Test("A control frame with FIN=0 is rejected as fragmented",
          arguments: [0x8, 0x9, 0xA] as [UInt8])
    func controlFrameWithFINZeroIsFragmented(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(fin: false, opcode: opcode, payloadLength: 0))
        if case .invalid(let v) = result, case .controlFrameFragmented(let o) = v {
            #expect(o == opcode)
        } else {
            Issue.record("Expected .invalid(.controlFrameFragmented) for opcode 0x\(String(opcode, radix: 16))")
        }
    }

    @Test("A control frame with FIN=1 is not rejected for fragmentation")
    func controlFrameWithFINOneAccepted() {
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: 0x9, payloadLength: 0))
        #expect(result == .valid)
    }

    @Test("Data frames may carry FIN=0 (fragmented message continuation)")
    func dataFrameWithFINZeroAccepted() {
        // A continuation or middle fragment of a data message: FIN=0, opcode=text.
        let result = WebSocketFrameValidator.validate(flags(fin: false, opcode: 0x1, payloadLength: 0))
        #expect(result == .valid)
    }

    // MARK: - Control-frame payload length

    @Test("A control frame with exactly 125-byte payload is at the limit and accepted",
          arguments: [0x8, 0x9, 0xA] as [UInt8])
    func controlFrameAtExactLimit(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: opcode, payloadLength: 125))
        #expect(result == .valid)
    }

    @Test("A control frame with 126-byte payload (one over limit) is rejected",
          arguments: [0x8, 0x9, 0xA] as [UInt8])
    func controlFrameOneOverLimit(opcode: UInt8) {
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: opcode, payloadLength: 126))
        if case .invalid(let v) = result, case .controlFramePayloadTooLong(let o, let len) = v {
            #expect(o == opcode)
            #expect(len == 126)
        } else {
            Issue.record("Expected .invalid(.controlFramePayloadTooLong) for opcode 0x\(String(opcode, radix: 16))")
        }
    }

    @Test("A control frame with 0-byte payload is accepted")
    func controlFrameZeroLengthAccepted() {
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: 0x9, payloadLength: 0))
        #expect(result == .valid)
    }

    @Test("Data frames may carry payloads larger than 125 bytes")
    func dataFrameLargePayloadAccepted() {
        // Binary frame with 65536-byte payload: valid (no length constraint on data frames).
        let result = WebSocketFrameValidator.validate(flags(fin: true, opcode: 0x2, payloadLength: 65536))
        #expect(result == .valid)
    }

    // MARK: - validateOrThrow

    @Test("validateOrThrow does not throw for a valid frame")
    func validateOrThrowValidFrame() throws {
        let f = WebSocketFrameFlags(fin: true, reservedBits: 0, opcode: 0x1, payloadLength: 10)
        try WebSocketFrameValidator.validateOrThrow(f)
    }

    @Test("validateOrThrow throws WebSocketFrameViolation for a reserved opcode")
    func validateOrThrowThrowsForReservedOpcode() {
        let f = WebSocketFrameFlags(fin: true, reservedBits: 0, opcode: 0x3, payloadLength: 0)
        #expect(throws: WebSocketFrameViolation.self) {
            try WebSocketFrameValidator.validateOrThrow(f)
        }
    }

    @Test("validateOrThrow throws WebSocketFrameViolation for a fragmented control frame")
    func validateOrThrowThrowsForFragmentedControlFrame() {
        let f = WebSocketFrameFlags(fin: false, reservedBits: 0, opcode: 0x9, payloadLength: 0)
        #expect(throws: WebSocketFrameViolation.self) {
            try WebSocketFrameValidator.validateOrThrow(f)
        }
    }

    @Test("validateOrThrow throws WebSocketFrameViolation for oversized control frame")
    func validateOrThrowThrowsForOversizedControlFrame() {
        let f = WebSocketFrameFlags(fin: true, reservedBits: 0, opcode: 0x8, payloadLength: 200)
        #expect(throws: WebSocketFrameViolation.self) {
            try WebSocketFrameValidator.validateOrThrow(f)
        }
    }

    // MARK: - byte0 convenience init round-trip

    @Test("byte0/byte1 init decodes a valid close frame header (0x88 0x05)")
    func byte0InitDecodeCloseFrame() {
        // 0x88 = 1000_1000: FIN=1, RSV=000, opcode=0x8 (close)
        // 0x05 = 0000_0101: MASK=0, payload_len=5
        let f = WebSocketFrameFlags(byte0: 0x88, byte1: 0x05)
        #expect(f.fin == true)
        #expect(f.reservedBits == 0)
        #expect(f.opcode == 0x8)
        #expect(f.payloadLength == 5)
        #expect(WebSocketFrameValidator.validate(f) == .valid)
    }

    @Test("byte0/byte1 init decodes a fragmented text frame (0x01 0x7D)")
    func byte0InitDecodeFragmentedTextFrame() {
        // 0x01 = 0000_0001: FIN=0, RSV=000, opcode=0x1 (text)
        // 0x7D = 0111_1101: MASK=0, payload_len=125
        let f = WebSocketFrameFlags(byte0: 0x01, byte1: 0x7D)
        #expect(f.fin == false)
        #expect(f.reservedBits == 0)
        #expect(f.opcode == 0x1)
        #expect(f.payloadLength == 125)
        // Data frames may be fragmented and may carry 125 bytes: valid.
        #expect(WebSocketFrameValidator.validate(f) == .valid)
    }

    @Test("RSV bits set in byte0 are detected and reported")
    func byte0RSVBitSetDetected() {
        // 0b0110_0001: FIN=0, RSV1=1, RSV2=1, RSV3=0, opcode=1
        // (byte0 >> 4) & 0x07 extracts bits 6-4 as a 3-bit value = 0b110 = 6
        let f = WebSocketFrameFlags(byte0: 0b0110_0001, byte1: 0x00)
        #expect(f.reservedBits == 0b110)
        if case .invalid(let v) = WebSocketFrameValidator.validate(f),
           case .reservedBitsSet(let bits) = v {
            #expect(bits == 0b110)
        } else {
            Issue.record("Expected .reservedBitsSet(0b110)")
        }
    }
}
