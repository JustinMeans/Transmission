/// WebSocketFrameValidator.swift
///
/// RFC 6455 §5.2 — Base Framing Protocol: Frame-Level Validation
///
/// This file enforces the structural constraints every WebSocket endpoint MUST
/// apply when it receives an incoming frame header, regardless of whether it is
/// acting as a client or server:
///
///   1. **Reserved bits** (RSV1, RSV2, RSV3 in byte 0) MUST all be zero unless an
///      extension negotiated during the handshake defines their meaning. This
///      implementation models the common case of no extensions — any non-zero RSV
///      bit is a protocol error (RFC 6455 §5.2, §7.2).
///
///   2. **Opcode validity** — opcodes 0x3–0x7 and 0x0B–0x0F are reserved by the
///      RFC and MUST NOT appear on the wire. Only the six defined opcodes are
///      accepted: continuation (0), text (1), binary (2), close (8), ping (9),
///      pong (10) (RFC 6455 §5.2).
///
///   3. **Control-frame fragmentation** — control frames (opcode ≥ 0x08) MUST NOT
///      be fragmented; their FIN bit MUST be set (RFC 6455 §5.5).
///
///   4. **Control-frame payload length** — control frames MUST carry at most 125
///      bytes of application data (RFC 6455 §5.5).
///
/// `WebSocketFrameFlags` models the four header bits that the validator inspects.
/// This keeps the validation logic independent of any specific transport, making
/// every rule directly and cheaply unit-testable.

// MARK: - Frame flags (extracted header bits)

/// The four header-level bit-fields that §5.2 validation cares about.
///
/// A real WebSocket implementation extracts these from byte 0 and the payload
/// length field. This struct lets the validator operate on them directly without
/// requiring a full frame deserialiser.
public struct WebSocketFrameFlags: Sendable, Equatable {
    /// Whether this is the final fragment (FIN bit, bit 7 of byte 0).
    public let fin: Bool

    /// Non-zero bits here indicate RSV1, RSV2, or RSV3 are set
    /// (bits 6–4 of byte 0); packed as a 3-bit value in bits 2–0.
    public let reservedBits: UInt8

    /// The 4-bit opcode (bits 3–0 of byte 0).
    public let opcode: UInt8

    /// The payload length (after decoding any extended-length encoding).
    public let payloadLength: UInt64

    /// Creates a frame-flags value.
    ///
    /// - Parameters:
    ///   - fin:           FIN bit.
    ///   - reservedBits:  3-bit RSV field (RSV1=bit2, RSV2=bit1, RSV3=bit0).
    ///   - opcode:        4-bit opcode.
    ///   - payloadLength: Decoded payload length in bytes.
    public init(fin: Bool, reservedBits: UInt8, opcode: UInt8, payloadLength: UInt64) {
        self.fin           = fin
        self.reservedBits  = reservedBits & 0x07
        self.opcode        = opcode & 0x0F
        self.payloadLength = payloadLength
    }

    /// Convenience initialiser that decodes all four fields from the first two
    /// raw header bytes (with the 7-bit payload length only; does not handle
    /// the 16-bit or 64-bit extended forms).
    ///
    /// Use this for tests against compact frames (payload ≤ 125 bytes).
    ///
    /// Layout of `byte0`:
    /// ```
    /// bit 7:   FIN
    /// bit 6:   RSV1
    /// bit 5:   RSV2
    /// bit 4:   RSV3
    /// bits 3-0: opcode
    /// ```
    /// Layout of `byte1` (low 7 bits = initial payload length, mask bit ignored):
    /// ```
    /// bit 7:   MASK
    /// bits 6-0: payload length (0-125 for compact form)
    /// ```
    public init(byte0: UInt8, byte1: UInt8) {
        fin          = (byte0 & 0x80) != 0
        reservedBits = (byte0 >> 4) & 0x07
        opcode       = byte0 & 0x0F
        payloadLength = UInt64(byte1 & 0x7F)
    }
}

// MARK: - Validation result

/// The outcome of validating a frame header.
public enum WebSocketFrameValidationResult: Sendable, Equatable {
    /// The frame header passes all RFC 6455 §5.2 and §5.5 structural checks.
    case valid

    /// The frame violates one or more RFC 6455 constraints.
    case invalid(WebSocketFrameViolation)
}

/// A specific RFC 6455 protocol violation detected in a frame header.
public enum WebSocketFrameViolation: Sendable, Equatable, Error {
    /// One or more reserved bits (RSV1/RSV2/RSV3) were non-zero without an
    /// extension negotiated to define their meaning (RFC 6455 §5.2).
    case reservedBitsSet(UInt8)

    /// The opcode is in the reserved range (0x3–0x7 or 0x0B–0x0F) and MUST NOT
    /// appear on the wire (RFC 6455 §5.2).
    case reservedOpcode(UInt8)

    /// A control frame (opcode ≥ 0x08) carried FIN=0, which would fragment it.
    /// Control frames MUST NOT be fragmented (RFC 6455 §5.5).
    case controlFrameFragmented(opcode: UInt8)

    /// A control frame carried a payload exceeding the 125-byte limit imposed by
    /// RFC 6455 §5.5.
    case controlFramePayloadTooLong(opcode: UInt8, length: UInt64)
}

// MARK: - Validator

/// Validates WebSocket frame headers against the structural rules in
/// RFC 6455 §5.2 (base framing) and §5.5 (control frames).
///
/// No state is required; all methods are static.
public enum WebSocketFrameValidator {

    // MARK: Public API

    /// Validates `flags` against all applicable RFC 6455 §5.2 and §5.5 rules.
    ///
    /// Checks are applied in the order they appear in the RFC:
    /// 1. Reserved bits must be zero.
    /// 2. Opcode must be a known, non-reserved value.
    /// 3. Control frames must not be fragmented (FIN must be set).
    /// 4. Control frames must have payload ≤ 125 bytes.
    ///
    /// - Parameter flags: The header bit-fields extracted from an incoming frame.
    /// - Returns: `.valid` if all checks pass; `.invalid(violation)` on first failure.
    public static func validate(_ flags: WebSocketFrameFlags) -> WebSocketFrameValidationResult {
        // 1. Reserved bits (RFC 6455 §5.2 — "MUST be 0 unless … extension is negotiated").
        if flags.reservedBits != 0 {
            return .invalid(.reservedBitsSet(flags.reservedBits))
        }

        // 2. Opcode validity (RFC 6455 §5.2 table).
        guard isKnownOpcode(flags.opcode) else {
            return .invalid(.reservedOpcode(flags.opcode))
        }

        // Rules 3 and 4 apply only to control frames (opcode ≥ 0x08).
        guard flags.opcode >= 0x08 else {
            return .valid
        }

        // 3. Control frames MUST NOT be fragmented (RFC 6455 §5.5).
        if !flags.fin {
            return .invalid(.controlFrameFragmented(opcode: flags.opcode))
        }

        // 4. Control-frame payload MUST be ≤ 125 bytes (RFC 6455 §5.5).
        if flags.payloadLength > 125 {
            return .invalid(.controlFramePayloadTooLong(opcode: flags.opcode, length: flags.payloadLength))
        }

        return .valid
    }

    /// Throws a `WebSocketFrameViolation` if `flags` fails validation, otherwise
    /// returns normally. Convenience wrapper around `validate(_:)` for call sites
    /// that prefer throwing over a result type.
    ///
    /// - Parameter flags: The header bit-fields to validate.
    /// - Throws: `WebSocketFrameViolation` describing the first detected issue.
    public static func validateOrThrow(_ flags: WebSocketFrameFlags) throws {
        if case .invalid(let violation) = validate(flags) {
            throw violation
        }
    }

    // MARK: Internal helpers

    /// Returns `true` when `opcode` is one of the six defined, non-reserved
    /// WebSocket opcodes from RFC 6455 §5.2.
    ///
    /// Defined data-frame opcodes:
    ///   - 0x0 continuation
    ///   - 0x1 text
    ///   - 0x2 binary
    ///   - 0x3–0x7 reserved (NOT valid)
    ///
    /// Defined control-frame opcodes:
    ///   - 0x8 close
    ///   - 0x9 ping
    ///   - 0xA pong
    ///   - 0xB–0xF reserved (NOT valid)
    ///
    /// - Parameter opcode: The 4-bit opcode value.
    static func isKnownOpcode(_ opcode: UInt8) -> Bool {
        switch opcode {
        case 0x0, 0x1, 0x2:   // data frames
            return true
        case 0x8, 0x9, 0xA:   // control frames
            return true
        default:
            return false
        }
    }

    /// Returns `true` when `opcode` represents a control frame (≥ 0x08).
    ///
    /// - Parameter opcode: The 4-bit opcode value.
    public static func isControlOpcode(_ opcode: UInt8) -> Bool {
        opcode >= 0x08
    }
}
