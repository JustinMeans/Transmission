import Foundation

/// A parsed and validated WebSocket Close frame body (RFC 6455 section 5.5.1).
///
/// A Close control frame MAY carry an application-data payload. When present, the
/// first two bytes are an unsigned 16-bit status code in network byte order
/// (big-endian), optionally followed by a UTF-8 reason phrase. The whole payload
/// is constrained by the control-frame rule that its length MUST be <= 125 bytes
/// (RFC 6455 section 5.5), so the reason phrase occupies at most 123 bytes.
///
/// A Close frame is also permitted to carry NO payload at all, in which case there
/// is no status code and no reason (treated as status code 1005 "no status
/// received" by the protocol, but represented here as an absent `code`).
///
/// This type models the *parsed* result. Parsing and validation live in the static
/// `parse(payload:)` entry point so the rules can be exercised directly against raw
/// payload bytes — independent of any transport (e.g. NIO `WebSocketFrame.data`) —
/// which keeps the wire-validation logic pure and unit-testable.
public struct WebSocketCloseFrame: Sendable, Equatable {
    /// The 2-byte status code, or `nil` when the Close frame carried an empty
    /// payload (no code, which RFC 6455 treats as 1005 "no status received").
    public let code: UInt16?

    /// The optional UTF-8 reason phrase following the status code. Empty when the
    /// payload contained only a status code or was itself empty.
    public let reason: String

    /// Creates a parsed Close frame value.
    /// - Parameters:
    ///   - code: The status code, or `nil` for an empty (codeless) Close payload.
    ///   - reason: The decoded UTF-8 reason phrase (empty if none).
    public init(code: UInt16?, reason: String = "") {
        self.code = code
        self.reason = reason
    }
}

/// The maximum length, in bytes, of any WebSocket control-frame payload, including
/// the Close frame body (RFC 6455 section 5.5): "All control frames ... MUST have a
/// payload length of 125 bytes or less".
public let webSocketControlFramePayloadMaxLength = 125

extension WebSocketCloseFrame {
    /// Validates a Close status code against the IANA "WebSocket Close Code Number
    /// Registry" (RFC 6455 section 7.4).
    ///
    /// Permitted codes:
    /// - `1000`-`1003` — normal closure, going away, protocol error, unsupported data.
    /// - `1007`-`1011` — invalid payload data, policy violation, message too big,
    ///   mandatory extension, internal error.
    /// - `3000`-`3999` — registered for use by libraries, frameworks, and applications.
    /// - `4000`-`4999` — reserved for private use.
    ///
    /// Explicitly rejected codes:
    /// - `< 1000` — undefined / reserved low range; never valid on the wire.
    /// - `1004` — reserved, no meaning assigned by RFC 6455.
    /// - `1005` — "no status received": a *reserved* pseudo-code that MUST NOT be set
    ///   in an actual Close frame (a codeless Close is the wire representation instead).
    /// - `1006` — "abnormal closure": reserved, MUST NOT be sent on the wire.
    /// - `1012`-`2999` — not assigned for endpoint use here (the registry only opens
    ///   `1000`-`1011` for the protocol; `1015` and the rest are reserved).
    /// - `1015` — "TLS handshake failure": reserved, MUST NOT be sent on the wire.
    /// - `> 4999` — beyond the defined private-use range.
    ///
    /// - Parameter code: The candidate status code.
    /// - Returns: `true` if the code is valid to appear in a Close frame on the wire.
    public static func isValidCloseCode(_ code: UInt16) -> Bool {
        switch code {
        case 1000...1003:
            return true
        case 1007...1011:
            return true
        case 3000...4999:
            return true
        default:
            // Covers < 1000, the reserved 1004/1005/1006, the unassigned
            // 1012...2999 range (including 1015), and everything > 4999.
            return false
        }
    }

    /// Parses and validates the application-data payload of a Close control frame
    /// (RFC 6455 section 5.5.1).
    ///
    /// Validation performed, in order:
    /// 1. **Length bound** — the payload MUST be <= 125 bytes (control-frame limit).
    ///    A longer payload is a protocol violation and is rejected.
    /// 2. **Empty payload** — a zero-length payload is valid and yields a frame with
    ///    `code == nil` (no status received).
    /// 3. **Truncated code** — a single-byte payload cannot hold the 2-byte status
    ///    code and is rejected; a status code, once started, must be complete.
    /// 4. **Status code validity** — the big-endian 2-byte code MUST be a value the
    ///    IANA registry permits (`isValidCloseCode(_:)`); reserved/undefined codes
    ///    are rejected.
    /// 5. **Reason phrase** — any bytes after the code are decoded as UTF-8. Invalid
    ///    UTF-8 is rejected (RFC 6455 requires the reason to be valid UTF-8 text).
    ///
    /// - Parameter payload: The raw, already-unmasked Close-frame application data.
    /// - Returns: The parsed and validated `WebSocketCloseFrame`.
    /// - Throws: `TransmissionError.decodingFailed` describing the first violation.
    public static func parse(payload: [UInt8]) throws -> WebSocketCloseFrame {
        // 1. Control-frame payload length bound (RFC 6455 section 5.5).
        guard payload.count <= webSocketControlFramePayloadMaxLength else {
            throw TransmissionError.decodingFailed(
                "Close frame payload of \(payload.count) bytes exceeds the control-frame "
                + "limit of \(webSocketControlFramePayloadMaxLength) bytes")
        }

        // 2. Empty payload: a valid codeless Close (no status received).
        if payload.isEmpty {
            return WebSocketCloseFrame(code: nil, reason: "")
        }

        // 3. A payload that has started a status code must contain both bytes.
        guard payload.count >= 2 else {
            throw TransmissionError.decodingFailed(
                "Close frame payload of 1 byte cannot hold the 2-byte status code")
        }

        // Decode the 16-bit status code in network byte order (big-endian).
        let code = (UInt16(payload[0]) << 8) | UInt16(payload[1])

        // 4. Reject reserved/undefined status codes per the IANA registry.
        guard isValidCloseCode(code) else {
            throw TransmissionError.decodingFailed(
                "Close frame carries reserved or invalid status code \(code)")
        }

        // 5. Decode any trailing bytes as the UTF-8 reason phrase.
        let reasonBytes = payload[2...]
        if reasonBytes.isEmpty {
            return WebSocketCloseFrame(code: code, reason: "")
        }
        guard let reason = String(bytes: reasonBytes, encoding: .utf8) else {
            throw TransmissionError.decodingFailed(
                "Close frame reason phrase is not valid UTF-8")
        }

        return WebSocketCloseFrame(code: code, reason: reason)
    }
}
