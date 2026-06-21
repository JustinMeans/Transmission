import NIO
import NIOWebSocket

/// WebSocketCloseHandshake.swift
///
/// RFC 6455 section 5.5.1 — Close Handshake
///
/// When an endpoint receives a Close frame it MUST send a Close frame in
/// response before closing the connection. The responding endpoint should
/// echo back the status code and reason from the received frame unless the
/// received payload violates the control-frame rules, in which case a 1002
/// (Protocol Error) response is appropriate.
///
/// RFC 6455 section 5.5.1 (abridged):
///   "Upon receiving such a frame, the other peer sends a Close frame in
///    response. It is safe for both peers to send a Close frame at the same
///    time; once the frame is received, the peer is confirmed to have
///    received all data before the close."
///
/// This module provides:
///   - `webSocketCloseEchoPayload(from:)` — derives the payload bytes that the
///     responding endpoint should place in its outbound Close frame. Pure function,
///     no NIO dependency, directly unit-testable.
///   - `webSocketCloseEchoFrame(for:)` — builds the `WebSocketFrame` (opcode
///     `.connectionClose`, `fin == true`) ready to write to the outbound channel.

// MARK: - Close echo payload

/// Derives the payload to echo in a Close frame response (RFC 6455 §5.5.1).
///
/// The RFC allows the responding endpoint to echo the status code and reason
/// received in the Close initiation frame. This implementation uses the
/// following rules to keep the echo well-formed:
///
/// 1. **Empty received payload** → echo an empty payload (no code, no reason).
///    This is the normal codeless close (status 1005 "no status received" is a
///    pseudo-code; it is represented by the absence of a payload, not by sending
///    the value 1005 on the wire).
///
/// 2. **Payload too large** (> 125 bytes, violating the control-frame length cap)
///    → echo a 1002 Protocol Error to signal the violation rather than reflecting
///    the malformed frame back verbatim.
///
/// 3. **Truncated status code** (exactly 1 byte) → same as (2): send 1002.
///
/// 4. **Reserved or invalid status code** → send 1002.
///
/// 5. **Valid code with non-UTF-8 reason** → send 1002 (RFC 6455 requires the
///    reason phrase to be valid UTF-8).
///
/// 6. **Valid code and valid (or absent) reason** → echo the received payload
///    bytes unchanged. This preserves the peer's status code and reason phrase
///    in the echo, which is informative and strictly conformant.
///
/// - Parameter received: The raw payload bytes from the inbound Close frame,
///   already unmasked (NIO unmasks client-to-server frames automatically).
/// - Returns: The payload bytes to place in the outbound echo Close frame.
public func webSocketCloseEchoPayload(from received: [UInt8]) -> [UInt8] {
    // Rule 1: empty → echo empty.
    if received.isEmpty {
        return []
    }

    // Rule 2: control-frame payload length cap (RFC 6455 §5.5).
    guard received.count <= 125 else {
        return protocolErrorPayload
    }

    // Rule 3: a single byte cannot contain a 2-byte status code.
    guard received.count >= 2 else {
        return protocolErrorPayload
    }

    // Decode the big-endian 16-bit status code.
    let code = (UInt16(received[0]) << 8) | UInt16(received[1])

    // Rule 4: reject reserved / undefined status codes.
    guard WebSocketCloseFrame.isValidCloseCode(code) else {
        return protocolErrorPayload
    }

    // Rule 5: reason phrase (if present) must be valid UTF-8.
    let reasonBytes = received[2...]
    if !reasonBytes.isEmpty {
        guard String(bytes: reasonBytes, encoding: .utf8) != nil else {
            return protocolErrorPayload
        }
    }

    // Rule 6: echo the received payload verbatim.
    return received
}

/// The 2-byte payload encoding status code 1002 (Protocol Error) in big-endian
/// network byte order. Used as the echo when the received Close frame is malformed.
private let protocolErrorPayload: [UInt8] = [
    UInt8((UInt16(1002) >> 8) & 0xFF),
    UInt8(UInt16(1002) & 0xFF)
]

// MARK: - Close echo frame builder

/// Builds a Close `WebSocketFrame` whose payload echoes `received` according to
/// the rules in `webSocketCloseEchoPayload(from:)`.
///
/// The returned frame has `fin == true` and opcode `.connectionClose`, matching
/// RFC 6455 §5.5.1. Server-to-client frames are NOT masked (servers MUST NOT
/// mask; only client-to-server frames carry a masking key per RFC 6455 §5.3).
///
/// Usage in a frame-processing loop:
/// ```swift
/// case .connectionClose:
///     let echo = webSocketCloseEchoFrame(for: frame)
///     try await outbound.write(echo)
///     return
/// ```
///
/// - Parameter received: The inbound Close `WebSocketFrame` from the peer.
/// - Returns: A well-formed Close `WebSocketFrame` to write to the outbound channel.
public func webSocketCloseEchoFrame(for received: WebSocketFrame) -> WebSocketFrame {
    var inData = received.data
    let payloadBytes = inData.readBytes(length: inData.readableBytes) ?? []
    let echoPayload = webSocketCloseEchoPayload(from: payloadBytes)
    var buf = ByteBuffer()
    buf.writeBytes(echoPayload)
    return WebSocketFrame(fin: true, opcode: .connectionClose, data: buf)
}
