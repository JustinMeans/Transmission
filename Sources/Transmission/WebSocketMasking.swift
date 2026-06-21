/// WebSocketMasking.swift
///
/// RFC 6455 §5.3 — Client-to-Server Masking
///
/// All frames sent from a WebSocket client MUST be masked with a 4-byte
/// masking key chosen at random per frame.  The mask is applied by XOR-ing
/// each payload byte with `key[i % 4]`.
///
/// This file provides:
///   - In-place and copy mask/unmask helpers (mask == unmask; XOR is its own inverse).
///   - `WebSocketFrameHeader` — builds the complete binary header for a masked
///     client frame including FIN, opcode, MASK bit, payload-length encoding
///     (7-bit / 7+16-bit / 7+64-bit) and the 4-byte masking key.

// MARK: - Payload masking / unmasking

/// Masks (or unmasks) `payload` in place using `key`.
///
/// - Parameters:
///   - payload: The bytes to transform.  Modified in place.
///   - key:     The 4-byte masking key (RFC 6455 §5.3).
public func webSocketMask(_ payload: inout [UInt8], key: (UInt8, UInt8, UInt8, UInt8)) {
    let k: [UInt8] = [key.0, key.1, key.2, key.3]
    for i in payload.indices {
        payload[i] ^= k[i & 3]
    }
}

/// Returns a new array with `payload` masked (or unmasked) using `key`.
///
/// - Parameters:
///   - payload: The bytes to transform.  Not modified.
///   - key:     The 4-byte masking key (RFC 6455 §5.3).
/// - Returns:   A new `[UInt8]` with each byte XOR-ed against the appropriate key byte.
public func webSocketMasked(_ payload: [UInt8], key: (UInt8, UInt8, UInt8, UInt8)) -> [UInt8] {
    let k: [UInt8] = [key.0, key.1, key.2, key.3]
    return payload.enumerated().map { i, byte in byte ^ k[i & 3] }
}

// MARK: - WebSocket opcodes

/// WebSocket frame opcodes defined by RFC 6455 §5.2.
///
/// Named `WSOpcode` to avoid shadowing `NIOWebSocket.WebSocketOpcode`.
public enum WSOpcode: UInt8, Sendable {
    case continuation = 0x0
    case text         = 0x1
    case binary       = 0x2
    case close        = 0x8
    case ping         = 0x9
    case pong         = 0xA
}

// MARK: - Frame header builder

/// Builds the binary header for a single masked WebSocket client frame.
///
/// Encoding rules (RFC 6455 §5.2):
/// ```
/// Byte 0:  FIN(1) | RSV1-3(000) | opcode(4)
/// Byte 1:  MASK(1) | payload length(7)
///            0-125    → length fits in 7 bits; header = 2 + 4 bytes key
///            126      → next 2 bytes are uint16 big-endian length
///            127      → next 8 bytes are uint64 big-endian length
/// Bytes ?:  4-byte masking key
/// ```
///
/// The caller appends the masked payload after the returned header.
public struct WebSocketFrameHeader: Sendable {

    /// The raw header bytes (variable length: 6, 8, or 14 bytes).
    public let bytes: [UInt8]

    /// Builds a header for a masked client frame.
    ///
    /// - Parameters:
    ///   - fin:           Set `true` for the final (or only) fragment.
    ///   - opcode:        Frame type.
    ///   - payloadLength: Number of bytes in the (unmasked) payload.
    ///   - maskingKey:    4-byte random masking key.
    public init(
        fin: Bool,
        opcode: WSOpcode,
        payloadLength: UInt64,
        maskingKey: (UInt8, UInt8, UInt8, UInt8)
    ) {
        var buf: [UInt8] = []
        buf.reserveCapacity(14)

        // Byte 0: FIN | RSV(000) | opcode
        let byte0: UInt8 = (fin ? 0x80 : 0x00) | (opcode.rawValue & 0x0F)
        buf.append(byte0)

        // Byte 1 + extended length
        let mask: UInt8 = 0x80   // MASK bit always set for client frames
        switch payloadLength {
        case 0...125:
            buf.append(mask | UInt8(payloadLength))
        case 126...65535:
            buf.append(mask | 126)
            let len16 = UInt16(payloadLength)
            buf.append(UInt8((len16 >> 8) & 0xFF))
            buf.append(UInt8(len16 & 0xFF))
        default:
            buf.append(mask | 127)
            for shift in stride(from: 56, through: 0, by: -8) {
                buf.append(UInt8((payloadLength >> shift) & 0xFF))
            }
        }

        // 4-byte masking key
        buf.append(maskingKey.0)
        buf.append(maskingKey.1)
        buf.append(maskingKey.2)
        buf.append(maskingKey.3)

        self.bytes = buf
    }
}
