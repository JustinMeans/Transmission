/// WebSocketMaskingTests.swift
///
/// Tests for RFC 6455 §5.3 WebSocket frame masking primitives.

import Testing
@testable import Transmission

@Suite("WebSocketMasking")
struct WebSocketMaskingTests {

    // MARK: - XOR round-trip

    @Test("mask then unmask returns original payload")
    func maskUnmaskRoundTrip() {
        let original: [UInt8] = [0x48, 0x65, 0x6C, 0x6C, 0x6F] // "Hello"
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x37, 0xFA, 0x21, 0x3D)

        var payload = original
        webSocketMask(&payload, key: key)
        #expect(payload != original, "masked payload must differ from original")

        webSocketMask(&payload, key: key)
        #expect(payload == original, "double-mask must restore original")
    }

    // MARK: - Known-vector XOR

    @Test("known-vector XOR matches hand-computed values")
    func knownVectorXOR() {
        // RFC 6455 §5.7 example: "Hello" masked with 0x37FA213D
        // H=0x48^0x37=0x7F  e=0x65^0xFA=0x9F  l=0x6C^0x21=0x4D
        // l=0x6C^0x3D=0x51  o=0x6F^0x37=0x58
        let payload: [UInt8] = [0x48, 0x65, 0x6C, 0x6C, 0x6F]
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x37, 0xFA, 0x21, 0x3D)
        let expected: [UInt8] = [0x7F, 0x9F, 0x4D, 0x51, 0x58]

        let result = webSocketMasked(payload, key: key)
        #expect(result == expected)
    }

    // MARK: - Copy variant does not modify original

    @Test("copy variant leaves original unchanged")
    func copyVariantPreservesOriginal() {
        let original: [UInt8] = [0x01, 0x02, 0x03, 0x04]
        let key: (UInt8, UInt8, UInt8, UInt8) = (0xAA, 0xBB, 0xCC, 0xDD)

        let masked = webSocketMasked(original, key: key)
        #expect(original == [0x01, 0x02, 0x03, 0x04], "original must be unmodified")
        #expect(masked != original)
    }

    // MARK: - Empty payload

    @Test("masking empty payload produces empty result")
    func emptyPayload() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x12, 0x34, 0x56, 0x78)

        var payload: [UInt8] = []
        webSocketMask(&payload, key: key)
        #expect(payload.isEmpty)

        let copied = webSocketMasked([], key: key)
        #expect(copied.isEmpty)
    }

    // MARK: - Key byte rotation

    @Test("key byte rotation i%4 is correct across 9 bytes")
    func keyByteRotation() {
        // All payload bytes zero; masked result equals the key bytes in rotation.
        let payload = [UInt8](repeating: 0x00, count: 9)
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x11, 0x22, 0x33, 0x44)
        let result = webSocketMasked(payload, key: key)
        let expected: [UInt8] = [0x11, 0x22, 0x33, 0x44, 0x11, 0x22, 0x33, 0x44, 0x11]
        #expect(result == expected)
    }

    // MARK: - Frame header: 7-bit length (≤ 125)

    @Test("header encodes 7-bit payload length 0")
    func header7BitZero() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0xAA, 0xBB, 0xCC, 0xDD)
        let hdr = WebSocketFrameHeader(fin: true, opcode: WSOpcode.binary, payloadLength: 0, maskingKey: key)
        // 2 bytes base + 4 key = 6 bytes total
        #expect(hdr.bytes.count == 6)
        // Byte 0: FIN(0x80) | binary(0x02) = 0x82
        #expect(hdr.bytes[0] == 0x82)
        // Byte 1: MASK(0x80) | 0 = 0x80
        #expect(hdr.bytes[1] == 0x80)
        // Key bytes
        #expect(hdr.bytes[2] == 0xAA)
        #expect(hdr.bytes[3] == 0xBB)
        #expect(hdr.bytes[4] == 0xCC)
        #expect(hdr.bytes[5] == 0xDD)
    }

    @Test("header encodes 7-bit payload length 125")
    func header7Bit125() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x01, 0x02, 0x03, 0x04)
        let hdr = WebSocketFrameHeader(fin: true, opcode: WSOpcode.text, payloadLength: 125, maskingKey: key)
        #expect(hdr.bytes.count == 6)
        #expect(hdr.bytes[0] == 0x81)          // FIN | text
        #expect(hdr.bytes[1] == 0x80 | 125)    // MASK | 125
    }

    // MARK: - Frame header: 7+16-bit length (126…65535)

    @Test("header encodes 16-bit payload length 126")
    func header16Bit126() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x01, 0x02, 0x03, 0x04)
        let hdr = WebSocketFrameHeader(fin: true, opcode: WSOpcode.binary, payloadLength: 126, maskingKey: key)
        // 2 base + 2 ext + 4 key = 8 bytes
        #expect(hdr.bytes.count == 8)
        #expect(hdr.bytes[1] == 0x80 | 126)    // MASK | 126
        // 126 encoded as uint16 big-endian
        #expect(hdr.bytes[2] == 0x00)
        #expect(hdr.bytes[3] == 0x7E)           // 126 = 0x007E
        // Key starts at byte 4
        #expect(hdr.bytes[4] == 0x01)
    }

    @Test("header encodes 16-bit payload length 65535")
    func header16Bit65535() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0xDE, 0xAD, 0xBE, 0xEF)
        let hdr = WebSocketFrameHeader(fin: false, opcode: WSOpcode.binary, payloadLength: 65535, maskingKey: key)
        #expect(hdr.bytes.count == 8)
        #expect(hdr.bytes[1] == 0x80 | 126)
        #expect(hdr.bytes[2] == 0xFF)
        #expect(hdr.bytes[3] == 0xFF)
        // FIN not set
        #expect(hdr.bytes[0] & 0x80 == 0x00)
        // Key
        #expect(hdr.bytes[4] == 0xDE)
        #expect(hdr.bytes[5] == 0xAD)
    }

    // MARK: - Frame header: 7+64-bit length (≥ 65536)

    @Test("header encodes 64-bit payload length 65536")
    func header64Bit65536() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x11, 0x22, 0x33, 0x44)
        let hdr = WebSocketFrameHeader(fin: true, opcode: WSOpcode.binary, payloadLength: 65536, maskingKey: key)
        // 2 base + 8 ext + 4 key = 14 bytes
        #expect(hdr.bytes.count == 14)
        #expect(hdr.bytes[1] == 0x80 | 127)
        // 65536 = 0x0000_0000_0001_0000
        #expect(hdr.bytes[2]  == 0x00)
        #expect(hdr.bytes[3]  == 0x00)
        #expect(hdr.bytes[4]  == 0x00)
        #expect(hdr.bytes[5]  == 0x00)
        #expect(hdr.bytes[6]  == 0x00)
        #expect(hdr.bytes[7]  == 0x01)
        #expect(hdr.bytes[8]  == 0x00)
        #expect(hdr.bytes[9]  == 0x00)
        // Key starts at byte 10
        #expect(hdr.bytes[10] == 0x11)
        #expect(hdr.bytes[11] == 0x22)
        #expect(hdr.bytes[12] == 0x33)
        #expect(hdr.bytes[13] == 0x44)
    }

    // MARK: - Masking key placement in header

    @Test("masking key bytes are placed after length in all three encoding forms")
    func maskingKeyPlacement() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0xCA, 0xFE, 0xBA, 0xBE)

        // 7-bit: key at [2..5]
        let h7 = WebSocketFrameHeader(fin: true, opcode: WSOpcode.ping, payloadLength: 10, maskingKey: key)
        #expect([h7.bytes[2], h7.bytes[3], h7.bytes[4], h7.bytes[5]] == [0xCA, 0xFE, 0xBA, 0xBE])

        // 16-bit: key at [4..7]
        let h16 = WebSocketFrameHeader(fin: true, opcode: WSOpcode.ping, payloadLength: 200, maskingKey: key)
        #expect([h16.bytes[4], h16.bytes[5], h16.bytes[6], h16.bytes[7]] == [0xCA, 0xFE, 0xBA, 0xBE])

        // 64-bit: key at [10..13]
        let h64 = WebSocketFrameHeader(fin: true, opcode: WSOpcode.ping, payloadLength: 70000, maskingKey: key)
        #expect([h64.bytes[10], h64.bytes[11], h64.bytes[12], h64.bytes[13]] == [0xCA, 0xFE, 0xBA, 0xBE])
    }

    // MARK: - Opcode encoding

    @Test("FIN bit and opcode are encoded correctly in byte 0")
    func finAndOpcodeEncoding() {
        let key: (UInt8, UInt8, UInt8, UInt8) = (0x00, 0x00, 0x00, 0x00)

        let textFin   = WebSocketFrameHeader(fin: true,  opcode: WSOpcode.text,         payloadLength: 0, maskingKey: key)
        let binaryNoFin = WebSocketFrameHeader(fin: false, opcode: WSOpcode.binary,     payloadLength: 0, maskingKey: key)
        let closeFrame  = WebSocketFrameHeader(fin: true,  opcode: WSOpcode.close,      payloadLength: 0, maskingKey: key)
        let pingFrame   = WebSocketFrameHeader(fin: true,  opcode: WSOpcode.ping,       payloadLength: 0, maskingKey: key)
        let contFrame   = WebSocketFrameHeader(fin: false, opcode: WSOpcode.continuation, payloadLength: 0, maskingKey: key)

        #expect(textFin.bytes[0]     == 0x81)  // FIN | 0x01
        #expect(binaryNoFin.bytes[0] == 0x02)  // no FIN | 0x02
        #expect(closeFrame.bytes[0]  == 0x88)  // FIN | 0x08
        #expect(pingFrame.bytes[0]   == 0x89)  // FIN | 0x09
        #expect(contFrame.bytes[0]   == 0x00)  // no FIN | 0x00
    }
}
