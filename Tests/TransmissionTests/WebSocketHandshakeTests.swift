/// WebSocketHandshakeTests.swift
///
/// Tests for RFC 6455 §4.2.2 WebSocket opening-handshake helpers.

import Foundation
import Testing
@testable import Transmission

@Suite("WebSocketHandshake")
struct WebSocketHandshakeTests {

    // MARK: - Accept value: RFC 6455 canonical vector

    /// The example from RFC 6455 §4.2.2 step 5.4:
    ///   key    = "dGhlIHNhbXBsZSBub25jZQ=="
    ///   accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    @Test("RFC 6455 canonical key produces canonical accept value")
    func canonicalRFCVector() {
        let key    = "dGhlIHNhbXBsZSBub25jZQ=="
        let accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        #expect(webSocketAcceptValue(for: key) == accept)
    }

    // MARK: - Accept value: second computed vector

    /// Cross-checked against an independent SHA-1 + base64 computation:
    ///   key    = "x3JJHMbDL1EzLkh9GBhXDw=="
    ///   input  = "x3JJHMbDL1EzLkh9GBhXDw==258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    ///   SHA-1  = HSmrc0sMlYUkAGmm5OPpG2HaGWk=  (20 bytes)
    @Test("second computed key produces correct accept value")
    func secondComputedVector() {
        let key    = "x3JJHMbDL1EzLkh9GBhXDw=="
        let accept = "HSmrc0sMlYUkAGmm5OPpG2HaGWk="
        #expect(webSocketAcceptValue(for: key) == accept)
    }

    // MARK: - Accept value: base64 / length sanity

    @Test("accept value is a valid base64 string")
    func acceptValueIsValidBase64() {
        let key    = "dGhlIHNhbXBsZSBub25jZQ=="
        let accept = webSocketAcceptValue(for: key)
        // SHA-1 produces 20 bytes; base64 of 20 bytes = 28 characters (with padding).
        #expect(accept.count == 28)
        // Must be decodable as base64.
        #expect(Data(base64Encoded: accept) != nil)
    }

    @Test("accept value decodes to 20 bytes (SHA-1 digest length)")
    func acceptValueIs20Bytes() {
        let key    = "dGhlIHNhbXBsZSBub25jZQ=="
        let accept = webSocketAcceptValue(for: key)
        let decoded = Data(base64Encoded: accept)
        #expect(decoded?.count == 20)
    }

    // MARK: - Header validation: happy path

    @Test("valid headers return the Sec-WebSocket-Key")
    func validHeadersReturnKey() throws {
        let headers = [
            "Upgrade":                "websocket",
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "13",
            "Sec-WebSocket-Key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        let key = try validateWebSocketHandshakeHeaders(headers)
        #expect(key == "dGhlIHNhbXBsZSBub25jZQ==")
    }

    @Test("header names are matched case-insensitively")
    func headerNamesAreCaseInsensitive() throws {
        let headers = [
            "upgrade":                "WEBSOCKET",
            "connection":             "keep-alive, Upgrade",
            "sec-websocket-version":  "13",
            "sec-websocket-key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        let key = try validateWebSocketHandshakeHeaders(headers)
        #expect(key == "dGhlIHNhbXBsZSBub25jZQ==")
    }

    // MARK: - Header validation: missing / invalid Upgrade

    @Test("missing Upgrade header throws missingUpgradeHeader")
    func missingUpgradeHeader() {
        let headers = [
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "13",
            "Sec-WebSocket-Key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        #expect(throws: WebSocketHandshakeError.missingUpgradeHeader) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    @Test("Upgrade value not containing websocket throws missingUpgradeHeader")
    func wrongUpgradeValue() {
        let headers = [
            "Upgrade":                "h2c",
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "13",
            "Sec-WebSocket-Key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        #expect(throws: WebSocketHandshakeError.missingUpgradeHeader) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    // MARK: - Header validation: missing Connection

    @Test("missing Connection header throws missingConnectionHeader")
    func missingConnectionHeader() {
        let headers = [
            "Upgrade":                "websocket",
            "Sec-WebSocket-Version":  "13",
            "Sec-WebSocket-Key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        #expect(throws: WebSocketHandshakeError.missingConnectionHeader) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    // MARK: - Header validation: version

    @Test("missing version header throws unsupportedVersion(nil)")
    func missingVersionHeader() {
        let headers = [
            "Upgrade":            "websocket",
            "Connection":         "Upgrade",
            "Sec-WebSocket-Key":  "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        #expect(throws: WebSocketHandshakeError.unsupportedVersion(nil)) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    @Test("version 8 throws unsupportedVersion(\"8\")")
    func wrongVersionThrows() {
        let headers = [
            "Upgrade":                "websocket",
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "8",
            "Sec-WebSocket-Key":      "dGhlIHNhbXBsZSBub25jZQ==",
        ]
        #expect(throws: WebSocketHandshakeError.unsupportedVersion("8")) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    // MARK: - Header validation: missing / invalid key

    @Test("missing Sec-WebSocket-Key throws missingKey")
    func missingKeyHeader() {
        let headers = [
            "Upgrade":                "websocket",
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "13",
        ]
        #expect(throws: WebSocketHandshakeError.missingKey) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }

    @Test("key that does not decode to 16 bytes throws invalidKeyLength")
    func shortKeyThrows() {
        // base64("hello") = "aGVsbG8=" — decodes to 5 bytes, not 16.
        let headers = [
            "Upgrade":                "websocket",
            "Connection":             "Upgrade",
            "Sec-WebSocket-Version":  "13",
            "Sec-WebSocket-Key":      "aGVsbG8=",
        ]
        #expect(throws: WebSocketHandshakeError.invalidKeyLength) {
            try validateWebSocketHandshakeHeaders(headers)
        }
    }
}
