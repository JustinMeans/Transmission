import Foundation
import CommonCrypto

/// WebSocketHandshake.swift
///
/// RFC 6455 §4.2.2 — WebSocket Opening Handshake (Server Side)
///
/// During the WebSocket upgrade handshake the server must prove it received the
/// client's `Sec-WebSocket-Key` header by computing:
///
///     accept = base64( SHA1( key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" ) )
///
/// and returning that value in the `Sec-WebSocket-Accept` response header.
///
/// This file provides:
///   - `webSocketAcceptValue(for:)` — computes the accept token from a raw key string.
///   - `WebSocketHandshakeError` — typed errors for header-validation failures.
///   - `validateWebSocketHandshakeHeaders(_:)` — validates that an HTTP upgrade
///     request carries the required headers (`Upgrade`, `Connection`,
///     `Sec-WebSocket-Version: 13`, `Sec-WebSocket-Key`) and returns the key.

// MARK: - RFC 6455 GUID

/// The fixed GUID appended to the client key before hashing, as specified by
/// RFC 6455 §4.2.2 step 5.4.
private let webSocketKeyGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// MARK: - Accept value computation

/// Computes the `Sec-WebSocket-Accept` token for a given `Sec-WebSocket-Key`.
///
/// The algorithm (RFC 6455 §4.2.2 step 5.4):
/// 1. Concatenate `key` with the fixed GUID `"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"`.
/// 2. Compute SHA-1 of the UTF-8 encoding of that concatenated string.
/// 3. Base64-encode the 20-byte digest.
///
/// - Parameter key: The raw value of the client's `Sec-WebSocket-Key` header,
///   without any surrounding whitespace.
/// - Returns: The base64-encoded SHA-1 digest to use as `Sec-WebSocket-Accept`.
public func webSocketAcceptValue(for key: String) -> String {
    let input = key + webSocketKeyGUID
    let inputBytes = Array(input.utf8)

    var digest = [UInt8](repeating: 0, count: Int(CC_SHA1_DIGEST_LENGTH))
    CC_SHA1(inputBytes, CC_LONG(inputBytes.count), &digest)

    return Data(digest).base64EncodedString()
}

// MARK: - Handshake header validation

/// Errors produced when a WebSocket upgrade request fails header validation.
public enum WebSocketHandshakeError: Error, Equatable, Sendable {
    /// The `Upgrade` header is missing or does not contain `"websocket"`.
    case missingUpgradeHeader
    /// The `Connection` header is missing or does not contain `"Upgrade"`.
    case missingConnectionHeader
    /// The `Sec-WebSocket-Version` header is absent or specifies a version other
    /// than `13` (the only version defined by RFC 6455).
    case unsupportedVersion(String?)
    /// The `Sec-WebSocket-Key` header is absent.
    case missingKey
    /// The `Sec-WebSocket-Key` value does not decode to a 16-byte sequence when
    /// base64-decoded (RFC 6455 §4.2.1 requires a 16-byte nonce).
    case invalidKeyLength
}

/// Validates the HTTP upgrade headers for a WebSocket handshake request and
/// returns the value of the `Sec-WebSocket-Key` header if all checks pass.
///
/// Validation performed (RFC 6455 §4.2.1):
/// 1. `Upgrade` header must be present and contain `"websocket"` (case-insensitive).
/// 2. `Connection` header must be present and contain `"Upgrade"` (case-insensitive).
/// 3. `Sec-WebSocket-Version` header must be present and equal `"13"`.
/// 4. `Sec-WebSocket-Key` header must be present and, when base64-decoded, must
///    produce exactly 16 bytes (a 16-byte random nonce, per §4.2.1 point 4).
///
/// - Parameter headers: A dictionary of HTTP header field names to values.
///   Header names are matched case-insensitively.
/// - Returns: The trimmed `Sec-WebSocket-Key` string on success.
/// - Throws: `WebSocketHandshakeError` describing the first validation failure.
public func validateWebSocketHandshakeHeaders(
    _ headers: [String: String]
) throws -> String {
    // Build a case-insensitive lookup over the provided headers.
    let normalized = Dictionary(
        uniqueKeysWithValues: headers.map { ($0.key.lowercased(), $0.value) }
    )

    // 1. Upgrade: websocket
    guard let upgrade = normalized["upgrade"],
          upgrade.lowercased().contains("websocket") else {
        throw WebSocketHandshakeError.missingUpgradeHeader
    }

    // 2. Connection: Upgrade
    guard let connection = normalized["connection"],
          connection.lowercased().contains("upgrade") else {
        throw WebSocketHandshakeError.missingConnectionHeader
    }

    // 3. Sec-WebSocket-Version: 13
    let version = normalized["sec-websocket-version"]
    guard version?.trimmingCharacters(in: .whitespaces) == "13" else {
        throw WebSocketHandshakeError.unsupportedVersion(version)
    }

    // 4. Sec-WebSocket-Key must be present and decode to exactly 16 bytes.
    guard let rawKey = normalized["sec-websocket-key"] else {
        throw WebSocketHandshakeError.missingKey
    }
    let key = rawKey.trimmingCharacters(in: .whitespaces)
    guard let decoded = Data(base64Encoded: key), decoded.count == 16 else {
        throw WebSocketHandshakeError.invalidKeyLength
    }

    return key
}
