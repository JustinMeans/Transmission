import Foundation

/// A remote call that reached the peer and failed there.
///
/// `message` is deliberately conservative: unless the thrown error is `Codable`
/// (an explicit statement that its contents may cross the wire) only its type
/// name is sent, so database errors and the like do not leak to remote callers.
public struct RemoteCallError: Error, Sendable, CustomStringConvertible {
    public enum Code: UInt8, Sendable, Codable {
        /// Forward compatibility: a code this build does not know.
        case unknown = 0
        /// The distributed method threw.
        case thrown = 1
        /// No distributed target with that identifier. This is what version skew looks like.
        case targetNotFound = 2
        /// No actor with that id on the peer.
        case actorNotFound = 3
        /// Arguments could not be decoded.
        case badArguments = 4
        /// The result could not be encoded.
        case resultEncodingFailed = 5
        /// The connection's call queue is full. Safe to retry.
        case overloaded = 6
        /// The reply exceeds the configured frame limit.
        case frameTooLarge = 7
    }

    public let callID: CallID
    public let code: Code
    /// Type name of the thrown error, when `code == .thrown`.
    public let errorType: String?
    public let message: String
    /// JSON of the thrown error when it was `Codable`.
    public let payload: Data?

    public init(callID: CallID, code: Code, errorType: String? = nil, message: String, payload: Data? = nil) {
        self.callID = callID
        self.code = code
        self.errorType = errorType
        self.message = message
        self.payload = payload
    }

    public var description: String {
        "RemoteCallError(\(code), \(errorType ?? "-")): \(message)"
    }
}
