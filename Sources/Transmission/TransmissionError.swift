import Foundation

/// Errors that can occur within the Transmission system.
public enum TransmissionError: Error, Sendable, LocalizedError {
    case noConnection
    case connectionFailed(String)
    case connectionTimeout
    case actorNotFound(ActorIdentity)
    case noNodeForActor(ActorIdentity)
    case typeMismatch(expected: String, got: String)
    case encodingFailed(String)
    case decodingFailed(String)
    case callTimeout(CallID)
    case callFailed(CallID, String)
    case invalidURL(String)
    case authenticationRequired
    case authenticationFailed(String)

    public var errorDescription: String? {
        switch self {
        case .noConnection:
            return "No connection to server"
        case .connectionFailed(let reason):
            return "Connection failed: \(reason)"
        case .connectionTimeout:
            return "Connection timed out"
        case .actorNotFound(let id):
            return "Actor not found: \(id)"
        case .noNodeForActor(let id):
            return "No node specified for actor: \(id)"
        case .typeMismatch(let expected, let got):
            return "Type mismatch: expected \(expected), got \(got)"
        case .encodingFailed(let reason):
            return "Encoding failed: \(reason)"
        case .decodingFailed(let reason):
            return "Decoding failed: \(reason)"
        case .callTimeout(let id):
            return "Call timed out: \(id)"
        case .callFailed(let id, let reason):
            return "Call \(id) failed: \(reason)"
        case .invalidURL(let url):
            return "Invalid URL: \(url)"
        case .authenticationRequired:
            return "Authentication required"
        case .authenticationFailed(let reason):
            return "Authentication failed: \(reason)"
        }
    }
}
