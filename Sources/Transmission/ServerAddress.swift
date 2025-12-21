import Foundation

/// Represents a server address for Transmission connections.
public struct ServerAddress: Sendable, Hashable, CustomStringConvertible {
    public enum Scheme: String, Sendable {
        case insecure = "ws"
        case secure = "wss"
    }

    public let scheme: Scheme
    public let host: String
    public let port: Int
    public let path: String

    public init(scheme: Scheme = .secure, host: String, port: Int, path: String = "/transmission") {
        self.scheme = scheme
        self.host = host
        self.port = port
        self.path = path
    }

    /// Creates an address from a URL string.
    public init?(url: String) {
        guard let parsed = URL(string: url) else { return nil }

        guard let scheme = parsed.scheme.flatMap(Scheme.init(rawValue:)) else {
            return nil
        }

        guard let host = parsed.host, !host.isEmpty else { return nil }

        let port = parsed.port ?? (scheme == .secure ? 443 : 80)
        let path = parsed.path.isEmpty ? "/transmission" : parsed.path

        self.init(scheme: scheme, host: host, port: port, path: path)
    }

    public var url: URL {
        var components = URLComponents()
        components.scheme = scheme.rawValue
        components.host = host
        components.port = port
        components.path = path
        return components.url!
    }

    public var description: String {
        "\(scheme.rawValue)://\(host):\(port)\(path)"
    }
}
