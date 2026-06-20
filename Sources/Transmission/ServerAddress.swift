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
        self.path = Self.normalizedPath(path)
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

    /// Normalizes a request path so it is always a valid origin-form URI path.
    ///
    /// A path that is empty, or that does not begin with `/`, is invalid as the
    /// path component of a URL that carries an authority (host). `URLComponents`
    /// returns `nil` for its `url` in that case, which previously caused the
    /// force-unwrap in `url` to trap and crash the process. Callers may pass an
    /// authority-relative path (e.g. `"ws"`); normalize it to `"/ws"` so the
    /// resulting `ServerAddress` always produces a usable URL and a well-formed
    /// `description`.
    static func normalizedPath(_ path: String) -> String {
        if path.isEmpty { return "/transmission" }
        if path.hasPrefix("/") { return path }
        return "/" + path
    }

    public var url: URL {
        var components = URLComponents()
        components.scheme = scheme.rawValue
        components.host = host
        components.port = port
        components.path = path
        // `path` is normalized to a leading-slash origin-form path in `init`, so
        // `components.url` is non-nil here. Fall back defensively rather than
        // force-unwrapping: a trap in `url` would crash the whole process on an
        // otherwise recoverable input.
        if let url = components.url {
            return url
        }
        return URL(string: "\(scheme.rawValue)://\(host):\(port)\(path)")
            ?? URL(string: "\(scheme.rawValue)://\(host):\(port)/transmission")!
    }

    public var description: String {
        "\(scheme.rawValue)://\(host):\(port)\(path)"
    }
}
