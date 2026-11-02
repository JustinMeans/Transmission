import Foundation

/// Controls how a client retries after a session ends, for any reason.
public struct ReconnectPolicy: Sendable {
    /// Delay curve applied between attempts.
    public var backoff: ExponentialBackoff
    /// A session must stay up this long before it counts as healthy and resets
    /// the backoff. Without this, a server that accepts then closes would reset
    /// the curve on every cycle and clients would hammer it.
    public var minimumSessionLifetime: Duration
    /// Lower bound in seconds on every automatic retry, including the first
    /// retry after a healthy session. Jittered by +/-50% so a server restart
    /// does not reconnect the whole fleet in the same instant.
    public var minimumDelay: Double

    public init(
        backoff: ExponentialBackoff = .standard,
        minimumSessionLifetime: Duration = .seconds(10),
        minimumDelay: Double = 0.25
    ) {
        self.backoff = backoff
        self.minimumSessionLifetime = minimumSessionLifetime
        self.minimumDelay = minimumDelay
    }

    public static let standard = ReconnectPolicy()
}

/// WebSocket ping/pong keepalive. A peer that sends nothing at all for
/// `timeout` is treated as dead and its socket is closed.
public struct HeartbeatPolicy: Sendable, Equatable {
    public var interval: Duration
    public var timeout: Duration

    public init(interval: Duration = .seconds(25), timeout: Duration = .seconds(60)) {
        self.interval = interval
        self.timeout = timeout
    }

    public static let standard = HeartbeatPolicy()
}

/// Options for `TransmissionSystem.connect(to:options:onStatus:)`.
public struct TransmissionClientOptions: Sendable {
    /// Static headers added to every upgrade request.
    public var headers: [String: String]
    /// Evaluated before every connection attempt, so a reconnect always carries
    /// a fresh token. Wins over `headers` on a name clash. A throw counts as a
    /// failed attempt and goes through normal backoff.
    public var headerProvider: (@Sendable () async throws -> [String: String])?
    public var reconnect: ReconnectPolicy
    /// `nil` disables keepalive.
    public var heartbeat: HeartbeatPolicy?

    public init(
        headers: [String: String] = [:],
        headerProvider: (@Sendable () async throws -> [String: String])? = nil,
        reconnect: ReconnectPolicy = .standard,
        heartbeat: HeartbeatPolicy? = .standard
    ) {
        self.headers = headers
        self.headerProvider = headerProvider
        self.reconnect = reconnect
        self.heartbeat = heartbeat
    }
}
