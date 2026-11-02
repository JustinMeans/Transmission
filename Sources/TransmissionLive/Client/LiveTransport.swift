import Foundation

/// What a client needs to open a socket.
public struct LiveConnectRequest: Sendable {
    public var url: URL
    public var headers: [String: String]
    public var subprotocols: [String]
    public var maxMessageSize: Int

    public init(url: URL, headers: [String: String] = [:], subprotocols: [String], maxMessageSize: Int) {
        self.url = url
        self.headers = headers
        self.subprotocols = subprotocols
        self.maxMessageSize = maxMessageSize
    }
}

/// An open socket. `receive` throws `LiveTransportClosed` when it ends.
public protocol LiveTransportConnection: Sendable {
    func send(_ data: Data) async throws
    func receive() async throws -> Data
    func close(code: LiveCloseCode) async
}

/// Opens sockets. The seam that keeps `LiveClient` independent of any socket library.
public protocol LiveTransport: Sendable {
    func connect(_ request: LiveConnectRequest) async throws -> any LiveTransportConnection
}

public struct LiveTransportClosed: Error, Sendable, Equatable {
    public let code: LiveCloseCode?
    public init(code: LiveCloseCode?) { self.code = code }
}

public enum LiveTransportError: Error, Sendable, Equatable {
    /// The server refused the upgrade with this HTTP status.
    case rejected(status: Int)
    case connectFailed(String)
}
