import NIOCore
import NIOHTTP1
import NIOWebSocket
import TransmissionLive
import Vapor

@_exported import TransmissionLive

/// Resolves the identity for an upgrade. Return nil or throw to refuse it with
/// a 401, before any WebSocket exists.
public typealias LiveAuthenticator = @Sendable (Request, _ bearerToken: String?) async throws -> LiveIdentity?

extension Application {
    public var live: LiveProvider { LiveProvider(application: self) }

    public struct LiveProvider: Sendable {
        let application: Application

        /// Mounts the live route, `/v3/live` by default.
        public func mount(
            hub: LiveHub,
            path: [PathComponent] = ["v3", "live"],
            maxInboundFrame: Int = 1 << 20,
            authenticate: @escaping LiveAuthenticator
        ) {
            application.on(.GET, path) { request async throws -> Response in
                let offered = request.headers[.secWebSocketProtocol]
                    .flatMap { $0.split(separator: ",") }
                    .map { $0.trimmingCharacters(in: .whitespaces) }
                guard offered.contains(LiveProtocol.subprotocol) else {
                    throw Abort(.badRequest, reason: "Offer subprotocol \(LiveProtocol.subprotocol)")
                }
                let token = request.headers.bearerAuthorization?.token
                let identity: LiveIdentity?
                do {
                    identity = try await authenticate(request, token)
                } catch {
                    throw Abort(.unauthorized)
                }
                guard let identity else {
                    throw Abort(.unauthorized)
                }
                let response = Response(status: .switchingProtocols)
                response.upgrader = LiveUpgrader(hub: hub, identity: identity, maxFrameSize: maxInboundFrame)
                return response
            }
        }
    }
}

/// Stands in for Vapor's `WebSocketUpgrader` because `WebSocket` keeps its
/// channel private, and only the channel can drop a peer that has stopped
/// reading. `ws.close` queues a Close frame behind everything the peer isn't
/// reading, then waits for an answer that never comes.
private struct LiveUpgrader: Upgrader {
    let hub: LiveHub
    let identity: LiveIdentity
    let maxFrameSize: Int

    func applyUpgrade(req: Request, res: Response) -> any HTTPServerProtocolUpgrader {
        let hub = self.hub
        let identity = self.identity
        return NIOWebSocketServerUpgrader(
            maxFrameSize: maxFrameSize,
            automaticErrorHandling: false,
            shouldUpgrade: { channel, _ in
                var headers = HTTPHeaders()
                headers.add(name: .secWebSocketProtocol, value: LiveProtocol.subprotocol)
                return channel.eventLoop.makeSucceededFuture(headers)
            },
            upgradePipelineHandler: { channel, _ in
                WebSocket.server(on: channel) { socket in
                    LiveSocketBridge.attach(socket: socket, channel: channel, hub: hub, identity: identity)
                }
            }
        )
    }
}

private enum LiveSocketBridge {
    /// Runs on the event loop, so no frame can arrive before the callbacks exist.
    /// These are websocket-kit's synchronous callbacks, which preserve order.
    static func attach(socket: WebSocket, channel: any Channel, hub: LiveHub, identity: LiveIdentity) {
        let connection = LiveConnection(
            hub: hub,
            transport: VaporLiveTransport(socket: socket, channel: channel),
            identity: identity)
        let onBinary: @Sendable (WebSocket, ByteBuffer) -> Void = { _, buffer in
            connection.receive(Array(buffer.readableBytesView))
        }
        socket.onBinary(onBinary)
        socket.onClose.whenComplete { _ in
            connection.transportClosed()
        }
        Task {
            await connection.start()
            if socket.isClosed {
                connection.transportClosed()
            }
        }
    }
}

private struct VaporLiveTransport: LiveServerTransport {
    let socket: WebSocket
    let channel: any Channel

    func write(_ frame: LiveWireFrame) async throws {
        try await socket.send(raw: frame.assembled, opcode: .binary)
    }

    func close(code: LiveCloseCode) async {
        let promise = socket.eventLoop.makePromise(of: Void.self)
        socket.close(code: .init(codeNumber: Int(code.rawValue)), promise: promise)
        // A peer that reads gets the code. One that doesn't is dropped regardless.
        let channel = self.channel
        channel.eventLoop.scheduleTask(in: .seconds(2)) {
            channel.close(mode: .all, promise: nil)
        }
        try? await promise.futureResult.get()
    }
}
