import Distributed
import Vapor
import Transmission
import NIO
import NIOWebSocket

extension Application {
    public var transmission: TransmissionProvider {
        .init(application: self)
    }

    public struct TransmissionProvider: Sendable {
        let application: Application

        private struct StorageKey: Vapor.StorageKey {
            typealias Value = TransmissionSystem
        }

        public var system: TransmissionSystem? {
            get { application.storage[StorageKey.self] }
            nonmutating set { application.storage[StorageKey.self] = newValue }
        }

        public func register(_ system: TransmissionSystem) throws {
            self.system = system
        }

        /// Mounts the Transmission route.
        ///
        /// The route is open unless `middleware` says otherwise: pass auth
        /// middleware, which runs before the upgrade. `onConnect` runs once per
        /// connection before its first call is dispatched, so it can copy the
        /// authenticated user onto the node for handlers to read through
        /// `RemoteNode.current`. If it throws, the socket closes.
        public func configure(
            path: String = "transmission",
            middleware: [any Middleware] = [],
            onConnect: (@Sendable (Request, RemoteNode) async throws -> Void)? = nil
        ) throws {
            guard let system else {
                throw Abort(.internalServerError, reason: "TransmissionSystem not registered")
            }

            let serverNodeID = system.nodeID.id
            let routes = application.grouped(middleware)
            let upgraded: @Sendable (Request, WebSocket) -> Void = { req, ws in
                VaporWebSocketBridge.accept(req: req, ws: ws, system: system, onConnect: onConnect)
            }
            routes.webSocket(
                PathComponent(stringLiteral: path),
                maxFrameSize: WebSocketMaxFrameSize(integerLiteral: system.maxFrameSize),
                shouldUpgrade: { req in
                    var headers = HTTPHeaders()
                    headers.add(name: "X-Server-Node-ID", value: serverNodeID)
                    return req.eventLoop.makeSucceededFuture(headers)
                },
                onUpgrade: upgraded
            )
        }
    }
}

/// Handles WebSocket connections in Vapor for the Transmission protocol.
public actor VaporWebSocketBridge {
    private let ws: WebSocket
    private let nodeID: NodeIdentity
    private let system: TransmissionSystem
    private let remoteNode: RemoteNode
    private let dispatcher: InboundDispatcher
    private var isClosed = false

    init(ws: WebSocket, nodeID: NodeIdentity, system: TransmissionSystem) {
        self.ws = ws
        self.nodeID = nodeID
        self.system = system

        // Create a RemoteNode that sends via Vapor's WebSocket
        let node = RemoteNode(
            nodeID: nodeID,
            send: { [ws] data in
                try await ws.send(raw: data, opcode: .binary)
            },
            close: { [ws] in
                try? await ws.close(code: .normalClosure)
            },
            maxFrameSize: system.maxFrameSize
        )
        self.remoteNode = node
        self.dispatcher = InboundDispatcher(system: system, node: node, limits: system.dispatchLimits)
    }

    /// Handles an incoming WebSocket connection.
    public static func handleWebSocket(req: Request, ws: WebSocket, system: TransmissionSystem) async {
        let bridge = makeBridge(req: req, ws: ws, system: system)
        await withCheckedContinuation { (attached: CheckedContinuation<Void, Never>) in
            ws.eventLoop.execute {
                bridge.attach()
                attached.resume()
            }
        }
        await bridge.run(req: req, onConnect: nil)
    }

    /// Wires up a connection on the event loop, before any frame can be lost.
    static func accept(
        req: Request,
        ws: WebSocket,
        system: TransmissionSystem,
        onConnect: (@Sendable (Request, RemoteNode) async throws -> Void)?
    ) {
        let bridge = makeBridge(req: req, ws: ws, system: system)
        bridge.attach()
        Task {
            await bridge.run(req: req, onConnect: onConnect)
        }
    }

    private static func makeBridge(req: Request, ws: WebSocket, system: TransmissionSystem) -> VaporWebSocketBridge {
        let nodeID = req.headers.first(name: "X-Node-ID").map { NodeIdentity(id: $0) } ?? NodeIdentity.random()
        return VaporWebSocketBridge(ws: ws, nodeID: nodeID, system: system)
    }

    /// Uses websocket-kit's synchronous callbacks. Its `async` overloads spawn a
    /// `Task` per frame, which is what reordered calls (audit V4). This must run
    /// on the WebSocket's event loop.
    private nonisolated func attach() {
        let dispatcher = self.dispatcher
        let onBinary: @Sendable (WebSocket, ByteBuffer) -> Void = { _, buffer in
            if let bytes = buffer.getBytes(at: buffer.readerIndex, length: buffer.readableBytes) {
                dispatcher.receive(Data(bytes))
            }
        }
        let onText: @Sendable (WebSocket, String) -> Void = { _, text in
            dispatcher.receive(Data(text.utf8))
        }
        ws.onBinary(onBinary)
        ws.onText(onText)
        if let heartbeat = system.serverHeartbeat {
            let (seconds, attoseconds) = heartbeat.interval.components
            ws.pingInterval = .nanoseconds(seconds * 1_000_000_000 + attoseconds / 1_000_000_000)
        }
    }

    func run(req: Request, onConnect: (@Sendable (Request, RemoteNode) async throws -> Void)?) async {
        let bridge = self

        // Register this client's node so the server can send calls to it
        await system.adopt(remoteNode)
        system.metrics.connectionOpened()

        ws.onClose.whenComplete { [bridge] _ in
            Task {
                await bridge.handleClose()
            }
        }

        do {
            try await onConnect?(req, remoteNode)
            dispatcher.start()
        } catch {
            system.logger.error("Connection rejected: \(error)")
            await handleClose()
        }

        try? await ws.onClose.get()
    }

    private func handleClose() async {
        guard !isClosed else { return }
        isClosed = true
        dispatcher.shutdown()
        system.metrics.connectionClosed()
        await system.nodeDidDisconnect(remoteNode)
        // Last: this waits for the peer's Close, which a dead peer never sends.
        try? await ws.close(code: .normalClosure)
    }

    /// Sends a wire envelope through the WebSocket.
    public func send(_ envelope: WireEnvelope) async throws {
        guard !isClosed else { throw TransmissionError.noConnection }

        let encoder = JSONEncoder()
        let data = try encoder.encode(envelope)
        try await ws.send(raw: data, opcode: .text)
    }
}
