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

        public func configure(
            path: String = "transmission",
            middleware: [any Middleware] = []
        ) throws {
            guard let system else {
                throw Abort(.internalServerError, reason: "TransmissionSystem not registered")
            }

            let routes = application.grouped(middleware)
            routes.webSocket(PathComponent(stringLiteral: path)) { req, ws in
                await VaporWebSocketBridge.handleWebSocket(req: req, ws: ws, system: system)
            }
        }
    }
}

/// Handles WebSocket connections in Vapor for the Transmission protocol.
public actor VaporWebSocketBridge {
    private let ws: WebSocket
    private let nodeID: NodeIdentity
    private let system: TransmissionSystem
    private var isClosed = false

    init(ws: WebSocket, nodeID: NodeIdentity, system: TransmissionSystem) {
        self.ws = ws
        self.nodeID = nodeID
        self.system = system
    }

    /// Handles an incoming WebSocket connection.
    public static func handleWebSocket(req: Request, ws: WebSocket, system: TransmissionSystem) async {
        let nodeID = req.headers.first(name: "X-Node-ID").map { NodeIdentity(id: $0) } ?? NodeIdentity.random()
        let bridge = VaporWebSocketBridge(ws: ws, nodeID: nodeID, system: system)
        await bridge.run()
    }

    func run() async {
        let bridge = self

        ws.onText { [bridge] _, text in
            await bridge.handleText(text)
        }

        ws.onBinary { [bridge] _, buffer in
            await bridge.handleBinary(buffer)
        }

        ws.onClose.whenComplete { [bridge] _ in
            Task {
                await bridge.handleClose()
            }
        }

        try? await ws.onClose.get()
    }

    private func handleText(_ text: String) async {
        guard let data = text.data(using: .utf8) else { return }
        await processMessage(data)
    }

    private func handleBinary(_ buffer: ByteBuffer) async {
        guard let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) else { return }
        let data = Data(bytes)
        await processMessage(data)
    }

    private func processMessage(_ data: Data) async {
        do {
            let decoder = JSONDecoder()
            decoder.userInfo[.transmissionSystem] = system

            let envelope = try decoder.decode(WireEnvelope.self, from: data)

            switch envelope {
            case .call(let call):
                await handleCall(call)
            case .reply(let reply):
                await system.pendingCalls.receive(reply: reply)
            case .close:
                await handleClose()
            }
        } catch {
            system.logger.error("Failed to decode message: \(error)")
        }
    }

    private func handleCall(_ call: CallEnvelope) async {
        // Capture ws for the sendReply closure
        let wsRef = ws

        await system.handleCall(call) { replyData in
            try await wsRef.send(raw: replyData, opcode: .text)
        }
    }

    private func handleClose() async {
        guard !isClosed else { return }
        isClosed = true
        try? await ws.close()
        await system.nodes.unregister(nodeID)
    }

    /// Sends a wire envelope through the WebSocket.
    public func send(_ envelope: WireEnvelope) async throws {
        guard !isClosed else { throw TransmissionError.noConnection }

        let encoder = JSONEncoder()
        let data = try encoder.encode(envelope)
        try await ws.send(raw: data, opcode: .text)
    }
}
