import Foundation
import NIO
import NIOHTTP1
import NIOWebSocket
import Logging

#if canImport(Network)
import NIOTransportServices
#else
import NIOPosix
#endif

public actor ClientManager {
    private let system: TransmissionSystem
    private var connection: ResilientConnection?
    private var statusHandler: (@Sendable (ConnectionStatus) async -> Void)?

    init(system: TransmissionSystem) {
        self.system = system
    }

    public func connect(to address: ServerAddress, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) {
        self.statusHandler = onStatus

        // Capture system reference for use in nonisolated context
        let systemRef = system

        connection = ResilientConnection(
            backoff: .standard,
            onStatusChange: onStatus
        ) {
            try await ClientConnectionHandler.establishConnection(to: address, system: systemRef)
        }

        Task {
            await connection?.start()
        }
    }

    public func disconnect() async {
        await connection?.stop()
        connection = nil
    }

    public var status: ConnectionStatus {
        get async {
            await connection?.currentStatus ?? .disconnected
        }
    }
}

/// Nonisolated connection handler to avoid actor isolation issues with NIO bootstrap.
private enum ClientConnectionHandler {
    static func establishConnection(to address: ServerAddress, system: TransmissionSystem) async throws {
        let bootstrap = createBootstrap()

        let upgrader = NIOTypedWebSocketClientUpgrader<UpgradeResult>(
            upgradePipelineHandler: { channel, _ in
                channel.eventLoop.makeCompletedFuture {
                    let asyncChannel = try NIOAsyncChannel<WebSocketFrame, WebSocketFrame>(
                        wrappingChannelSynchronously: channel
                    )
                    return UpgradeResult.websocket(asyncChannel)
                }
            }
        )

        var headers = HTTPHeaders()
        headers.add(name: "Host", value: address.host)
        headers.add(name: "X-Node-ID", value: system.nodeID.id)

        let requestHead = HTTPRequestHead(
            version: .http1_1,
            method: .GET,
            uri: address.path,
            headers: headers
        )

        let config = NIOTypedHTTPClientUpgradeConfiguration(
            upgradeRequestHead: requestHead,
            upgraders: [upgrader],
            notUpgradingCompletionHandler: { channel in
                channel.eventLoop.makeCompletedFuture {
                    UpgradeResult.notUpgraded
                }
            }
        )

        let upgradeResult = try await bootstrap.connect(host: address.host, port: address.port) { channel in
            channel.eventLoop.makeCompletedFuture {
                try channel.pipeline.syncOperations.configureUpgradableHTTPClientPipeline(
                    configuration: .init(upgradeConfiguration: config)
                )
            }
        }

        // Wait for the negotiation handler to complete and get the upgrade result
        let result = try await upgradeResult.get()

        switch result {
        case .websocket(let wsChannel):
            try await handleWebSocket(wsChannel, system: system)
        case .notUpgraded:
            throw TransmissionError.connectionFailed("WebSocket upgrade failed")
        }
    }

    static func handleWebSocket(
        _ channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        system: TransmissionSystem
    ) async throws {
        try await channel.executeThenClose { inbound, outbound in
            let node = RemoteNode(
                nodeID: .server,
                channel: channel,
                inbound: inbound,
                outbound: outbound
            )

            await system.nodes.register(node)
            defer {
                Task { await system.nodes.unregister(.server) }
            }

            try await processFrames(
                inbound: inbound,
                outbound: outbound,
                system: system,
                node: node
            )
        }
    }

    static func processFrames(
        inbound: NIOAsyncChannelInboundStream<WebSocketFrame>,
        outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>,
        system: TransmissionSystem,
        node: RemoteNode
    ) async throws {
        var buffer = Data()

        for try await frame in inbound {
            switch frame.opcode {
            case .text, .binary:
                var data = frame.data
                if let bytes = data.readBytes(length: data.readableBytes) {
                    buffer.append(contentsOf: bytes)
                }
                if frame.fin {
                    await system.decodeAndDeliver(data: buffer, from: node)
                    buffer = Data()
                }
            case .ping:
                let pong = WebSocketFrame(fin: true, opcode: .pong, data: frame.data)
                try await outbound.write(pong)
            case .pong:
                break
            case .connectionClose:
                return
            default:
                break
            }
        }
    }

    #if canImport(Network)
    static func createBootstrap() -> NIOTSConnectionBootstrap {
        NIOTSConnectionBootstrap(group: NIOSingletons.posixEventLoopGroup)
    }
    #else
    static func createBootstrap() -> ClientBootstrap {
        ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
    }
    #endif
}

private enum UpgradeResult: Sendable {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>)
    case notUpgraded
}

extension TransmissionSystem {
    public func connect(to address: ServerAddress, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        let client = ClientManager(system: self)
        await client.connect(to: address, onStatus: onStatus)
    }

    public func connect(to url: String, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        guard let address = ServerAddress(url: url) else {
            throw TransmissionError.invalidURL(url)
        }
        try await connect(to: address, onStatus: onStatus)
    }
}
