import Foundation
import NIO
import NIOHTTP1
import NIOWebSocket
import NIOPosix
import Logging

public actor ServerManager {
    private let system: TransmissionSystem
    private var isRunning = false

    init(system: TransmissionSystem) {
        self.system = system
    }

    public func start(at address: ServerAddress) async throws {
        guard !isRunning else { return }
        isRunning = true

        let upgrader = NIOTypedWebSocketServerUpgrader<UpgradeResult>(
            shouldUpgrade: { channel, head in
                channel.eventLoop.makeSucceededFuture(HTTPHeaders())
            },
            upgradePipelineHandler: { channel, _ in
                channel.eventLoop.makeCompletedFuture {
                    let asyncChannel = try NIOAsyncChannel<WebSocketFrame, WebSocketFrame>(
                        wrappingChannelSynchronously: channel
                    )
                    return UpgradeResult.websocket(asyncChannel)
                }
            }
        )

        let serverChannel = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .bind(host: address.host, port: address.port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let upgradeConfig = NIOTypedHTTPServerUpgradeConfiguration(
                        upgraders: [upgrader],
                        notUpgradingCompletionHandler: { channel in
                            channel.eventLoop.makeCompletedFuture {
                                try channel.pipeline.syncOperations.addHandler(HTTPNotFoundHandler())
                                return UpgradeResult.notUpgraded
                            }
                        }
                    )
                    let config = NIOUpgradableHTTPServerPipelineConfiguration(upgradeConfiguration: upgradeConfig)
                    return try channel.pipeline.syncOperations.configureUpgradableHTTPServerPipeline(configuration: config)
                }
            }

        system.logger.info("Server listening on \(address)")

        // Capture system reference for use in task group
        let systemRef = system

        try await withThrowingDiscardingTaskGroup { group in
            try await serverChannel.executeThenClose { inbound in
                for try await connectionResult in inbound {
                    group.addTask {
                        do {
                            let result = try await connectionResult.get()
                            if case .websocket(let wsChannel) = result {
                                try await ServerConnectionHandler.handleWebSocket(
                                    channel: wsChannel,
                                    system: systemRef
                                )
                            }
                        } catch {
                            systemRef.logger.debug("Connection error: \(error)")
                        }
                    }
                }
            }
        }
    }

    public func stop() async {
        isRunning = false
    }
}

/// Nonisolated connection handler to avoid actor isolation in task group closures.
private enum ServerConnectionHandler {
    static func handleWebSocket(
        channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        system: TransmissionSystem
    ) async throws {
        try await channel.executeThenClose { inbound, outbound in
            let clientID = NodeIdentity.random()
            let node = RemoteNode(
                nodeID: clientID,
                channel: channel,
                inbound: inbound,
                outbound: outbound
            )

            await system.nodes.register(node)
            system.metrics.connectionOpened()

            defer {
                Task {
                    await system.nodes.unregister(clientID)
                    system.metrics.connectionClosed()
                }
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
                    system.metrics.recordMessageReceived(bytes: bytes.count)
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
}

private enum UpgradeResult: Sendable {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>)
    case notUpgraded
}

private final class HTTPNotFoundHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let part = unwrapInboundIn(data)
        guard case .head = part else { return }

        let head = HTTPResponseHead(version: .http1_1, status: .notFound)
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
    }
}

extension TransmissionSystem {
    public func runServer(at address: ServerAddress) async throws {
        let server = ServerManager(system: self)
        try await server.start(at: address)
    }

    public func runServer(host: String = "0.0.0.0", port: Int = 8080) async throws {
        let address = ServerAddress(scheme: .insecure, host: host, port: port)
        try await runServer(at: address)
    }
}
