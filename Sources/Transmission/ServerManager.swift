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

        let serverNodeID = system.nodeID.id

        let upgrader = NIOTypedWebSocketServerUpgrader<UpgradeResult>(
            shouldUpgrade: { channel, head in
                let clientNodeID = head.headers.first(name: "X-Node-ID")
                var responseHeaders = HTTPHeaders()
                responseHeaders.add(name: "X-Server-Node-ID", value: serverNodeID)
                if let nodeID = clientNodeID {
                    responseHeaders.add(name: "X-Client-Node-ID", value: nodeID)
                }
                return channel.eventLoop.makeSucceededFuture(responseHeaders)
            },
            upgradePipelineHandler: { channel, upgradeResponse in
                channel.eventLoop.makeCompletedFuture {
                    let asyncChannel = try NIOAsyncChannel<WebSocketFrame, WebSocketFrame>(
                        wrappingChannelSynchronously: channel
                    )
                    let clientNodeID = upgradeResponse.headers.first(name: "X-Client-Node-ID")
                    return UpgradeResult.websocket(asyncChannel, clientNodeID: clientNodeID)
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

        let systemRef = system

        try await withThrowingDiscardingTaskGroup { group in
            try await serverChannel.executeThenClose { inbound in
                for try await connectionResult in inbound {
                    group.addTask {
                        do {
                            let result = try await connectionResult.get()
                            if case .websocket(let wsChannel, let clientNodeID) = result {
                                try await ServerConnectionHandler.handleWebSocket(
                                    channel: wsChannel,
                                    clientNodeID: clientNodeID,
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

private enum ServerConnectionHandler {
    static func handleWebSocket(
        channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        clientNodeID: String?,
        system: TransmissionSystem
    ) async throws {
        try await channel.executeThenClose { inbound, outbound in
            let nodeID: NodeIdentity
            if let clientID = clientNodeID {
                nodeID = NodeIdentity(id: clientID)
                system.logger.debug("Client connected with ID: \(clientID)")
            } else {
                nodeID = NodeIdentity.random()
                system.logger.debug("Client connected with random ID: \(nodeID.id)")
            }

            let node = RemoteNode(
                nodeID: nodeID,
                channel: channel,
                inbound: inbound,
                outbound: outbound
            )

            await system.nodes.register(node)
            system.metrics.connectionOpened()

            do {
                try await processFrames(
                    inbound: inbound,
                    outbound: outbound,
                    system: system,
                    node: node
                )
            } catch {
                await system.nodes.unregister(nodeID)
                system.metrics.connectionClosed()
                throw error
            }

            await system.nodes.unregister(nodeID)
            system.metrics.connectionClosed()
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
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>, clientNodeID: String?)
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
