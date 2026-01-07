import Foundation
import NIO
import NIOHTTP1
import NIOWebSocket
import NIOPosix
import Logging

public struct RawWebSocketChannel: Sendable {
    public let inbound: NIOAsyncChannelInboundStream<WebSocketFrame>
    public let outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>

    public func send(_ data: Data) async throws {
        var buffer = ByteBuffer()
        buffer.writeBytes(data)
        try await outbound.write(WebSocketFrame(fin: true, opcode: .binary, data: buffer))
    }

    public func send(_ bytes: [UInt8]) async throws {
        var buffer = ByteBuffer()
        buffer.writeBytes(bytes)
        try await outbound.write(WebSocketFrame(fin: true, opcode: .binary, data: buffer))
    }

    public func send(text: String) async throws {
        var buffer = ByteBuffer()
        buffer.writeString(text)
        try await outbound.write(WebSocketFrame(fin: true, opcode: .text, data: buffer))
    }

    public func close() async throws {
        try await outbound.write(WebSocketFrame(fin: true, opcode: .connectionClose, data: ByteBuffer()))
    }
}

public typealias RawWebSocketHandler = @Sendable (RawWebSocketChannel) async throws -> Void

public actor RawWebSocketServer {
    private let logger: Logger
    private let httpHandler: HTTPRequestHandler?
    private var isRunning = false

    public typealias HTTPRequestHandler = @Sendable (String) -> (status: HTTPResponseStatus, body: String)?

    public init(label: String = "raw-websocket", httpHandler: HTTPRequestHandler? = nil) {
        self.logger = Logger(label: label)
        self.httpHandler = httpHandler
    }

    public func run(host: String = "127.0.0.1", port: Int = 8080, onConnection handler: @escaping RawWebSocketHandler) async throws {
        guard !isRunning else { return }
        isRunning = true

        let httpHandler = self.httpHandler

        let upgrader = NIOTypedWebSocketServerUpgrader<RawUpgradeResult>(
            shouldUpgrade: { channel, _ in
                channel.eventLoop.makeSucceededFuture(HTTPHeaders())
            },
            upgradePipelineHandler: { channel, _ in
                channel.eventLoop.makeCompletedFuture {
                    let asyncChannel = try NIOAsyncChannel<WebSocketFrame, WebSocketFrame>(wrappingChannelSynchronously: channel)
                    return RawUpgradeResult.websocket(asyncChannel)
                }
            }
        )

        let serverChannel = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .bind(host: host, port: port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let upgradeConfig = NIOTypedHTTPServerUpgradeConfiguration(
                        upgraders: [upgrader],
                        notUpgradingCompletionHandler: { channel in
                            channel.eventLoop.makeCompletedFuture {
                                try channel.pipeline.syncOperations.addHandler(RawHTTPHandler(requestHandler: httpHandler))
                                return RawUpgradeResult.http
                            }
                        }
                    )
                    return try channel.pipeline.syncOperations.configureUpgradableHTTPServerPipeline(
                        configuration: NIOUpgradableHTTPServerPipelineConfiguration(upgradeConfiguration: upgradeConfig)
                    )
                }
            }

        logger.info("Raw WebSocket server listening on \(host):\(port)")

        try await withThrowingDiscardingTaskGroup { group in
            try await serverChannel.executeThenClose { inbound in
                for try await connectionResult in inbound {
                    group.addTask {
                        do {
                            let result = try await connectionResult.get()
                            if case .websocket(let wsChannel) = result {
                                try await wsChannel.executeThenClose { inbound, outbound in
                                    try await handler(RawWebSocketChannel(inbound: inbound, outbound: outbound))
                                }
                            }
                        } catch {}
                    }
                }
            }
        }
    }

    public func stop() { isRunning = false }
}

private enum RawUpgradeResult: Sendable {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>)
    case http
}

private final class RawHTTPHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias OutboundOut = HTTPServerResponsePart

    private let requestHandler: RawWebSocketServer.HTTPRequestHandler?

    init(requestHandler: RawWebSocketServer.HTTPRequestHandler?) {
        self.requestHandler = requestHandler
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let part = unwrapInboundIn(data)
        guard case .head(let head) = part else { return }

        if let handler = requestHandler, let response = handler(head.uri) {
            var headers = HTTPHeaders()
            headers.add(name: "Content-Type", value: "text/plain")
            headers.add(name: "Content-Length", value: "\(response.body.utf8.count)")
            context.write(wrapOutboundOut(.head(HTTPResponseHead(version: .http1_1, status: response.status, headers: headers))), promise: nil)
            var body = context.channel.allocator.buffer(capacity: response.body.utf8.count)
            body.writeString(response.body)
            context.write(wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
        } else {
            context.write(wrapOutboundOut(.head(HTTPResponseHead(version: .http1_1, status: .notFound))), promise: nil)
            context.writeAndFlush(wrapOutboundOut(.end(nil)), promise: nil)
        }
    }
}
