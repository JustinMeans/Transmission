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
    private var connectionTask: Task<Void, Never>?
    private var statusHandler: (@Sendable (ConnectionStatus) async -> Void)?
    private var currentStatus: ConnectionStatus = .disconnected
    private var backoff = ExponentialBackoff.standard

    init(system: TransmissionSystem) {
        self.system = system
    }

    public func connect(to address: ServerAddress, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) {
        connectionTask?.cancel()
        self.statusHandler = onStatus

        let systemRef = system

        connectionTask = Task { [weak self] in
            guard let self else { return }
            await self.connectionLoop(address: address, system: systemRef)
        }
    }

    public func disconnect() async {
        connectionTask?.cancel()
        connectionTask = nil
        await updateStatus(.disconnected)
    }

    public var status: ConnectionStatus {
        currentStatus
    }

    private func connectionLoop(address: ServerAddress, system: TransmissionSystem) async {
        var attempt = 0

        while !Task.isCancelled {
            attempt += 1
            await updateStatus(attempt == 1 ? .connecting : .reconnecting(attempt: attempt))

            do {
                try await ClientConnectionHandler.establishConnection(
                    to: address,
                    system: system,
                    onConnected: { [weak self] in
                        await self?.updateStatus(.connected)
                    }
                )
                backoff.reset()
                attempt = 0
            } catch is CancellationError {
                break
            } catch {
                await updateStatus(.failed(error.localizedDescription))
            }

            if let delay = backoff.next(), delay > 0 {
                do {
                    try await Task.sleep(for: .seconds(delay))
                } catch {
                    break
                }
            }
        }

        await updateStatus(.disconnected)
    }

    private func updateStatus(_ newStatus: ConnectionStatus) async {
        currentStatus = newStatus
        await statusHandler?(newStatus)
    }
}

private enum ClientConnectionHandler {
    static func establishConnection(
        to address: ServerAddress,
        system: TransmissionSystem,
        onConnected: @escaping @Sendable () async -> Void
    ) async throws {
        let bootstrap = createBootstrap()

        let upgrader = NIOTypedWebSocketClientUpgrader<UpgradeResult>(
            upgradePipelineHandler: { channel, upgradeResponse in
                channel.eventLoop.makeCompletedFuture {
                    let asyncChannel = try NIOAsyncChannel<WebSocketFrame, WebSocketFrame>(
                        wrappingChannelSynchronously: channel
                    )
                    let serverNodeID = upgradeResponse.headers.first(name: "X-Server-Node-ID")
                    return UpgradeResult.websocket(asyncChannel, serverNodeID: serverNodeID)
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

        let result = try await upgradeResult.get()

        switch result {
        case .websocket(let wsChannel, let serverNodeID):
            try await handleWebSocket(wsChannel, serverNodeID: serverNodeID, system: system, onConnected: onConnected)
        case .notUpgraded:
            throw TransmissionError.connectionFailed("WebSocket upgrade failed")
        }
    }

    static func handleWebSocket(
        _ channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        serverNodeID: String?,
        system: TransmissionSystem,
        onConnected: @escaping @Sendable () async -> Void
    ) async throws {
        try await channel.executeThenClose { inbound, outbound in
            let nodeID: NodeIdentity
            if let serverID = serverNodeID {
                nodeID = NodeIdentity(id: serverID)
                system.logger.debug("Connected to server with ID: \(serverID)")
            } else {
                nodeID = .server
                system.logger.debug("Connected to server (no ID provided, using default)")
            }

            let node = RemoteNode(
                nodeID: nodeID,
                channel: channel,
                inbound: inbound,
                outbound: outbound
            )

            await system.nodes.register(node)
            await onConnected()

            do {
                try await processFrames(
                    inbound: inbound,
                    outbound: outbound,
                    system: system,
                    node: node
                )
            } catch {
                await system.nodes.unregister(nodeID)
                throw error
            }

            await system.nodes.unregister(nodeID)
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
        NIOTSConnectionBootstrap(group: NIOTSEventLoopGroup.singleton)
    }
    #else
    static func createBootstrap() -> ClientBootstrap {
        ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
    }
    #endif
}

private enum UpgradeResult: Sendable {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>, serverNodeID: String?)
    case notUpgraded
}

extension TransmissionSystem {
    public func connect(to address: ServerAddress, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        let client = ClientManager(system: self)
        setClientManager(client)
        await client.connect(to: address, onStatus: onStatus)
    }

    public func connect(to url: String, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        guard let address = ServerAddress(url: url) else {
            throw TransmissionError.invalidURL(url)
        }
        try await connect(to: address, onStatus: onStatus)
    }
}
