import Foundation
import NIO
import NIOHTTP1
import NIOWebSocket
import Logging

#if canImport(Network)
import Network
import NIOTransportServices
#else
import NIOPosix
#endif

public actor ClientManager {
    private weak var system: TransmissionSystem?
    private var connectionTask: Task<Void, Never>?
    private var statusHandler: (@Sendable (ConnectionStatus) async -> Void)?
    private var currentStatus: ConnectionStatus = .disconnected
    private var target: ServerAddress?

    private var activeSession: Task<Void, any Error>?
    private var retrySleep: Task<Void, any Error>?
    private var skipNextWait = false

    init(system: TransmissionSystem) {
        self.system = system
    }

    /// Starts the connection loop. Calling it again for the same address only
    /// updates the status handler; a different address replaces the loop.
    public func connect(
        to address: ServerAddress,
        options: TransmissionClientOptions = .init(),
        onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil
    ) async {
        if connectionTask != nil, target == address {
            if let onStatus { statusHandler = onStatus }
            return
        }
        await stopLoop()
        statusHandler = onStatus
        target = address
        skipNextWait = false

        let task = Task {
            await self.connectionLoop(address: address, options: options)
        }
        connectionTask = task
        system?.setClientLoop(task)
    }

    /// Ends the loop and closes the socket. It stays down until the next `connect`.
    public func disconnect() async {
        await stopLoop()
        await updateStatus(.disconnected)
    }

    /// Retries at once, bypassing any backoff wait and resetting the curve. A
    /// session that is up gets dropped first: call this when the network path
    /// changes or the app returns to the foreground, when an open socket may be
    /// one the OS has already killed.
    public func reconnectNow() {
        skipNextWait = true
        retrySleep?.cancel()
        activeSession?.cancel()
    }

    public var status: ConnectionStatus {
        currentStatus
    }

    private func stopLoop() async {
        guard let task = connectionTask else { return }
        connectionTask = nil
        target = nil
        task.cancel()
        activeSession?.cancel()
        retrySleep?.cancel()
        await task.value
    }

    private func connectionLoop(address: ServerAddress, options: TransmissionClientOptions) async {
        let clock = ContinuousClock()
        let policy = options.reconnect
        var backoff = policy.backoff
        var attempt = 0

        while !Task.isCancelled {
            guard let handle = system.map({ WeakSystem($0) }) else { break }
            attempt += 1
            await updateStatus(attempt == 1 ? .connecting : .reconnecting(attempt: attempt))

            let established = EstablishedAt()
            var failure: (any Error)?
            let session = Task {
                let headers = try await ClientConnectionHandler.resolveHeaders(options)
                try await ClientConnectionHandler.establishConnection(
                    to: address,
                    system: handle,
                    headers: headers,
                    heartbeat: options.heartbeat,
                    onConnected: {
                        established.mark(clock.now)
                        await self.updateStatus(.connected)
                    }
                )
            }
            activeSession = session
            do {
                try await withTaskCancellationHandler {
                    try await session.value
                } onCancel: {
                    session.cancel()
                }
            } catch {
                failure = error
            }
            activeSession = nil
            if Task.isCancelled { break }

            let lived = established.value.map { $0.duration(to: clock.now) }
            if let lived, lived >= policy.minimumSessionLifetime {
                backoff.reset()
                attempt = 0
            }
            if let failure, !(failure is CancellationError) {
                await updateStatus(.failed(failure.localizedDescription))
            }

            // Every path waits, including a clean close: a server that accepts and
            // closes used to get about 1,850 reconnects a second from each client.
            let curve = backoff.next() ?? 0
            let floor = policy.minimumDelay * Double.random(in: 0.5...1.5)
            await waitBeforeRetry(seconds: max(curve, floor), backoff: &backoff)
        }
    }

    private func waitBeforeRetry(seconds: Double, backoff: inout ExponentialBackoff) async {
        if !skipNextWait {
            let sleep = Task { try await Task.sleep(for: .seconds(seconds)) }
            retrySleep = sleep
            _ = await withTaskCancellationHandler {
                await sleep.result
            } onCancel: {
                sleep.cancel()
            }
            retrySleep = nil
        }
        if skipNextWait {
            skipNextWait = false
            backoff.reset()
        }
    }

    private func updateStatus(_ newStatus: ConnectionStatus) async {
        currentStatus = newStatus
        await statusHandler?(newStatus)
    }
}

/// Holds the system without keeping it alive, so that dropping a client's last
/// reference ends its connection loop instead of leaving a zombie.
struct WeakSystem: Sendable {
    private final class Box: @unchecked Sendable {
        weak var value: TransmissionSystem?
    }

    private let box = Box()

    init(_ system: TransmissionSystem) {
        box.value = system
    }

    var value: TransmissionSystem? { box.value }
}

private final class EstablishedAt: @unchecked Sendable {
    private let lock = NSLock()
    private var instant: ContinuousClock.Instant?

    func mark(_ now: ContinuousClock.Instant) {
        lock.withLock { instant = now }
    }

    var value: ContinuousClock.Instant? {
        lock.withLock { instant }
    }
}

enum ClientConnectionHandler {
    /// Headers the WebSocket handshake or the node protocol owns.
    private static let reservedHeaders: Set<String> = [
        "host", "connection", "upgrade", "content-length", "transfer-encoding",
        "sec-websocket-key", "sec-websocket-version", "sec-websocket-accept",
        "sec-websocket-extensions", "x-node-id",
    ]

    static func requiresTLS(_ scheme: ServerAddress.Scheme) -> Bool {
        scheme == .secure
    }

    static func resolveHeaders(_ options: TransmissionClientOptions) async throws -> [String: String] {
        var merged = options.headers
        if let provider = options.headerProvider {
            merged.merge(try await provider()) { _, fresh in fresh }
        }
        return try sanitizedHeaders(merged)
    }

    /// Rejects CR/LF injection and drops anything the handshake owns.
    static func sanitizedHeaders(_ headers: [String: String]) throws -> [String: String] {
        var clean: [String: String] = [:]
        for (name, value) in headers {
            let validName = !name.isEmpty && name.utf8.allSatisfy { $0 > 0x20 && $0 < 0x7F && $0 != UInt8(ascii: ":") }
            let validValue = value.utf8.allSatisfy { $0 != 0x0D && $0 != 0x0A && $0 != 0 }
            guard validName, validValue else {
                throw TransmissionError.connectionFailed("Invalid header: \(name)")
            }
            if !reservedHeaders.contains(name.lowercased()) {
                clean[name] = value
            }
        }
        return clean
    }

    /// RFC 9110 section 7.2: the port is required unless it is the scheme's default.
    static func hostHeader(for address: ServerAddress) -> String {
        let defaultPort = address.scheme == .secure ? 443 : 80
        return address.port == defaultPort ? address.host : "\(address.host):\(address.port)"
    }

    static func establishConnection(
        to address: ServerAddress,
        system handle: WeakSystem,
        headers extraHeaders: [String: String],
        heartbeat: HeartbeatPolicy?,
        onConnected: @escaping @Sendable () async -> Void
    ) async throws {
        guard let system = handle.value else { return }
        let bootstrap = try createBootstrap(for: address.scheme)
        let maxFrameSize = system.maxFrameSize

        let upgrader = NIOTypedWebSocketClientUpgrader<UpgradeResult>(
            maxFrameSize: maxFrameSize,
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
        headers.add(name: "Host", value: hostHeader(for: address))
        headers.add(name: "X-Node-ID", value: system.nodeID.id)
        for (name, value) in extraHeaders {
            headers.add(name: name, value: value)
        }

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
            try await handleWebSocket(
                wsChannel, serverNodeID: serverNodeID, system: handle, maxFrameSize: maxFrameSize,
                heartbeat: heartbeat, onConnected: onConnected)
        case .notUpgraded:
            throw TransmissionError.connectionFailed("WebSocket upgrade failed")
        }
    }

    static func handleWebSocket(
        _ channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        serverNodeID: String?,
        system handle: WeakSystem,
        maxFrameSize: Int,
        heartbeat: HeartbeatPolicy?,
        onConnected: @escaping @Sendable () async -> Void
    ) async throws {
        try await channel.executeThenClose { inbound, outbound in
            let nodeID = serverNodeID.map { NodeIdentity(id: $0) } ?? .server
            let node = RemoteNode(
                nodeID: nodeID,
                channel: channel,
                inbound: inbound,
                outbound: outbound,
                masksFrames: true,
                maxFrameSize: maxFrameSize
            )

            guard let dispatcher = await prepare(node, handle: handle) else { return }
            await onConnected()

            defer { dispatcher.shutdown() }
            do {
                try await WebSocketSession.run(
                    inbound: inbound,
                    outbound: outbound,
                    node: node,
                    dispatcher: dispatcher,
                    maxMessageSize: maxFrameSize,
                    heartbeat: heartbeat
                )
            } catch {
                await handle.value?.nodeDidDisconnect(node)
                throw error
            }
            await handle.value?.nodeDidDisconnect(node)
        }
    }

    private static func prepare(_ node: RemoteNode, handle: WeakSystem) async -> InboundDispatcher? {
        guard let system = handle.value else { return nil }
        await system.adoptUpstream(node)
        let dispatcher = system.makeInboundDispatcher(for: node)
        dispatcher.start()
        return dispatcher
    }

    #if canImport(Network)
    static func createBootstrap(for scheme: ServerAddress.Scheme) throws -> NIOTSConnectionBootstrap {
        let bootstrap = NIOTSConnectionBootstrap(group: NIOTSEventLoopGroup.singleton)
        if requiresTLS(scheme) {
            return bootstrap.tlsOptions(NWProtocolTLS.Options())
        }
        return bootstrap
    }
    #else
    static func createBootstrap(for scheme: ServerAddress.Scheme) throws -> ClientBootstrap {
        // TODO: Add TLS support for wss:// on the NIOPosix (Linux) path via NIOSSL.
        // Refused rather than silently plaintext: requests can now carry credentials.
        guard !requiresTLS(scheme) else {
            throw TransmissionError.connectionFailed("wss:// is not supported on this platform yet")
        }
        return ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
    }
    #endif
}

private enum UpgradeResult: Sendable {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>, serverNodeID: String?)
    case notUpgraded
}

extension TransmissionSystem {
    /// Starts connecting. Idempotent: a repeat call for the same address only
    /// updates `onStatus`; a different address replaces the connection.
    public func connect(to address: ServerAddress, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        try await connect(to: address, options: .init(), onStatus: onStatus)
    }

    public func connect(
        to address: ServerAddress,
        options: TransmissionClientOptions,
        onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil
    ) async throws {
        await sharedClientManager().connect(to: address, options: options, onStatus: onStatus)
    }

    public func connect(to url: String, onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil) async throws {
        try await connect(to: url, options: .init(), onStatus: onStatus)
    }

    public func connect(
        to url: String,
        options: TransmissionClientOptions,
        onStatus: (@Sendable (ConnectionStatus) async -> Void)? = nil
    ) async throws {
        guard let address = ServerAddress(url: url) else {
            throw TransmissionError.invalidURL(url)
        }
        try await connect(to: address, options: options, onStatus: onStatus)
    }

    /// Ends the connection loop and closes the socket. In-flight calls fail with
    /// `noConnection`. Unlike closing a node, this does not reconnect.
    public func disconnect() async {
        await clientManagerIfAny()?.disconnect()
    }

    /// Retries now, bypassing backoff. See `ClientManager.reconnectNow()`.
    public func reconnectNow() async {
        await clientManagerIfAny()?.reconnectNow()
    }
}
