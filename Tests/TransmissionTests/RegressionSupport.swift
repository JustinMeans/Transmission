import Distributed
import Foundation
import Logging
import NIOCore
import Testing
import TransmissionTestSupport
import TransmissionVapor
import Vapor
@testable import Transmission

struct Boom: Error, Codable, Equatable { var reason = "boom" }
struct Opaque: Error { let secret = "do-not-leak" }

actor Recorder {
    private(set) var seen: [Int] = []
    func add(_ value: Int) { seen.append(value) }
    var inversions: Int {
        zip(seen, seen.dropFirst()).filter { $0 > $1 }.count
    }
}

/// Shared across one test's server actor through the actor itself.
final class ServiceContext: @unchecked Sendable {
    let recorder = Recorder()
    let lock = NSLock()
    private var _peer: RegPeer?
    private var _caller: NodeIdentity?
    private var _running = 0
    private var _peakRunning = 0
    var peer: RegPeer? {
        get { lock.withLock { _peer } }
        set { lock.withLock { _peer = newValue } }
    }
    var caller: NodeIdentity? {
        get { lock.withLock { _caller } }
        set { lock.withLock { _caller = newValue } }
    }
    var peakRunning: Int { lock.withLock { _peakRunning } }
    func enter() { lock.withLock { _running += 1; _peakRunning = max(_peakRunning, _running) } }
    func leave() { lock.withLock { _running -= 1 } }
}

distributed actor RegSvc {
    typealias ActorSystem = TransmissionSystem
    let context: ServiceContext

    init(actorSystem: ActorSystem, context: ServiceContext = ServiceContext()) {
        self.actorSystem = actorSystem
        self.context = context
    }

    distributed func ok() -> Int { 42 }
    distributed func voidThrow() throws { throw Boom() }
    distributed func valThrow() throws -> Int { throw Boom() }
    distributed func opaqueThrow() throws { throw Opaque() }
    distributed func big(_ count: Int) -> String { String(repeating: "x", count: count) }
    distributed func echoSize(_ text: String) -> Int { text.utf8.count }
    distributed func seq(_ value: Int) async { await context.recorder.add(value) }
    distributed func callback() async throws -> Int { try await context.peer!.pong() }
    distributed func whoCalled() -> String? { RemoteNode.current?.nodeID.id }
    distributed func slow(_ milliseconds: Int) async -> Int {
        context.enter()
        defer { context.leave() }
        try? await Task.sleep(for: .milliseconds(milliseconds))
        return milliseconds
    }
    distributed func userTag() async -> String? { await RemoteNode.current?.getUserInfo("user") }
    distributed func waitForCancel() async {
        try? await Task.sleep(for: .seconds(30))
        await context.recorder.add(Task.isCancelled ? 1 : 0)
    }
    distributed func call(peer: RegPeer) async throws -> Int { try await peer.pong() }
    /// Keeps a slot busy so that a nested call into this same actor has to get past it.
    distributed func reenter(from peer: RegPeer) async throws -> Int { try await peer.callBack(self) }
}

distributed actor RegPeer {
    typealias ActorSystem = TransmissionSystem
    distributed func pong() -> Int { 7 }
    distributed func callBack(_ service: RegSvc) async throws -> Int { try await service.ok() }
}

func address(_ port: Int, path: String = "/transmission") -> ServerAddress {
    ServerAddress(scheme: .insecure, host: "127.0.0.1", port: port, path: path)
}

/// A NIO `ServerManager` on a free port, cancelled when `body` returns.
func withNIOServer<T: Sendable>(
    id: String = "srv",
    configure: @Sendable (TransmissionSystem) -> Void = { _ in },
    _ body: @Sendable (TransmissionSystem, RegSvc, Int) async throws -> T
) async throws -> T {
    let server = TransmissionSystem.server(id: id)
    configure(server)
    let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server) }
    let port = freePort()
    let task = Task { try await server.runServer(at: address(port)) }
    defer { task.cancel() }
    await eventually { (try? tcpPortAccepts(port)) == true }
    return try await body(server, service, port)
}

/// Real `TransmissionVapor` on a real port.
func withVaporServer<T: Sendable>(
    id: String = "mainframe",
    middleware: [any Vapor.Middleware] = [],
    onConnect: (@Sendable (Request, RemoteNode) async throws -> Void)? = nil,
    configure: @Sendable (TransmissionSystem) -> Void = { _ in },
    _ body: @Sendable (TransmissionSystem, RegSvc, Int) async throws -> T
) async throws -> T {
    let app = try await Application.make(Environment(name: "testing", arguments: ["vapor"]))
    app.logger.logLevel = .critical
    app.http.server.configuration.hostname = "127.0.0.1"
    app.http.server.configuration.port = 0
    app.http.server.configuration.shutdownTimeout = .milliseconds(100)
    let server = TransmissionSystem.server(id: id)
    configure(server)
    try app.transmission.register(server)
    try app.transmission.configure(path: "transmission", middleware: middleware, onConnect: onConnect)
    let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server) }
    do {
        try await app.startup()
        let port = app.http.server.shared.localAddress!.port!
        let result = try await body(server, service, port)
        try await app.asyncShutdown()
        return result
    } catch {
        try? await app.asyncShutdown()
        throw error
    }
}

func connectedClient(
    id: String = UUID().uuidString,
    port: Int,
    options: TransmissionClientOptions = .init(),
    serverID: NodeIdentity
) async throws -> TransmissionSystem {
    let client = TransmissionSystem(id: id)
    try await client.connect(to: address(port), options: options)
    _ = try await client.nodes.node(for: serverID, timeout: .seconds(5))
    return client
}

/// Learns the mangled identifier of a distributed target without hard-coding it,
/// so ordering tests can put frames on one socket without awaiting each reply.
func capturedTarget(
    _ invoke: @escaping @Sendable (RegSvc) async throws -> Void
) async throws -> String {
    let probe = TransmissionSystem(id: "probe")
    let captured = CapturedTarget()
    let node = RemoteNode(nodeID: "probe-server", send: { data in
        if case .call(let call) = try WireEnvelope.decodeCompact(from: data) {
            captured.set(call.target)
        }
    }, close: {})
    await probe.nodes.register(node)
    let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "probe-server"), using: probe)
    let task = Task { try await invoke(remote) }
    await eventually { captured.value != nil }
    task.cancel()
    await probe.pendingCalls.cancelAll(for: "probe-server")
    return try #require(captured.value)
}

final class CapturedTarget: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: String?
    var value: String? { lock.withLock { _value } }
    func set(_ value: String) { lock.withLock { _value = value } }
}

/// `RawWebSocketServer` on a free port, cancelled when `body` returns.
func withRawServer<T: Sendable>(
    handler: @escaping RawWebSocketHandler,
    _ body: @Sendable (Int) async throws -> T
) async throws -> T {
    let raw = RawWebSocketServer()
    let port = freePort()
    let task = Task { try await raw.run(port: port, onConnection: handler) }
    defer { task.cancel() }
    await eventually { (try? tcpProbe(port)) == true }
    return try await body(port)
}

func tcpProbe(_ port: Int) throws -> Bool {
    try tcpPortAccepts(port)
}

actor Counter {
    private(set) var value = 0
    func increment() { value += 1 }
}

actor StatusLog {
    private(set) var all: [ConnectionStatus] = []
    func add(_ status: ConnectionStatus) { all.append(status) }
    var connectedCount: Int { all.filter { $0 == .connected }.count }
}
