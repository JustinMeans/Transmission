import Distributed
import Foundation
import Testing
import TransmissionTestSupport
import TransmissionVapor
import Vapor
@testable import Transmission

struct RequireToken: AsyncMiddleware {
    func respond(to request: Request, chainingTo next: any AsyncResponder) async throws -> Response {
        guard request.headers.bearerAuthorization?.token == "good" else {
            throw Abort(.unauthorized)
        }
        return try await next.respond(to: request)
    }
}

final class HeaderLog: @unchecked Sendable {
    private let lock = NSLock()
    private var _host: String?
    private var _tokens: [String] = []
    var host: String? { lock.withLock { _host } }
    var tokens: [String] { lock.withLock { _tokens } }
    func record(_ headers: HTTPHeaders) {
        lock.withLock {
            _host = headers.first(name: .host)
            if let token = headers.bearerAuthorization?.token { _tokens.append(token) }
        }
    }
}

struct RecordHeaders: AsyncMiddleware {
    let log: HeaderLog
    func respond(to request: Request, chainingTo next: any AsyncResponder) async throws -> Response {
        log.record(request.headers)
        return try await next.respond(to: request)
    }
}

/// The real server path: `TransmissionVapor` on a real socket.
@Suite("TransmissionVapor end to end (audit V rows)", .serialized)
struct VaporEndToEndTests {

    @Test("V1 without middleware the route stays open, as documented")
    func v1Open() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("V1 with auth middleware an anonymous client cannot reach an actor")
    func v1Rejected() async throws {
        try await withVaporServer(middleware: [RequireToken()]) { server, _, port in
            let client = TransmissionSystem(id: "anon")
            client.connectionTimeout = .milliseconds(500)
            try await client.connect(to: address(port))
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            await #expect(throws: (any Error).self) { _ = try await remote.ok() }
            #expect(await server.nodes.connectedNodes.isEmpty)
        }
    }

    @Test("V1 injected Authorization header passes the middleware; Host carries the port")
    func v1Accepted() async throws {
        let log = HeaderLog()
        try await withVaporServer(middleware: [RecordHeaders(log: log), RequireToken()]) { _, _, port in
            let options = TransmissionClientOptions(headers: ["Authorization": "Bearer good"])
            let client = try await connectedClient(port: port, options: options, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await remote.ok() == 42)
            #expect(log.host == "127.0.0.1:\(port)")
        }
    }

    @Test("headerProvider is re-evaluated for every connection attempt")
    func headerProviderPerAttempt() async throws {
        let log = HeaderLog()
        let attempts = AttemptCounter()
        try await withVaporServer(middleware: [RecordHeaders(log: log)]) { server, _, port in
            let options = TransmissionClientOptions(
                headerProvider: { ["Authorization": "Bearer token-\(attempts.next())"] },
                reconnect: ReconnectPolicy(minimumDelay: 0.01))
            let client = try await connectedClient(id: "rotating", port: port, options: options, serverID: "mainframe")
            await eventually { await server.nodes.connectedNodes.count == 1 }
            for node in await server.nodes.connectedNodes { await node.close() }
            await eventually { log.tokens.count >= 2 }
            await client.disconnect()
            #expect(log.tokens.prefix(2) == ["token-1", "token-2"])
        }
    }

    @Test("V2 a remote Void throw surfaces over Vapor")
    func v2() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            await #expect(throws: RemoteCallError.self) { try await remote.voidThrow() }
            await #expect(throws: RemoteCallError.self) { _ = try await remote.valThrow() }
        }
    }

    @Test("E2/E3 skew and unknown actors fail fast over Vapor")
    func skewOverVapor() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: "mainframe", timeout: .seconds(2))
            for (actor, target) in [("svc", "$sNope"), ("nope", "$sNope")] {
                await #expect(throws: RemoteCallError.self) {
                    _ = try await client.pendingCalls.send(
                        envelope: CallEnvelope(
                            callID: CallID(),
                            recipient: ActorIdentity(id: actor, node: "mainframe"),
                            target: target, genericSubs: [], args: [], priority: .normal),
                        via: node, timeout: .seconds(2))
                }
            }
        }
    }

    @Test("V3 20 KiB and 20 MiB arguments succeed over Vapor")
    func v3() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await remote.echoSize(String(repeating: "y", count: 20_000)) == 20_000)
            let size = try await withDeadline(.seconds(30), "V3") {
                try await remote.echoSize(String(repeating: "y", count: 20 << 20))
            }
            #expect(size == 20 << 20)
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("V4 2,000 in-order frames on one socket stay in order over Vapor")
    func v4() async throws {
        let target = try await capturedTarget { try await $0.seq(0) }
        let context = ServiceContext()
        try await withVaporApp(context: context) { _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: "mainframe", timeout: .seconds(2))
            for index in 0..<2000 {
                let argument = try JSONEncoder().encode(index)
                try await node.send(.call(CallEnvelope(
                    callID: CallID(),
                    recipient: ActorIdentity(id: "svc", node: "mainframe"),
                    target: target, genericSubs: [], args: [argument], priority: .normal)))
            }
            await eventually(within: .seconds(10)) { await context.recorder.seen.count == 2000 }
            #expect(await context.recorder.seen.count == 2000)
            #expect(await context.recorder.inversions == 0)
        }
    }

    @Test("E4 a handler calling back to its caller works over Vapor")
    func reentrantOverVapor() async throws {
        let context = ServiceContext()
        try await withVaporApp(context: context) { server, port in
            let client = try await connectedClient(id: "cli-vapor", port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let peer = client.makeLocalActor(id: "peer") { RegPeer(actorSystem: client) }
            _ = peer
            await eventually { await server.nodes.connectedNodes.count == 1 }
            context.peer = try RegPeer.resolve(id: ActorIdentity(id: "peer", node: "cli-vapor"), using: server)
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await withDeadline(.seconds(5), "E4 Vapor") { try await remote.callback() } == 7)
        }
    }

    @Test("E6 masked client frames decode correctly on Vapor")
    func maskedOverVapor() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await remote.echoSize("héllo") == 6)
        }
    }

    @Test("E7 the stale close race over Vapor")
    func staleCloseOverVapor() async throws {
        try await withVaporServer { server, _, port in
            let first = try await connectedClient(id: "phone", port: port, serverID: "mainframe")
            await eventually { await server.nodes.connectedNodes.count == 1 }
            let second = try await connectedClient(id: "phone", port: port, serverID: "mainframe")
            defer { Task { await second.disconnect() } }
            try await Task.sleep(for: .milliseconds(100))
            await first.disconnect()
            try await Task.sleep(for: .milliseconds(200))
            #expect(await server.nodes.connectedNodes.count == 1)
            let peer = second.makeLocalActor(id: "peer") { RegPeer(actorSystem: second) }
            _ = peer
            let remote = try RegPeer.resolve(id: ActorIdentity(id: "peer", node: "phone"), using: server)
            #expect(try await withDeadline(.seconds(2), "server to replacement") { try await remote.pong() } == 7)
        }
    }

    @Test("T4 RemoteNode.current over Vapor, including the upgrade hook's identity")
    func currentNodeOverVapor() async throws {
        try await withVaporServer { _, _, port in
            let client = try await connectedClient(id: "vapor-caller", port: port, serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await remote.whoCalled() == "vapor-caller")
        }
    }

    @Test("oversize fails the call and keeps the connection over Vapor")
    func oversizeOverVapor() async throws {
        try await withVaporServer(configure: { $0.maxFrameSize = 1 << 20 }) { _, _, port in
            let client = TransmissionSystem(id: "limit")
            client.maxFrameSize = 1 << 20
            try await client.connect(to: address(port))
            defer { Task { await client.disconnect() } }
            _ = try await client.nodes.node(for: "mainframe", timeout: .seconds(2))
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            await #expect(throws: TransmissionError.self) {
                _ = try await remote.echoSize(String(repeating: "y", count: 2 << 20))
            }
            await #expect(throws: RemoteCallError.self) { _ = try await remote.big(2 << 20) }
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("Vapor drops a client that stops answering pings")
    func vaporHeartbeat() async throws {
        try await withVaporServer(configure: {
            $0.serverHeartbeat = HeartbeatPolicy(interval: .milliseconds(100), timeout: .milliseconds(200))
        }) { server, _, port in
            let silent = try RawSocketClient(port: port, path: "/transmission", headers: ["X-Node-ID": "silent"])
            defer { silent.shutdown() }
            await eventually { await server.nodes.connectedNodes.count == 1 }
            let dropped = await eventually(within: .seconds(3)) { await server.nodes.connectedNodes.isEmpty }
            #expect(dropped)
        }
    }

    @Test("client reconnects after the Vapor server closes it, then calls work again")
    func reconnectOverVapor() async throws {
        try await withVaporServer { server, _, port in
            let client = try await connectedClient(
                id: "again", port: port,
                options: .init(reconnect: ReconnectPolicy(minimumDelay: 0.01)),
                serverID: "mainframe")
            defer { Task { await client.disconnect() } }
            for node in await server.nodes.connectedNodes { await node.close() }
            try await Task.sleep(for: .milliseconds(100))
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "mainframe"), using: client)
            #expect(try await withDeadline(.seconds(5), "after reconnect") { try await remote.ok() } == 42)
        }
    }
}

final class AttemptCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func next() -> Int { lock.withLock { value += 1; return value } }
}

private func withVaporApp<T: Sendable>(
    context: ServiceContext,
    _ body: @Sendable (TransmissionSystem, Int) async throws -> T
) async throws -> T {
    let app = try await Application.make(Environment(name: "testing", arguments: ["vapor"]))
    app.logger.logLevel = .critical
    app.http.server.configuration.hostname = "127.0.0.1"
    app.http.server.configuration.port = 0
    let server = TransmissionSystem.server(id: "mainframe")
    try app.transmission.register(server)
    try app.transmission.configure(path: "transmission")
    let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server, context: context) }
    _ = service
    do {
        try await app.startup()
        let result = try await body(server, app.http.server.shared.localAddress!.port!)
        try await app.asyncShutdown()
        return result
    } catch {
        try? await app.asyncShutdown()
        throw error
    }
}
