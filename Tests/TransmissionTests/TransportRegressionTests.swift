import Distributed
import Foundation
import NIOCore
import NIOWebSocket
import Testing
import TransmissionTestSupport
@testable import Transmission

/// Every E row of the audit's section 3, against the NIO transports.
/// IDs match `transmission-audit.md`.
@Suite("Transport regressions (audit section 3)", .serialized)
struct TransportRegressionTests {

    @Test("E1a remote Void func that throws surfaces the error")
    func e1a() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            await #expect(throws: (any Error).self) { try await remote.voidThrow() }
        }
    }

    @Test("E1b remote value func that throws surfaces the error, not a DecodingError")
    func e1b() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            do {
                _ = try await remote.valThrow()
                Issue.record("expected a throw")
            } catch {
                #expect(!(error is DecodingError), "got \(error)")
                #expect(String(describing: error).contains("Boom"))
            }
        }
    }

    @Test("E2 a non-existent mangled target fails instead of reporting success")
    func e2() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: "srv", timeout: .seconds(2))
            let call = CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "svc", node: "srv"),
                target: "$s6Bridge3HubC3get3appSayAA4StatVGSS_tFTE",
                genericSubs: [], args: [], priority: .normal)
            await #expect(throws: (any Error).self) {
                _ = try await client.pendingCalls.send(envelope: call, via: node, timeout: .seconds(3))
            }
        }
    }

    @Test("E3 an unknown actor answers at once instead of waiting out the timeout")
    func e3() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: "srv", timeout: .seconds(2))
            let call = CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "nope", node: "srv"),
                target: "x", genericSubs: [], args: [], priority: .normal)
            let clock = ContinuousClock()
            let started = clock.now
            await #expect(throws: (any Error).self) {
                _ = try await client.pendingCalls.send(envelope: call, via: node, timeout: .seconds(3))
            }
            #expect(started.duration(to: clock.now) < .seconds(2))
        }
    }

    @Test("E4 a handler awaiting a call back to the same peer does not deadlock")
    func e4() async throws {
        let context = ServiceContext()
        let server = TransmissionSystem.server(id: "srv")
        let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server, context: context) }
        _ = service
        let port = freePort()
        let task = Task { try await server.runServer(at: address(port)) }
        defer { task.cancel() }
        let client = try await connectedClient(id: "cli-e4", port: port, serverID: "srv")
        defer { Task { await client.disconnect() } }
        let peer = client.makeLocalActor(id: "peer") { RegPeer(actorSystem: client) }
        _ = peer
        context.peer = try RegPeer.resolve(id: ActorIdentity(id: "peer", node: "cli-e4"), using: server)
        let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
        let value = try await withDeadline(.seconds(5), "E4") { try await remote.callback() }
        #expect(value == 7)
    }

    @Test("E5a a 20 KiB argument reaches ServerManager and the connection survives")
    func e5a() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            let size = try await withDeadline(.seconds(5), "E5a") {
                try await remote.echoSize(String(repeating: "y", count: 20_000))
            }
            #expect(size == 20_000)
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("E5b/c 20 MiB and 15 MiB replies reach a NIO client")
    func e5b() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            let large = try await withDeadline(.seconds(30), "E5b") { try await remote.big(20 << 20) }
            #expect(large.utf8.count == 20 << 20)
            let medium = try await withDeadline(.seconds(30), "E5c") { try await remote.big(15 << 20) }
            #expect(medium.utf8.count == 15 << 20)
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("E6 every client frame is masked and every payload survives unmasking")
    func e6() async throws {
        let masks = FrameLog()
        try await withRawServer(handler: { channel in
            for try await frame in channel.inbound where frame.opcode == .binary {
                masks.add(masked: frame.maskKey != nil, bytes: frame.unmaskedData.readableBytesView.map { $0 })
            }
        }) { port in
            let client = try await connectedClient(port: port, serverID: .server)
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: .server, timeout: .seconds(1))
            try await node.sendBinary(Data([1, 2, 3, 4, 5]))
            await eventually { masks.count > 0 }
            #expect(masks.allMasked)
            #expect(masks.first == [1, 2, 3, 4, 5])
        }
    }

    @Test("E6 a masked client works end to end against ServerManager")
    func e6EndToEnd() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            #expect(try await remote.echoSize("héllo") == 6)
        }
    }

    @Test("E7 a late close from the old socket leaves the new connection in place")
    func e7() async throws {
        let system = TransmissionSystem(id: "d")
        let old = RemoteNode(nodeID: "mobile", send: { _ in }, close: {})
        let new = RemoteNode(nodeID: "mobile", send: { _ in }, close: {})
        await system.nodes.register(old)
        await system.nodes.register(new)
        await system.nodeDidDisconnect(old)
        #expect(await system.nodes.connectedNodes.count == 1)
    }

    @Test("E7 end to end: the first client's close does not evict its replacement")
    func e7EndToEnd() async throws {
        try await withNIOServer { server, _, port in
            let first = try await connectedClient(id: "phone", port: port, serverID: "srv")
            await eventually { await server.nodes.connectedNodes.count == 1 }
            let second = try await connectedClient(id: "phone", port: port, serverID: "srv")
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

    @Test("E8 a server that accepts then closes does not cause a reconnect storm")
    func e8() async throws {
        let connections = Counter()
        try await withRawServer(handler: { _ in await connections.increment() }) { port in
            let client = TransmissionSystem(id: "e8")
            try await client.connect(to: address(port))
            try await Task.sleep(for: .seconds(2))
            await client.disconnect()
            // The old code measured 3,697 here. Backoff from 0.25s floor: about 5.
            #expect(await connections.value <= 12)
            #expect(await connections.value >= 2)
        }
    }

    @Test("E10 a client addressing NodeIdentity.server reaches a named server")
    func e10() async throws {
        try await withNIOServer(id: "mainframe") { _, _, port in
            let client = TransmissionSystem(id: "e10")
            client.connectionTimeout = .seconds(2)
            try await client.connect(to: address(port))
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: .server), using: client)
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("E13 closing nodes and dropping the system leaves no zombie reconnect")
    func e13() async throws {
        try await withNIOServer { server, _, port in
            var client: TransmissionSystem? = try await connectedClient(port: port, serverID: "srv")
            await eventually { await server.nodes.connectedNodes.count == 1 }
            for node in await client!.nodes.connectedNodes { await node.close() }
            client = nil
            _ = client
            let empty = await eventually(within: .seconds(2)) {
                await server.nodes.connectedNodes.isEmpty
            }
            try await Task.sleep(for: .milliseconds(800))
            #expect(empty)
            #expect(await server.nodes.connectedNodes.isEmpty)
        }
    }

    @Test("E13 public disconnect leaves nothing connected and stays down")
    func e13Disconnect() async throws {
        try await withNIOServer { server, _, port in
            let log = StatusLog()
            let client = TransmissionSystem(id: "e13d")
            try await client.connect(to: address(port), onStatus: { await log.add($0) })
            await eventually { await server.nodes.connectedNodes.count == 1 }
            await client.disconnect()
            await eventually { await server.nodes.connectedNodes.isEmpty }
            try await Task.sleep(for: .milliseconds(600))
            #expect(await server.nodes.connectedNodes.isEmpty)
            #expect(await log.all.last == .disconnected)
        }
    }

    @Test("connect twice leaves one loop")
    func idempotentConnect() async throws {
        let connections = Counter()
        try await withRawServer(handler: { channel in
            await connections.increment()
            for try await _ in channel.inbound {}
        }) { port in
            let client = TransmissionSystem(id: "twice")
            try await client.connect(to: address(port))
            try await client.connect(to: address(port))
            try await client.connect(to: address(port))
            try await Task.sleep(for: .milliseconds(400))
            await client.disconnect()
            #expect(await connections.value == 1)
        }
    }

    @Test("reconnectNow skips a long backoff wait")
    func reconnectNow() async throws {
        let connections = Counter()
        try await withRawServer(handler: { _ in await connections.increment() }) { port in
            let client = TransmissionSystem(id: "now")
            let slow = ReconnectPolicy(
                backoff: ExponentialBackoff(initial: 30, minimum: 30, maximum: 30, jitter: 0),
                minimumDelay: 30)
            try await client.connect(to: address(port), options: .init(reconnect: slow))
            await eventually { await connections.value == 1 }
            try await Task.sleep(for: .milliseconds(100))
            #expect(await connections.value == 1)
            await client.reconnectNow()
            let retried = await eventually(within: .seconds(2)) { await connections.value >= 2 }
            #expect(retried)
            await client.disconnect()
        }
    }

    @Test("a session that survives the minimum lifetime resets the backoff")
    func backoffResetsAfterHealthySession() async throws {
        let connections = Counter()
        try await withRawServer(handler: { _ in
            await connections.increment()
            if await connections.value >= 3 { return }
            try await Task.sleep(for: .milliseconds(150))
        }) { port in
            let client = TransmissionSystem(id: "reset")
            let policy = ReconnectPolicy(
                backoff: ExponentialBackoff(initial: 20, minimum: 20, maximum: 20, jitter: 0),
                minimumSessionLifetime: .milliseconds(100),
                minimumDelay: 0.01)
            try await client.connect(to: address(port), options: .init(reconnect: policy))
            let reconnected = await eventually(within: .seconds(2)) { await connections.value >= 2 }
            #expect(reconnected)
            await client.disconnect()
        }
    }

    @Test("a dead peer that never pongs is detected and the client reconnects")
    func clientDetectsDeadPeer() async throws {
        let connections = Counter()
        try await withRawServer(handler: { channel in
            await connections.increment()
            // Swallows every frame, pings included: a half-open socket.
            for try await _ in channel.inbound {}
        }) { port in
            let client = TransmissionSystem(id: "ping")
            let policy = HeartbeatPolicy(interval: .milliseconds(50), timeout: .milliseconds(200))
            let quick = ReconnectPolicy(minimumDelay: 0.01)
            try await client.connect(to: address(port), options: .init(reconnect: quick, heartbeat: policy))
            let reconnected = await eventually(within: .seconds(3)) { await connections.value >= 2 }
            #expect(reconnected)
            await client.disconnect()
        }
    }

    @Test("a live peer that answers pings stays connected past the timeout")
    func livePeerSurvivesHeartbeat() async throws {
        try await withNIOServer { server, _, port in
            let log = StatusLog()
            let client = TransmissionSystem(id: "alive")
            let policy = HeartbeatPolicy(interval: .milliseconds(50), timeout: .milliseconds(200))
            try await client.connect(to: address(port), options: .init(heartbeat: policy), onStatus: { await log.add($0) })
            try await Task.sleep(for: .milliseconds(600))
            #expect(await log.connectedCount == 1)
            await client.disconnect()
        }
    }

    @Test("ServerManager drops a client that stops answering pings")
    func serverDetectsDeadPeer() async throws {
        try await withNIOServer(configure: {
            $0.serverHeartbeat = HeartbeatPolicy(interval: .milliseconds(50), timeout: .milliseconds(200))
        }) { server, _, port in
            let silent = try RawSocketClient(port: port, path: "/transmission", headers: ["X-Node-ID": "silent"])
            defer { silent.shutdown() }
            await eventually { await server.nodes.connectedNodes.count == 1 }
            let dropped = await eventually(within: .seconds(3)) { await server.nodes.connectedNodes.isEmpty }
            #expect(dropped)
        }
    }

    @Test("custom headers reject CR/LF and cannot replace protocol headers")
    func headerSanitising() throws {
        let cleaned = try ClientConnectionHandler.sanitizedHeaders([
            "Authorization": "Bearer t",
            "Host": "evil",
            "Sec-WebSocket-Key": "evil",
            "X-Node-ID": "evil",
        ])
        #expect(cleaned == ["Authorization": "Bearer t"])
        #expect(throws: TransmissionError.self) {
            _ = try ClientConnectionHandler.sanitizedHeaders(["X-Bad": "a\r\nInjected: yes"])
        }
        #expect(throws: TransmissionError.self) {
            _ = try ClientConnectionHandler.sanitizedHeaders(["X\nBad": "a"])
        }
    }

    @Test("Host header keeps a non-default port")
    func hostHeader() {
        #expect(ClientConnectionHandler.hostHeader(for: address(19000)) == "127.0.0.1:19000")
        #expect(ClientConnectionHandler.hostHeader(for: ServerAddress(scheme: .secure, host: "a.example", port: 443)) == "a.example")
        #expect(ClientConnectionHandler.hostHeader(for: ServerAddress(scheme: .insecure, host: "a.example", port: 80)) == "a.example")
    }

    @Test("ordering: 2,000 frames on one socket reach one actor in order (NIO)")
    func orderingNIO() async throws {
        let target = try await capturedTarget { try await $0.seq(0) }
        let context = ServiceContext()
        let server = TransmissionSystem.server(id: "srv")
        let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server, context: context) }
        _ = service
        let port = freePort()
        let task = Task { try await server.runServer(at: address(port)) }
        defer { task.cancel() }
        let client = try await connectedClient(port: port, serverID: "srv")
        defer { Task { await client.disconnect() } }
        let node = try await client.nodes.node(for: "srv", timeout: .seconds(2))
        for index in 0..<2000 {
            let argument = try JSONEncoder().encode(index)
            try await node.send(.call(CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "svc", node: "srv"),
                target: target, genericSubs: [], args: [argument], priority: .normal)))
        }
        await eventually(within: .seconds(10)) { await context.recorder.seen.count == 2000 }
        #expect(await context.recorder.seen.count == 2000)
        #expect(await context.recorder.inversions == 0)
    }

    @Test("connectionTimeout is safe to mutate concurrently")
    func timeoutIsSynchronized() async {
        let system = TransmissionSystem(id: "race")
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<200 {
                group.addTask {
                    system.connectionTimeout = .seconds(index)
                    _ = system.connectionTimeout
                }
            }
        }
    }
}

final class FrameLog: @unchecked Sendable {
    private let lock = NSLock()
    private var frames: [(masked: Bool, bytes: [UInt8])] = []
    func add(masked: Bool, bytes: [UInt8]) { lock.withLock { frames.append((masked, bytes)) } }
    var count: Int { lock.withLock { frames.count } }
    var allMasked: Bool { lock.withLock { frames.allSatisfy(\.masked) } }
    var first: [UInt8]? { lock.withLock { frames.first?.bytes } }
}
