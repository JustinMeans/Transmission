#if !canImport(FoundationNetworking)
import Foundation
import Testing
import TransmissionLive
import TransmissionLiveVapor
import TransmissionTestSupport
import Vapor
@testable import TransmissionLive

/// Real Vapor, real sockets, real `URLSessionWebSocketTask`.
func withLiveServer<T: Sendable>(
    configuration: LiveHubConfiguration = fastConfiguration(),
    setup: @Sendable (LiveHub) async throws -> Void,
    _ body: @Sendable (LiveHub, Int) async throws -> T
) async throws -> T {
    let app = try await Application.make(Environment(name: "testing", arguments: ["vapor"]))
    app.logger.logLevel = .critical
    app.http.server.configuration.hostname = "127.0.0.1"
    app.http.server.configuration.port = 0
    app.http.server.configuration.shutdownTimeout = .milliseconds(100)
    let hub = LiveHub(configuration: configuration, authorizer: Rules())
    try await setup(hub)
    app.live.mount(hub: hub) { _, token in
        guard let token, token != "bad" else { return nil }
        return LiveIdentity(subject: token)
    }
    do {
        try await app.startup()
        let result = try await body(hub, app.http.server.shared.localAddress!.port!)
        await hub.closeAll()
        try await app.asyncShutdown()
        return result
    } catch {
        await hub.closeAll()
        try? await app.asyncShutdown()
        throw error
    }
}

func socketClient(port: Int, token: String? = "good", id: String = UUID().uuidString) -> LiveClient {
    var configuration = LiveClientConfiguration(
        url: URL(string: "ws://127.0.0.1:\(port)/v3/live")!, clientID: id, build: "test",
        tokenProvider: { token })
    configuration.reconnect = LiveReconnectPolicy(minimumDelay: .milliseconds(20), maximumDelay: .milliseconds(200))
    return LiveClient(configuration: configuration, transport: URLSessionLiveTransport())
}

@Suite("TransmissionLiveVapor end to end", .serialized)
struct LiveVaporTests {
    @Test("subscribe over a real socket: snapshot, then deltas")
    func realSocket() async throws {
        try await withLiveServer(setup: { try await $0.host(statsTopic, initial: ["a": 1]) }) { hub, port in
            let client = socketClient(port: port)
            await client.start()
            let log = UpdateLog(await client.subscribe(statsTopic))
            await eventually { log.last?.state == ["a": 1] }
            #expect(log.last?.freshness == .live)
            try await hub.publish(statsTopic, ["b": 2])
            await eventually { log.last?.state == ["a": 1, "b": 2] }
            #expect(log.last?.state == ["a": 1, "b": 2])
            await client.stop()
        }
    }

    @Test("a bad token is refused with 401 before any socket exists")
    func rejected() async throws {
        try await withLiveServer(setup: { _ in }) { hub, port in
            let transport = URLSessionLiveTransport()
            let request = LiveConnectRequest(
                url: URL(string: "ws://127.0.0.1:\(port)/v3/live")!,
                headers: ["Authorization": "Bearer bad"],
                subprotocols: [LiveProtocol.subprotocol], maxMessageSize: 1 << 20)
            await #expect(throws: LiveTransportError.rejected(status: 401)) {
                _ = try await transport.connect(request)
            }
            #expect(await hub.connectionCount == 0)
        }
    }

    @Test("no token at all is refused with 401")
    func anonymous() async throws {
        try await withLiveServer(setup: { _ in }) { hub, port in
            let raw = try RawSocketClient(
                port: port, path: "/v3/live",
                headers: ["Sec-WebSocket-Protocol": LiveProtocol.subprotocol])
            defer { raw.shutdown() }
            #expect(raw.responseHead.contains("401"))
            #expect(await hub.connectionCount == 0)
        }
    }

    @Test("a client that does not offer tx-live.1 is refused with 400")
    func subprotocolRequired() async throws {
        try await withLiveServer(setup: { _ in }) { _, port in
            let raw = try RawSocketClient(port: port, path: "/v3/live", headers: ["Authorization": "Bearer good"])
            defer { raw.shutdown() }
            #expect(raw.responseHead.contains("400"))
        }
    }

    @Test("the upgrade echoes the subprotocol")
    func subprotocolEcho() async throws {
        try await withLiveServer(setup: { _ in }) { _, port in
            let raw = try RawSocketClient(port: port, path: "/v3/live", headers: [
                "Authorization": "Bearer good",
                "Sec-WebSocket-Protocol": "other, \(LiveProtocol.subprotocol)",
            ])
            defer { raw.shutdown() }
            #expect(raw.responseHead.contains("101"))
            #expect(raw.responseHead.lowercased().contains("sec-websocket-protocol: \(LiveProtocol.subprotocol)"))
        }
    }

    @Test("a server drop resumes from the cursor and catches up")
    func deployShape() async throws {
        try await withLiveServer(setup: { try await $0.host(logTopic, initial: []) }) { hub, port in
            let client = socketClient(port: port)
            await client.start()
            let log = UpdateLog(await client.subscribe(logTopic))
            try await hub.publish(logTopic, "before")
            await eventually { log.last?.state == ["before"] }
            await hub.closeAll()
            try await hub.publish(logTopic, "during")
            await eventually { log.last?.state == ["before", "during"] }
            #expect(log.last?.state == ["before", "during"])
            #expect(log.last?.freshness == .live)
            await client.stop()
        }
    }

    @Test("a message over URLSession's 1 MiB default arrives whole")
    func largeMessage() async throws {
        var configuration = fastConfiguration()
        configuration.maxFrame = 8 << 20
        let huge = ["k": String(repeating: "x", count: 3 << 20)]
        let topic = Topic<[String: String], [String: String]>(name: "huge", kind: .state, apply: { $0 = $1 })
        try await withLiveServer(configuration: configuration, setup: { try await $0.host(topic, initial: huge) }) { _, port in
            let client = socketClient(port: port)
            await client.start()
            let log = UpdateLog(await client.subscribe(topic))
            await eventually(within: .seconds(10)) { log.last?.state["k"]?.count == 3 << 20 }
            #expect(log.last?.state["k"]?.count == 3 << 20)
            await client.stop()
        }
    }

    @Test("500 deltas stay in order over a real socket")
    func ordering() async throws {
        try await withLiveServer(setup: { try await $0.host(logTopic, initial: []) }) { hub, port in
            let client = socketClient(port: port)
            await client.start()
            let log = UpdateLog(await client.subscribe(logTopic))
            await eventually { log.last?.freshness == .live }
            for index in 0..<500 { try await hub.publish(logTopic, "\(index)") }
            await eventually(within: .seconds(10)) { log.last?.state.count == 500 }
            #expect(log.last?.state == (0..<500).map(String.init))
            await client.stop()
        }
    }

    @Test("a peer that stopped reading is dropped by the socket, not just forgotten")
    func stalledSocket() async throws {
        var configuration = fastConfiguration()
        configuration.stallLimit = .milliseconds(300)
        configuration.maxFrame = 4 << 20
        let padded = Topic<Tick, Tick>(name: "padded", kind: .log, apply: { $0 = $1 })
        try await withLiveServer(
            configuration: configuration,
            setup: { try await $0.host(padded, initial: Tick(sentAt: 0)) }
        ) { hub, port in
            let stalled = try stalledReader(port: port, topic: padded)
            defer { stalled.shutdown() }
            await eventually { await hub.subscriberCount(padded.key) == 1 }
            let chunk = String(repeating: "x", count: 1 << 18)
            for _ in 0..<200 {
                try await hub.publish(padded, Tick(sentAt: 1, padding: chunk))
                try await Task.sleep(for: .milliseconds(5))
                if await hub.connectionCount == 0 { break }
            }
            let dropped = await eventually(within: .seconds(5)) { await hub.connectionCount == 0 }
            #expect(dropped)
            // The socket really closes, even though the Close frame can't get through.
            let closed = await eventually(within: .seconds(5)) { stalled.peerClosed }
            #expect(closed)
        }
    }

    @Test("100 real sockets: stalled and flapping peers do not move healthy latency")
    func loadOverSockets() async throws {
        var configuration = fastConfiguration()
        configuration.stallLimit = .milliseconds(300)
        configuration.maxFrame = 4 << 20
        let padding = String(repeating: "x", count: 16 << 10)
        try await withLiveServer(
            configuration: configuration,
            setup: { try await $0.host(tickTopic, initial: Tick(sentAt: 0)) }
        ) { hub, port in
            let samples = Samples()
            var healthy: [LiveClient] = []
            var readers: [Task<Void, Never>] = []
            for _ in 0..<70 {
                let client = socketClient(port: port)
                await client.start()
                readers.append(measure(await client.subscribe(tickTopic), into: samples))
                healthy.append(client)
            }
            let reachedAll = await eventually(within: .seconds(20)) { await hub.subscriberCount(tickTopic.key) == 70 }
            #expect(reachedAll)
            let clock = ContinuousClock()
            func publish(for duration: Duration) async throws {
                let deadline = clock.now.advanced(by: duration)
                var next = clock.now
                while clock.now < deadline {
                    try await hub.publish(tickTopic, Tick(sentAt: nowNanos(), padding: padding))
                    next = next.advanced(by: .milliseconds(50))
                    try await clock.sleep(until: next)
                }
            }

            try await publish(for: .seconds(3))
            try await Task.sleep(for: .milliseconds(100))
            let baseline = LoadPhase(samples.take())

            // Stalled alone: this is the property the hub promises. They cost no
            // CPU, so any movement here would be real coupling.
            var stalled: [RawSocketClient] = []
            for _ in 0..<15 { stalled.append(try stalledReader(port: port, topic: tickTopic)) }
            await eventually { await hub.subscriberCount(tickTopic.key) == 85 }
            _ = samples.take()
            try await publish(for: .seconds(3))
            try await Task.sleep(for: .milliseconds(100))
            let stalledOnly = LoadPhase(samples.take())

            let flaps = Counter()
            // Real threads: the sockets block, which must not eat the cooperative
            // pool the healthy clients and the server run on.
            let stopFlapping = Counter()
            for _ in 0..<15 {
                Thread.detachNewThread {
                    while stopFlapping.value == 0 {
                        if let raw = try? flappingReader(port: port, topic: tickTopic) {
                            usleep(UInt32.random(in: 50_000...150_000))
                            raw.shutdown()
                            flaps.increment()
                        }
                    }
                }
            }
            try await Task.sleep(for: .milliseconds(200))
            _ = samples.take()
            try await publish(for: .seconds(3))
            try await Task.sleep(for: .milliseconds(100))
            let adversarial = LoadPhase(samples.take())
            stopFlapping.increment()

            print("LOAD sockets baseline p50=\(baseline.p50)ms p99=\(baseline.p99)ms n=\(baseline.count)")
            print("LOAD sockets stalled-only p50=\(stalledOnly.p50)ms p99=\(stalledOnly.p99)ms n=\(stalledOnly.count)")
            print("LOAD sockets stalled+flapping p50=\(adversarial.p50)ms p99=\(adversarial.p99)ms n=\(adversarial.count) flaps=\(flaps.value)")
            let settled = await eventually(within: .seconds(5)) { await hub.connectionCount == 70 }
            let remaining = await hub.connectionCount
            #expect(settled, "stalled sockets were dropped: \(remaining)")
            for client in healthy { #expect(await client.state == .connected) }
            #expect(adversarial.count > Int(Double(baseline.count) * 0.9), "healthy clients lost updates")
            #expect(flaps.value >= 15)
            #expect(stalledOnly.count > Int(Double(baseline.count) * 0.9), "healthy clients lost updates")
            // Run-to-run p99 varies about threefold on a debug build, hence the margin.
            #expect(stalledOnly.p99 <= max(baseline.p99 * 3, baseline.p99 + 10),
                    "stalled peers moved p99: \(baseline.p99) -> \(stalledOnly.p99)")
            // Flappers are raw sockets on purpose. As URLSession clients in this same
            // process, their session churn pushed p99 past 300 ms on the client side,
            // which says nothing about the hub.
            #expect(adversarial.p99 <= max(baseline.p99 * 4, baseline.p99 + 25),
                    "p99 moved: \(baseline.p99) -> \(adversarial.p99)")
            for client in healthy { await client.stop() }
            stalled.forEach { $0.shutdown() }
            readers.forEach { $0.cancel() }
        }
    }
}

/// A full handshake and subscribe, read like a client would.
func flappingReader<S, D>(port: Int, topic: Topic<S, D>) throws -> RawSocketClient {
    try stalledReader(port: port, topic: topic, receiveBuffer: nil)
}

/// Upgrades, says HELLO and SUB, then never reads again.
func stalledReader<S, D>(port: Int, topic: Topic<S, D>, receiveBuffer: Int32? = 4096) throws -> RawSocketClient {
    let raw = try RawSocketClient(
        port: port, path: "/v3/live",
        headers: ["Authorization": "Bearer stalled", "Sec-WebSocket-Protocol": LiveProtocol.subprotocol],
        receiveBuffer: receiveBuffer)
    raw.sendBinary(Array(LiveCodec.encode(.hello(protoMin: 1, protoMax: 1, build: "stalled", clientID: UUID().uuidString))))
    raw.sendBinary(Array(LiveCodec.encode(.sub(
        subID: 1, topic: topic.name, params: topic.params, schema: UInt64(topic.schema), maxMilliHz: 0, resume: nil))))
    return raw
}
#endif
