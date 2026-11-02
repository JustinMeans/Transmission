import Foundation
import Testing
import TransmissionTestSupport
@testable import TransmissionLive

/// Drives a real hub through an in-memory transport.
@Suite("LiveClient against a hub")
struct LiveClientHubTests {
    @Test("subscribe receives the snapshot then deltas, marked live")
    func basics() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: ["a": 1])
        let client = LiveClient(configuration: clientConfiguration(), transport: LoopbackTransport(hub: hub))
        await client.start()
        let log = UpdateLog(await client.subscribe(statsTopic))
        await eventually { log.last?.state == ["a": 1] }
        #expect(log.last?.freshness == .live)
        try await hub.publish(statsTopic, ["b": 2])
        await eventually { log.last?.state == ["a": 1, "b": 2] }
        #expect(log.last?.seq == 1)
        await client.stop()
    }

    @Test("a subscription made before start is issued on connect")
    func subscribeBeforeStart() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: ["a": 1])
        let client = LiveClient(configuration: clientConfiguration(), transport: LoopbackTransport(hub: hub))
        let log = UpdateLog(await client.subscribe(statsTopic))
        await client.start()
        await eventually { log.last?.state == ["a": 1] }
        #expect(log.last?.freshness == .live)
        await client.stop()
    }

    @Test("start is idempotent")
    func startTwice() async throws {
        let hub = hub()
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await client.start()
        try await Task.sleep(for: .milliseconds(30))
        #expect(transport.attempts == 1)
        await client.stop()
    }

    @Test("a reconnect resumes from the cursor with no snapshot, and misses nothing")
    func reconnectResumes() async throws {
        let hub = hub()
        let instance = try await hub.host(logTopic, initial: [])
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        let log = UpdateLog(await client.subscribe(logTopic))
        try await hub.publish(logTopic, "one")
        await eventually { log.last?.state == ["one"] }

        await transport.latest?.close(code: .deadPeer)
        await eventually {
            if case .stale? = log.last?.freshness { return true }
            return false
        }
        try await hub.publish(logTopic, "two")
        try await hub.publish(logTopic, "three")
        await eventually { log.last?.state == ["one", "two", "three"] }
        #expect(log.last?.freshness == .live)

        let sub = transport.latest?.sentFrames.first { if case .sub = $0 { true } else { false } }
        guard case let .sub(_, _, _, _, _, resume)? = sub else {
            Issue.record("no SUB")
            return
        }
        #expect(resume == LiveCursor(epoch: instance.ring.epoch, seq: 1))
        await client.stop()
    }

    @Test("a quiet topic reports live after a resume with nothing to replay")
    func quietResume() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: ["a": 1])
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        let log = UpdateLog(await client.subscribe(statsTopic))
        await eventually { log.last?.freshness == .live }
        await transport.latest?.close(code: .deadPeer)
        await eventually { transport.attempts == 2 }
        let live = await eventually {
            guard let last = log.all.last else { return false }
            return last.freshness == .live && transport.attempts == 2
        }
        #expect(live)
        #expect(log.all.contains { $0.freshness == .resyncing })
        await client.stop()
    }

    @Test("a server restart (new epoch) gets a full snapshot")
    func serverRestart() async throws {
        let first = hub()
        try await first.host(statsTopic, initial: ["old": 1])
        let transport = LoopbackTransport(hub: first)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        let second = hub()
        try await second.host(statsTopic, initial: ["new": 2])
        let router = RoutingTransport(transport)
        let restarted = LiveClient(configuration: clientConfiguration(), transport: router)
        await restarted.start()
        let log = UpdateLog(await restarted.subscribe(statsTopic))
        await eventually { log.last?.state == ["old": 1] }
        router.next = LoopbackTransport(hub: second)
        await transport.latest?.close(code: .normal)
        await eventually { log.last?.state == ["new": 2] }
        #expect(log.last?.freshness == .live)
        await restarted.stop()
        _ = client
    }

    @Test("a slow consumer close heals itself")
    func slowConsumerHeals() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        let log = UpdateLog(await client.subscribe(statsTopic))
        await eventually { log.last?.freshness == .live }
        await transport.latest?.close(code: .slowConsumer)
        try await hub.publish(statsTopic, ["a": 1])
        await eventually { log.last?.state == ["a": 1] }
        #expect(log.last?.freshness == .live)
        await client.stop()
    }

    @Test("pause closes and keeps cursors; resume reconnects at once and catches up")
    func pauseAndResume() async throws {
        let hub = hub()
        try await hub.host(logTopic, initial: [])
        let transport = LoopbackTransport(hub: hub)
        var configuration = clientConfiguration()
        configuration.reconnect = LiveReconnectPolicy(minimumDelay: .seconds(60), maximumDelay: .seconds(60))
        let client = LiveClient(configuration: configuration, transport: transport)
        await client.start()
        let log = UpdateLog(await client.subscribe(logTopic))
        try await hub.publish(logTopic, "one")
        await eventually { log.last?.state == ["one"] }

        await client.pause()
        #expect(await client.state == .paused)
        await eventually { await hub.connectionCount == 0 }
        await eventually {
            if case .stale? = log.last?.freshness { return true }
            return false
        }
        try await Task.sleep(for: .milliseconds(30))
        #expect(transport.attempts == 1, "paused means no reconnect")

        try await hub.publish(logTopic, "two")
        await client.resume()
        await eventually { log.last?.state == ["one", "two"] }
        #expect(log.last?.freshness == .live)
        #expect(transport.attempts == 2)
        await client.stop()
    }

    @Test("a per-subscription pause stops one stream and leaves the socket up")
    func subscriptionPause() async throws {
        let hub = hub()
        try await hub.host(logTopic, initial: [])
        let client = LiveClient(configuration: clientConfiguration(), transport: LoopbackTransport(hub: hub))
        await client.start()
        let subscription = await client.subscribe(logTopic)
        let log = UpdateLog(subscription)
        await eventually { log.last?.freshness == .live }
        await subscription.pause()
        try await Task.sleep(for: .milliseconds(30))
        try await hub.publish(logTopic, "held")
        try await Task.sleep(for: .milliseconds(30))
        #expect(log.last?.state == [])
        await subscription.resume()
        await eventually { log.last?.state == ["held"] }
        await client.stop()
    }

    @Test("cancelling a subscription ends its stream and releases the server side")
    func cancel() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let client = LiveClient(configuration: clientConfiguration(), transport: LoopbackTransport(hub: hub))
        await client.start()
        let subscription = await client.subscribe(statsTopic)
        let log = UpdateLog(subscription)
        await eventually { log.last != nil }
        await subscription.cancel()
        await eventually { await hub.subscriberCount(statsTopic.key) == 0 }
        #expect(await hub.subscriberCount(statsTopic.key) == 0)
        await client.stop()
    }

    @Test("a log client sees dropped lines rather than silence")
    func gapSurfaced() async throws {
        var configuration = fastConfiguration()
        configuration.mailboxCapacity = 2
        let hub = hub(configuration)
        try await hub.host(logTopic, initial: [])
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        let subscription = await client.subscribe(logTopic)
        let log = UpdateLog(subscription)
        await eventually { log.last?.freshness == .live }
        await subscription.pause()
        try await Task.sleep(for: .milliseconds(30))
        for index in 1...10 { try await hub.publish(logTopic, "line \(index)") }
        await subscription.resume()
        await eventually { log.last?.state.last == "line 10" }
        #expect(log.all.map(\.dropped).reduce(0, +) == 8)
        await client.stop()
    }

    @Test("a token provider runs for every attempt")
    func tokens() async throws {
        let hub = hub()
        let transport = LoopbackTransport(hub: hub)
        var configuration = clientConfiguration()
        let counter = Sequence1()
        configuration.tokenProvider = { "t\(counter.next())" }
        let client = LiveClient(configuration: configuration, transport: transport)
        await client.start()
        await eventually { transport.attempts == 1 }
        try await Task.sleep(for: .milliseconds(20))
        await transport.latest?.close(code: .authExpired)
        await eventually { transport.attempts == 2 }
        #expect(transport.headers.map { $0["Authorization"] } == ["Bearer t1", "Bearer t2"])
        await client.stop()
    }

    @Test("refreshToken sends AUTH_REFRESH")
    func refresh() async throws {
        let hub = LiveHub(configuration: fastConfiguration(), authorizer: Rules())
        let transport = LoopbackTransport(hub: hub)
        transport.identity = LiveIdentity(subject: "alice")
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { await client.state == .connected }
        await client.refreshToken("alice")
        await eventually { transport.latest?.sentFrames.contains(.authRefresh(token: "alice")) == true }
        #expect(transport.latest?.sentFrames.contains(.authRefresh(token: "alice")) == true)
        await client.stop()
    }

    @Test("a refused connection backs off rather than spinning")
    func backoff() async throws {
        let hub = hub()
        let transport = LoopbackTransport(hub: hub)
        transport.refuse = true
        var configuration = clientConfiguration()
        configuration.reconnect = LiveReconnectPolicy(minimumDelay: .milliseconds(50), maximumDelay: .seconds(1))
        let client = LiveClient(configuration: configuration, transport: transport)
        await client.start()
        try await Task.sleep(for: .milliseconds(400))
        #expect(transport.attempts <= 6, "got \(transport.attempts)")
        #expect(transport.attempts >= 2)
        await client.stop()
    }

    @Test("reconnectNow skips the wait")
    func reconnectNow() async throws {
        let hub = hub()
        let transport = LoopbackTransport(hub: hub)
        transport.refuse = true
        var configuration = clientConfiguration()
        configuration.reconnect = LiveReconnectPolicy(minimumDelay: .seconds(60), maximumDelay: .seconds(60))
        let client = LiveClient(configuration: configuration, transport: transport)
        await client.start()
        await eventually { await client.state == .waitingToRetry }
        #expect(transport.attempts == 1)
        transport.refuse = false
        await client.reconnectNow()
        await eventually { await client.state == .connected }
        #expect(await client.state == .connected)
        await client.stop()
    }

    @Test("reconnectNow(dropExisting:) replaces a live socket")
    func reconnectDropsSocket() async throws {
        let hub = hub()
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { await client.state == .connected }
        await client.reconnectNow(dropExisting: true)
        await eventually { transport.attempts == 2 }
        #expect(transport.attempts == 2)
        await client.stop()
    }

    @Test("stop finishes every stream and stays down")
    func stop() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        let subscription = await client.subscribe(statsTopic)
        await client.stop()
        let finished = Task {
            for await _ in subscription.updates {}
            return true
        }
        #expect(try await withDeadline(.seconds(2), "stream finish") { await finished.value })
        try await Task.sleep(for: .milliseconds(30))
        #expect(transport.attempts <= 1)
        #expect(await client.state == .stopped)
    }

    @Test("a heartbeat from the hub is echoed, so the session survives")
    func heartbeatEcho() async throws {
        var configuration = fastConfiguration()
        configuration.heartbeat = .milliseconds(200)
        let hub = hub(configuration)
        let transport = LoopbackTransport(hub: hub)
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { await client.state == .connected }
        try await Task.sleep(for: .milliseconds(1000))
        #expect(transport.attempts == 1)
        await client.stop()
    }

    @Test("a large snapshot in parts reassembles on the client")
    func snapshotParts() async throws {
        var configuration = fastConfiguration()
        configuration.maxFrame = 256
        let hub = hub(configuration)
        var large: Stats = [:]
        for index in 0..<200 { large["key-\(index)"] = Double(index) }
        try await hub.host(statsTopic, initial: large)
        let client = LiveClient(configuration: clientConfiguration(), transport: LoopbackTransport(hub: hub))
        await client.start()
        let log = UpdateLog(await client.subscribe(statsTopic))
        await eventually { log.last?.state.count == 200 }
        #expect(log.last?.state == large)
        await client.stop()
    }
}

/// Protocol rules a real hub never breaks, driven by a scripted server.
@Suite("LiveClient rules")
struct LiveClientRuleTests {
    private func connected() async -> (LiveClient, ScriptedTransport, UpdateLog<Stats, Stats>) {
        let transport = ScriptedTransport()
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        let log = UpdateLog(await client.subscribe(statsTopic))
        await client.start()
        await eventually { transport.sent.count >= 1 }
        transport.welcome()
        await eventually { !transport.subs.isEmpty }
        transport.server(.subAck(subID: 1, epoch: 7, headSeq: 5, schema: 1, replay: false))
        transport.server(.snapshot(subID: 1, epoch: 7, seq: 5, schema: 1, part: 0, last: true, payload: encoded(["a": 1.0])))
        await eventually { log.last?.seq == 5 }
        return (client, transport, log)
    }

    private func delta(_ seq: UInt64, span: UInt64 = 1, _ state: Stats) -> LiveFrame {
        .delta(subID: 1, seq: seq, sampleTimeMs: 0, span: span, payload: encoded(state))
    }

    @Test("HELLO is the first frame and carries identity and version range")
    func hello() async {
        let (client, transport, _) = await connected()
        guard case let .hello(protoMin, protoMax, build, _)? = transport.sent.first else {
            Issue.record("HELLO must come first")
            return
        }
        #expect(protoMin == 1)
        #expect(protoMax == 1)
        #expect(build == "test")
        await client.stop()
    }

    @Test("a delta at lastSeq + 1 applies")
    func contiguous() async {
        let (client, transport, log) = await connected()
        transport.server(delta(6, ["b": 2]))
        await eventually { log.last?.seq == 6 }
        #expect(log.last?.state == ["a": 1, "b": 2])
        #expect(log.last?.delta == ["b": 2])
        await client.stop()
    }

    @Test("a coalesced delta applies when seq minus span equals lastSeq")
    func coalesced() async {
        let (client, transport, log) = await connected()
        transport.server(delta(9, span: 4, ["b": 2]))
        await eventually { log.last?.seq == 9 }
        #expect(transport.subs.count == 1)
        await client.stop()
    }

    @Test("duplicates are ignored")
    func duplicates() async throws {
        let (client, transport, log) = await connected()
        transport.server(delta(5, ["a": 99]))
        transport.server(delta(4, ["a": 98]))
        try await Task.sleep(for: .milliseconds(30))
        #expect(log.last?.state == ["a": 1])
        #expect(transport.subs.count == 1)
        await client.stop()
    }

    @Test("a hole re-issues SUB with the last good cursor")
    func hole() async {
        let (client, transport, log) = await connected()
        transport.server(delta(8, ["b": 2]))
        await eventually { transport.subs.count == 2 }
        guard case let .sub(subID, _, _, _, _, resume)? = transport.subs.last else {
            Issue.record("expected a second SUB")
            return
        }
        #expect(subID == 1)
        #expect(resume == LiveCursor(epoch: 7, seq: 5))
        #expect(log.last?.state == ["a": 1], "the out-of-sequence delta was not applied")
        await eventually { log.last?.freshness == .resyncing }
        #expect(log.last?.freshness == .resyncing)
        await client.stop()
    }

    @Test("deltas that race a re-SUB are ignored until SUBACK, then replay applies")
    func resyncRace() async throws {
        let (client, transport, log) = await connected()
        transport.server(delta(8, ["b": 2]))
        await eventually { transport.subs.count == 2 }
        transport.server(delta(9, ["c": 3]))
        try await Task.sleep(for: .milliseconds(20))
        #expect(transport.subs.count == 2, "must not storm")
        transport.server(.subAck(subID: 1, epoch: 7, headSeq: 6, schema: 1, replay: true))
        transport.server(delta(6, ["d": 4]))
        await eventually { log.last?.seq == 6 }
        #expect(log.last?.state == ["a": 1, "d": 4])
        #expect(log.last?.freshness == .live)
        await client.stop()
    }

    @Test("a state topic treats GAP as a hole")
    func gapOnState() async {
        let (client, transport, _) = await connected()
        transport.server(.gap(subID: 1, fromSeq: 6, toSeq: 8, droppedCount: 3, reason: .overflow))
        await eventually { transport.subs.count == 2 }
        #expect(transport.subs.count == 2)
        await client.stop()
    }

    @Test("a snapshot part out of order re-subscribes")
    func partOrder() async {
        let (client, transport, _) = await connected()
        transport.server(.snapshot(subID: 1, epoch: 7, seq: 9, schema: 1, part: 3, last: true, payload: Data()))
        await eventually { transport.subs.count == 2 }
        #expect(transport.subs.count == 2)
        await client.stop()
    }

    @Test("a span larger than seq is a hole, not a crash")
    func hostileSpan() async {
        let (client, transport, _) = await connected()
        transport.server(delta(6, span: 100, ["x": 1]))
        await eventually { transport.subs.count == 2 }
        #expect(transport.subs.count == 2)
        await client.stop()
    }

    @Test("a new epoch in a snapshot replaces the cursor")
    func newEpoch() async {
        let (client, transport, log) = await connected()
        transport.server(.snapshot(subID: 1, epoch: 8, seq: 2, schema: 1, part: 0, last: true, payload: encoded(["z": 1.0])))
        await eventually { log.last?.seq == 2 }
        #expect(log.last?.state == ["z": 1])
        await client.stop()
    }

    @Test("SUBACK alone keeps the old epoch in the cursor until the snapshot lands")
    func epochOnlyWithSnapshot() async {
        let (client, transport, _) = await connected()
        transport.server(.subAck(subID: 1, epoch: 99, headSeq: 0, schema: 1, replay: false))
        transport.serverClose(nil)
        await eventually { transport.connects == 2 }
        await eventually { !transport.sent.isEmpty }
        transport.welcome()
        await eventually { transport.subs.count == 2 }
        guard case let .sub(_, _, _, _, _, resume)? = transport.subs.last else {
            Issue.record("expected a second SUB")
            return
        }
        #expect(resume == LiveCursor(epoch: 7, seq: 5))
        await client.stop()
    }

    @Test("unknown frames are ignored")
    func unknownFrame() async throws {
        let (client, transport, log) = await connected()
        transport.serverRaw([0x7F, 0, 0, 1, 2, 3])
        transport.server(delta(6, ["b": 2]))
        await eventually { log.last?.seq == 6 }
        #expect(log.last?.seq == 6)
        await client.stop()
    }

    @Test("update-required stops retrying and says so")
    func updateRequired() async throws {
        let transport = ScriptedTransport()
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { !transport.sent.isEmpty }
        transport.server(.error(subID: 0, code: .updateRequired, retryable: false, message: "update"))
        await eventually { await client.state == .updateRequired }
        try await Task.sleep(for: .milliseconds(40))
        #expect(transport.connects == 1)
        #expect(await client.state == .updateRequired)
    }

    @Test("close code 4001 is also terminal")
    func updateRequiredByClose() async throws {
        let transport = ScriptedTransport()
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { !transport.sent.isEmpty }
        transport.serverClose(.updateRequired)
        await eventually { await client.state == .updateRequired }
        #expect(await client.state == .updateRequired)
    }

    @Test("superseded stops retrying, so two installs cannot fight")
    func superseded() async throws {
        let (client, transport, _) = await connected()
        transport.serverClose(.superseded)
        await eventually { await client.state == .superseded }
        try await Task.sleep(for: .milliseconds(40))
        #expect(transport.connects == 1)
    }

    @Test("silence past two heartbeats drops the socket and reconnects")
    func watchdog() async {
        let transport = ScriptedTransport()
        let client = LiveClient(configuration: clientConfiguration(), transport: transport)
        await client.start()
        await eventually { !transport.sent.isEmpty }
        transport.welcome(heartbeatMs: 20)
        let reconnected = await eventually(within: .seconds(2)) { transport.connects >= 2 }
        #expect(reconnected)
        await client.stop()
    }

    @Test("AUTH_EXPIRING fetches a fresh token and sends AUTH_REFRESH")
    func authExpiring() async {
        let transport = ScriptedTransport()
        var configuration = clientConfiguration()
        configuration.tokenProvider = { "fresh" }
        let client = LiveClient(configuration: configuration, transport: transport)
        await client.start()
        await eventually { !transport.sent.isEmpty }
        transport.welcome()
        transport.server(.authExpiring(secondsLeft: 30))
        await eventually { transport.sent.contains(.authRefresh(token: "fresh")) }
        #expect(transport.sent.contains(.authRefresh(token: "fresh")))
        await client.stop()
    }

    @Test("HB is echoed")
    func heartbeatEcho() async {
        let (client, transport, _) = await connected()
        transport.server(.heartbeat(serverTimeMs: 42))
        await eventually { transport.sent.contains(.heartbeat(serverTimeMs: 42)) }
        #expect(transport.sent.contains(.heartbeat(serverTimeMs: 42)))
        await client.stop()
    }

    @Test("ACK goes out periodically")
    func acks() async {
        let transport = ScriptedTransport()
        var configuration = clientConfiguration()
        configuration.ackEvery = 2
        let client = LiveClient(configuration: configuration, transport: transport)
        let log = UpdateLog(await client.subscribe(statsTopic))
        await client.start()
        await eventually { !transport.sent.isEmpty }
        transport.welcome()
        await eventually { !transport.subs.isEmpty }
        transport.server(.subAck(subID: 1, epoch: 7, headSeq: 0, schema: 1, replay: false))
        transport.server(.snapshot(subID: 1, epoch: 7, seq: 0, schema: 1, part: 0, last: true, payload: encoded(Stats())))
        for seq in 1...2 as ClosedRange<UInt64> {
            transport.server(.delta(subID: 1, seq: seq, sampleTimeMs: 0, span: 1, payload: encoded(["a": 1.0])))
        }
        await eventually { transport.sent.contains(.ack(subID: 1, seq: 2)) }
        #expect(transport.sent.contains(.ack(subID: 1, seq: 2)))
        _ = log
        await client.stop()
    }
}

final class Sequence1: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0
    func next() -> Int { lock.withLock { value += 1; return value } }
}

/// Switches the hub behind a client mid-test, as a deploy would.
final class RoutingTransport: LiveTransport, @unchecked Sendable {
    private let lock = NSLock()
    private var current: any LiveTransport
    var next: (any LiveTransport)? {
        get { nil }
        set { if let newValue { lock.withLock { current = newValue } } }
    }

    init(_ initial: any LiveTransport) { current = initial }

    func connect(_ request: LiveConnectRequest) async throws -> any LiveTransportConnection {
        try await lock.withLock { current }.connect(request)
    }
}
