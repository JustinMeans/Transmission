import Foundation
import Testing
import TransmissionTestSupport
@testable import TransmissionLive

func hub(_ configuration: LiveHubConfiguration = fastConfiguration(), authorizer: any LiveAuthorizer = AllowAll()) -> LiveHub {
    LiveHub(configuration: configuration, authorizer: authorizer)
}

@Suite("LiveHub server behaviour")
struct LiveHubTests {
    @Test("HELLO gets WELCOME listing topics and schema ranges")
    func welcome() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        try await hub.host(logTopic, initial: [])
        let peer = await Peer.connect(hub)
        guard case let .welcome(proto, epoch, _, heartbeatMs, _, topics) = try #require(peer.frames.first) else {
            Issue.record("expected WELCOME")
            return
        }
        #expect(proto == 1)
        #expect(epoch == hub.epoch)
        #expect(heartbeatMs == 25_000)
        #expect(topics.map(\.name) == ["logs", "stats"])
        #expect(topics[0].kind == .log)
    }

    @Test("a client with no overlapping version gets a clear refusal")
    func versionRefused() async {
        let hub = hub()
        let peer = await Peer.connect(hub, hello: false)
        peer.send(.hello(protoMin: 9, protoMax: 10, build: "t", clientID: "c"))
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .updateRequired)
        #expect(peer.errors == [.updateRequired])
    }

    @Test("a client that overlaps at the top of its range is accepted")
    func versionOverlap() async {
        let hub = hub()
        let peer = await Peer.connect(hub, hello: false)
        peer.send(.hello(protoMin: 1, protoMax: 5, build: "t", clientID: "c"))
        await eventually { !peer.frames.isEmpty }
        #expect(peer.closeCode == nil)
        guard case .welcome(proto: 1, _, _, _, _, _)? = peer.frames.first else {
            Issue.record("expected WELCOME v1")
            return
        }
    }

    @Test("anything before HELLO closes the connection")
    func helloRequired() async {
        let hub = hub()
        let peer = await Peer.connect(hub, hello: false)
        peer.send(.ack(subID: 1, seq: 1))
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .protocolError)
    }

    @Test("unknown frames are ignored; a malformed one closes")
    func unknownAndMalformed() async {
        let hub = hub()
        let peer = await Peer.connect(hub)
        peer.connection.receive([0x7F, 0, 0, 1, 2, 3])
        try? await Task.sleep(for: .milliseconds(30))
        #expect(peer.closeCode == nil)
        peer.connection.receive([0x03, 0])
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .protocolError)
    }

    @Test("SUB gets SUBACK and a snapshot; publishes arrive as deltas")
    func snapshotThenDelta() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: ["a": 1])
        try await hub.publish(statsTopic, ["b": 2])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        await eventually { !peer.snapshots.isEmpty }
        let ack = try #require(peer.acks.first)
        #expect(ack.epoch == instance.ring.epoch)
        #expect(ack.head == 1)
        #expect(!ack.replay)
        #expect(try JSONDecoder().decode(Stats.self, from: peer.snapshots[0].payload) == ["a": 1, "b": 2])
        try await hub.publish(statsTopic, ["c": 3])
        await eventually { !peer.deltas.isEmpty }
        #expect(peer.deltas.map(\.seq) == [2])
        #expect(peer.deltas[0].span == 1)
    }

    @Test("SUBACK always precedes data")
    func ackFirst() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        await eventually { peer.frames.count >= 3 }
        guard case .subAck = peer.frames[1], case .snapshot = peer.frames[2] else {
            Issue.record("wrong order: \(peer.frames)")
            return
        }
    }

    @Test("resume in the ring replays deltas with no snapshot")
    func resumeReplay() async throws {
        let hub = hub()
        let instance = try await hub.host(logTopic, initial: [])
        for index in 1...5 { try await hub.publish(logTopic, "line \(index)") }
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 2))
        await eventually { peer.deltas.count == 3 }
        #expect(peer.acks.first?.replay == true)
        #expect(peer.acks.first?.head == 5)
        #expect(peer.snapshots.isEmpty)
        #expect(peer.deltas.map(\.seq) == [3, 4, 5])
    }

    @Test("resume at the head sends SUBACK and nothing else")
    func resumeNothingToDo() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:])
        try await hub.publish(statsTopic, ["a": 1])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 1))
        try await Task.sleep(for: .milliseconds(30))
        #expect(peer.acks.first?.replay == true)
        #expect(peer.deltas.isEmpty)
        #expect(peer.snapshots.isEmpty)
    }

    @Test("resume with another epoch gets a snapshot")
    func resumeWrongEpoch() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: ["a": 1])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, resume: LiveCursor(epoch: 12345, seq: 0))
        await eventually { !peer.snapshots.isEmpty }
        #expect(peer.acks.first?.replay == false)
    }

    @Test("resume ahead of the head gets a snapshot")
    func resumeFuture() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 99))
        await eventually { !peer.snapshots.isEmpty }
        #expect(peer.acks.first?.replay == false)
    }

    @Test("state resume behind the ring gets a snapshot")
    func stateBehindRing() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:], limits: .init(maxEntries: 2, maxBytes: 1 << 20))
        for index in 1...5 { try await hub.publish(statsTopic, ["k\(index)": 1]) }
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 1))
        await eventually { !peer.snapshots.isEmpty }
        #expect(peer.snapshots[0].seq == 5)
    }

    @Test("log resume behind the ring reports a GAP, never a silent loss")
    func logBehindRing() async throws {
        let hub = hub()
        let instance = try await hub.host(logTopic, initial: [], limits: .init(maxEntries: 3, maxBytes: 1 << 20))
        for index in 1...10 { try await hub.publish(logTopic, "line \(index)") }
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 2))
        await eventually { peer.deltas.count == 3 }
        #expect(peer.gaps.count == 1)
        #expect(peer.gaps[0].from == 3)
        #expect(peer.gaps[0].to == 7)
        #expect(peer.gaps[0].dropped == 5)
        #expect(peer.deltas.map(\.seq) == [8, 9, 10])
    }

    @Test("a slow state subscriber gets one merged delta covering the span")
    func stateMerges() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, maxMilliHz: 10)   // one every 100 s after the first
        await eventually { !peer.snapshots.isEmpty }
        try await hub.publish(statsTopic, ["a": 1])
        await eventually { peer.deltas.count == 1 }
        // Rate limited now; these pile up.
        for index in 2...6 { try await hub.publish(statsTopic, ["a": Double(index), "k\(index)": 1]) }
        try await Task.sleep(for: .milliseconds(50))
        #expect(peer.deltas.count == 1)
        let resumePeer = await Peer.connect(hub)
        await resumePeer.subscribe(statsTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 1))
        await eventually { resumePeer.deltas.count == 1 }
        let merged = try #require(resumePeer.deltas.first)
        #expect(merged.seq == 6)
        #expect(merged.span == 5)
        let state = try JSONDecoder().decode(Stats.self, from: merged.payload)
        #expect(state == ["a": 6, "k2": 1, "k3": 1, "k4": 1, "k5": 1, "k6": 1])
    }

    @Test("a state topic without merge falls back to a snapshot")
    func stateWithoutMerge() async throws {
        let hub = hub()
        let instance = try await hub.host(plainTopic, initial: [:])
        for index in 1...4 { try await hub.publish(plainTopic, ["k\(index)": 1]) }
        let peer = await Peer.connect(hub)
        await peer.subscribe(plainTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 1))
        await eventually { !peer.snapshots.isEmpty }
        #expect(peer.deltas.isEmpty)
        #expect(peer.snapshots[0].seq == 4)
    }

    @Test("a series downsamples to the latest point with a span")
    func seriesDownsamples() async throws {
        let hub = hub()
        let instance = try await hub.host(seriesTopic, initial: [])
        for index in 1...5 { try await hub.publish(seriesTopic, Double(index)) }
        let peer = await Peer.connect(hub)
        await peer.subscribe(seriesTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 1))
        await eventually { !peer.deltas.isEmpty }
        #expect(peer.deltas[0].seq == 5)
        #expect(peer.deltas[0].span == 4)
        #expect(try JSONDecoder().decode(Double.self, from: peer.deltas[0].payload) == 5)
    }

    @Test("maxHz caps a state topic's rate and never drops the final value")
    func maxHz() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, maxMilliHz: 10_000)   // 10 Hz
        for index in 0..<100 {
            try await hub.publish(statsTopic, ["v": Double(index)])
            try await Task.sleep(for: .milliseconds(1))
        }
        await eventually { peer.deltas.last?.seq == 100 }
        #expect(peer.deltas.last?.seq == 100)
        #expect(peer.deltas.count < 30, "got \(peer.deltas.count)")
    }

    @Test("log overflow past the mailbox capacity drops oldest with a GAP")
    func logCapacity() async throws {
        var configuration = fastConfiguration()
        configuration.mailboxCapacity = 4
        let hub = hub(configuration)
        let instance = try await hub.host(logTopic, initial: [])
        for index in 1...20 { try await hub.publish(logTopic, "line \(index)") }
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic, resume: LiveCursor(epoch: instance.ring.epoch, seq: 0))
        await eventually { peer.deltas.count == 4 }
        #expect(peer.gaps.first?.dropped == 16)
        #expect(peer.deltas.map(\.seq) == [17, 18, 19, 20])
    }

    @Test("PAUSE stops delivery and RESUME catches up from the cursor")
    func pauseResume() async throws {
        let hub = hub()
        try await hub.host(logTopic, initial: [])
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic)
        peer.send(.pause(subID: 1))
        try await Task.sleep(for: .milliseconds(30))
        for index in 1...3 { try await hub.publish(logTopic, "line \(index)") }
        try await Task.sleep(for: .milliseconds(30))
        #expect(peer.deltas.isEmpty)
        peer.send(.resume(subID: 1))
        await eventually { peer.deltas.count == 3 }
        #expect(peer.deltas.map(\.seq) == [1, 2, 3])
    }

    @Test("UNSUB stops delivery and drops the refcount")
    func unsub() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        #expect(await hub.subscriberCount(statsTopic.key) == 1)
        peer.send(.unsub(subID: 1))
        await eventually { await hub.subscriberCount(statsTopic.key) == 0 }
        try await hub.publish(statsTopic, ["a": 1])
        try await Task.sleep(for: .milliseconds(30))
        #expect(peer.deltas.isEmpty)
    }

    @Test("a repeat SUB on one subID replaces it without leaking a refcount")
    func resubscribe() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        peer.send(.sub(subID: 1, topic: "stats", params: [:], schema: 1, maxMilliHz: 0, resume: nil))
        await eventually { peer.acks.count == 2 }
        #expect(await hub.subscriberCount(statsTopic.key) == 1)
    }

    @Test("closing drops refcounts; the demand handler sees first and last")
    func demandAndRefcounts() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let log = DemandLog()
        await hub.setDemandHandler { _, active in log.add(active) }
        let a = await Peer.connect(hub)
        let b = await Peer.connect(hub)
        await a.subscribe(statsTopic)
        await b.subscribe(statsTopic)
        #expect(await hub.subscriberCount(statsTopic.key) == 2)
        #expect(log.events == [true])
        a.connection.transportClosed()
        b.connection.transportClosed()
        await eventually { await hub.subscriberCount(statsTopic.key) == 0 }
        #expect(log.events == [true, false])
        await eventually { await hub.connectionCount == 0 }
        #expect(await hub.connectionCount == 0)
    }

    @Test("unknown topics and unpublished schemas are refused by name")
    func refusals() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        peer.send(.sub(subID: 1, topic: "nope", params: [:], schema: 1, maxMilliHz: 0, resume: nil))
        peer.send(.sub(subID: 2, topic: "stats", params: [:], schema: 9, maxMilliHz: 0, resume: nil))
        await eventually { peer.errors.count == 2 }
        #expect(peer.errors == [.unknownTopic, .unsupportedSchema])
    }

    @Test("two schemas run side by side with separate sequences")
    func schemaOverlap() async throws {
        let v1 = Topic<Stats, Stats>(name: "m", schema: 1, kind: .state, apply: { _, _ in })
        let v2 = Topic<Stats, Stats>(name: "m", schema: 2, kind: .state, apply: { _, _ in })
        let hub = hub()
        try await hub.host(v1, initial: [:])
        try await hub.host(v2, initial: [:])
        try await hub.publish(v2, ["a": 1])
        let peer = await Peer.connect(hub)
        guard case let .welcome(_, _, _, _, _, topics)? = peer.frames.first else {
            Issue.record("expected WELCOME")
            return
        }
        #expect(topics == [TopicAdvert(name: "m", schemaMin: 1, schemaMax: 2, kind: .state)])
        await peer.subscribe(v1, subID: 1)
        await peer.subscribe(v2, subID: 2)
        await eventually { peer.acks.count == 2 }
        #expect(Set(peer.acks.map(\.head)) == [0, 1])
    }

    @Test("every SUB is authorized")
    func authorization() async throws {
        let hub = hub(authorizer: Rules(denied: ["stats"]))
        try await hub.host(statsTopic, initial: [:])
        try await hub.host(logTopic, initial: [])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic, subID: 1)
        await peer.subscribe(logTopic, subID: 2)
        #expect(peer.errors == [.forbidden])
        #expect(peer.acks.count == 1)
    }

    @Test("a connection with no identity is refused")
    func noIdentity() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub, identity: nil)
        await peer.subscribe(statsTopic)
        #expect(peer.errors == [.forbidden])
    }

    @Test("a cap on subscriptions per connection")
    func subscriptionCap() async throws {
        var configuration = fastConfiguration()
        configuration.maxSubscriptionsPerConnection = 2
        let hub = hub(configuration)
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        for subID in 1...3 as ClosedRange<UInt64> { await peer.subscribe(statsTopic, subID: subID) }
        #expect(peer.errors == [.tooManySubscriptions])
        #expect(peer.connection.subscriptionCount == 2)
    }

    @Test("AUTH_REFRESH with the same subject keeps the session")
    func authRefresh() async throws {
        let hub = hub(authorizer: Rules())
        let peer = await Peer.connect(hub, identity: LiveIdentity(subject: "alice"))
        peer.send(.authRefresh(token: "alice"))
        try await Task.sleep(for: .milliseconds(30))
        #expect(peer.closeCode == nil)
    }

    @Test("AUTH_REFRESH cannot change who the connection belongs to")
    func authRefreshSwap() async throws {
        let hub = hub(authorizer: Rules())
        let peer = await Peer.connect(hub, identity: LiveIdentity(subject: "alice"))
        peer.send(.authRefresh(token: "mallory"))
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .authFailed)
    }

    @Test("a rejected AUTH_REFRESH closes the connection")
    func authRefreshRejected() async throws {
        let hub = hub(authorizer: Rules())
        let peer = await Peer.connect(hub, identity: LiveIdentity(subject: "alice"))
        peer.send(.authRefresh(token: "bad"))
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .authFailed)
        #expect(peer.errors == [.authFailed])
    }

    @Test("failed revalidation ends a live session")
    func revalidation() async throws {
        var configuration = fastConfiguration()
        configuration.revalidateInterval = .milliseconds(20)
        let hub = hub(configuration, authorizer: Rules(revalidates: false))
        let peer = await Peer.connect(hub)
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .authExpired)
    }

    @Test("an expiring identity gets a warning, then a close")
    func expiry() async throws {
        var configuration = fastConfiguration()
        configuration.expiryWarning = .seconds(10)
        let hub = hub(configuration)
        let peer = await Peer.connect(hub, identity: LiveIdentity(subject: "u", expiresAt: Date().addingTimeInterval(0.3)))
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .authExpired)
        #expect(peer.frames.contains { if case .authExpiring = $0 { true } else { false } })
    }

    @Test("a new connection for one client supersedes the old")
    func supersede() async throws {
        let hub = hub()
        let old = await Peer.connect(hub, clientID: "install")
        let new = await Peer.connect(hub, clientID: "install")
        await eventually { old.closeCode != nil }
        #expect(old.closeCode == .superseded)
        #expect(new.closeCode == nil)
    }

    @Test("E7: the old socket's late close leaves the new client in place")
    func staleClose() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let old = await Peer.connect(hub, clientID: "install")
        let new = await Peer.connect(hub, clientID: "install")
        await new.subscribe(statsTopic)
        await eventually { await hub.connectionCount == 1 }
        old.connection.transportClosed()
        try await Task.sleep(for: .milliseconds(20))
        #expect(await hub.connectionCount == 1)
        try await hub.publish(statsTopic, ["a": 1])
        await eventually { !new.deltas.isEmpty }
        #expect(new.deltas.count == 1)
        // The slot still belongs to `new`: a third socket must displace it.
        let third = await Peer.connect(hub, clientID: "install")
        await eventually { new.closeCode != nil }
        #expect(new.closeCode == .superseded)
        _ = third
    }

    @Test("clients of different subjects never supersede each other")
    func supersedeScopedToSubject() async throws {
        let hub = hub()
        let alice = await Peer.connect(hub, identity: LiveIdentity(subject: "alice"), clientID: "same")
        let bob = await Peer.connect(hub, identity: LiveIdentity(subject: "bob"), clientID: "same")
        try await Task.sleep(for: .milliseconds(20))
        #expect(alice.closeCode == nil)
        #expect(bob.closeCode == nil)
    }

    @Test("the server heartbeats, and a silent peer is closed after two intervals")
    func heartbeat() async throws {
        var configuration = fastConfiguration()
        configuration.heartbeat = .milliseconds(250)
        let hub = hub(configuration)
        let peer = await Peer.connect(hub)
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .deadPeer)
        #expect(peer.frames.contains { if case .heartbeat = $0 { true } else { false } })
    }

    @Test("a peer that echoes heartbeats stays up")
    func heartbeatEcho() async throws {
        var configuration = fastConfiguration()
        configuration.heartbeat = .milliseconds(300)
        let hub = hub(configuration)
        let peer = await Peer.connect(hub)
        for _ in 0..<20 {
            peer.send(.heartbeat(serverTimeMs: 0))
            try await Task.sleep(for: .milliseconds(50))
        }
        #expect(peer.closeCode == nil)
    }

    @Test("a socket that dies during start leaves nothing in the hub")
    func closedDuringStart() async throws {
        let hub = hub()
        let peer = Peer()
        peer.connection = LiveConnection(hub: hub, transport: peer, identity: nil)
        peer.connection.transportClosed()
        await peer.connection.start()
        try await Task.sleep(for: .milliseconds(30))
        #expect(await hub.connectionCount == 0)
    }

    @Test("ACK is recorded and advisory")
    func ack() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        peer.send(.ack(subID: 1, seq: 7))
        await eventually { peer.connection.ackedSeq(subID: 1) == 7 }
        #expect(peer.connection.ackedSeq(subID: 1) == 7)
    }

    @Test("CALL gets a failed REPLY until aliases exist")
    func callUnsupported() async {
        let hub = hub()
        let peer = await Peer.connect(hub)
        peer.send(.call(alias: "hub.x.v1", callID: 9, payload: Data()))
        await eventually { peer.frames.count == 2 }
        guard case .reply(callID: 9, isError: true, _)? = peer.frames.last else {
            Issue.record("expected failed REPLY, got \(peer.frames)")
            return
        }
    }

    @Test("a snapshot larger than maxFrame splits and reassembles")
    func snapshotParts() async throws {
        var configuration = fastConfiguration()
        configuration.maxFrame = 256
        let hub = hub(configuration)
        var large: Stats = [:]
        for index in 0..<100 { large["key-\(index)"] = Double(index) }
        try await hub.host(statsTopic, initial: large)
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        await eventually { peer.snapshots.last?.last == true }
        let parts = peer.snapshots
        #expect(parts.count > 1)
        #expect(parts.map(\.part) == Array(0..<UInt64(parts.count)))
        #expect(parts.dropLast().allSatisfy { !$0.last })
        #expect(peer.raw.allSatisfy { $0.byteCount <= 256 })
        let joined = parts.reduce(into: Data()) { $0.append($1.payload) }
        #expect(try JSONDecoder().decode(Stats.self, from: joined) == large)
    }

    @Test("a delta too large for a frame is refused at publish")
    func oversizeDelta() async throws {
        var configuration = fastConfiguration()
        configuration.maxFrame = 128
        let hub = hub(configuration)
        try await hub.host(logTopic, initial: [])
        await #expect(throws: LiveHubError.self) {
            try await hub.publish(logTopic, String(repeating: "x", count: 1000))
        }
    }

    @Test("publishing to a topic nobody hosts is an error; hosting twice is an error")
    func registryErrors() async throws {
        let hub = hub()
        await #expect(throws: LiveHubError.self) { try await hub.publish(statsTopic, [:]) }
        try await hub.host(statsTopic, initial: [:])
        await #expect(throws: LiveHubError.self) { try await hub.host(statsTopic, initial: [:]) }
    }
}

@Suite("Shared encoding")
struct SharedEncodingTests {
    @Test("a delta is encoded once however many subscribers there are")
    func encodeOnce() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:])
        var peers: [Peer] = []
        for _ in 0..<20 {
            let peer = await Peer.connect(hub)
            await peer.subscribe(statsTopic)
            peers.append(peer)
        }
        let afterSnapshot = instance.encodeCount
        #expect(afterSnapshot == 1, "20 subscribers shared one snapshot")
        try await hub.publish(statsTopic, ["a": 1])
        let joined = peers
        await eventually { joined.allSatisfy { $0.deltas.count == 1 } }
        #expect(instance.encodeCount == afterSnapshot)
        let addresses = Set(peers.map { peer -> UInt? in
            peer.raw.last?.body.withUnsafeReadableBytes { UInt(bitPattern: $0.baseAddress) }
        })
        #expect(addresses.count == 1, "every subscriber references the same bytes")
    }

    @Test("subscribers lagging by the same amount share one merged buffer")
    func coalescedOnce() async throws {
        let hub = hub()
        let instance = try await hub.host(statsTopic, initial: [:])
        for index in 1...5 { try await hub.publish(statsTopic, ["k\(index)": 1]) }
        let before = instance.encodeCount
        let cursor = LiveCursor(epoch: instance.ring.epoch, seq: 1)
        var peers: [Peer] = []
        for _ in 0..<10 {
            let peer = await Peer.connect(hub)
            await peer.subscribe(statsTopic, resume: cursor)
            peers.append(peer)
        }
        let joined = peers
        await eventually { joined.allSatisfy { !$0.deltas.isEmpty } }
        #expect(instance.encodeCount == before + 1)
    }

    @Test("a stalled mailbox holds a cursor, not frames")
    func mailboxHoldsNoCopies() async throws {
        let hub = hub()
        let instance = try await hub.host(logTopic, initial: [], limits: .init(maxEntries: 8, maxBytes: 1 << 20))
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic)
        peer.send(.pause(subID: 1))
        try await Task.sleep(for: .milliseconds(20))
        for index in 1...1000 { try await hub.publish(logTopic, "line \(index)") }
        #expect(instance.ring.count == 8)
        #expect(peer.deltas.isEmpty)
    }
}

@Suite("Slow consumers")
struct SlowConsumerTests {
    @Test("a stalled write is closed as a slow consumer and others are untouched")
    func stall() async throws {
        var configuration = fastConfiguration()
        configuration.stallLimit = .milliseconds(100)
        let hub = hub(configuration)
        try await hub.host(statsTopic, initial: [:])
        let stalled = await Peer.connect(hub, stalled: true, hello: false)
        stalled.send(.hello(protoMin: 1, protoMax: 1, build: "t", clientID: "stalled"))
        await eventually { stalled.writes > 0 }
        let healthy = await Peer.connect(hub)
        await healthy.subscribe(statsTopic)
        for index in 1...50 {
            try await hub.publish(statsTopic, ["a": Double(index)])
        }
        await eventually { healthy.deltas.last?.seq == 50 }
        await eventually { stalled.closeCode != nil }
        #expect(stalled.closeCode == .slowConsumer)
        #expect(healthy.closeCode == nil)
        #expect(healthy.deltas.last?.seq == 50)
        await eventually { await hub.connectionCount == 1 }
        #expect(await hub.connectionCount == 1)
    }

    @Test("too many queued bytes closes a slow consumer")
    func queuedBytes() async throws {
        var configuration = fastConfiguration()
        configuration.maxQueuedBytes = 1024
        let hub = hub(configuration)
        try await hub.host(logTopic, initial: [])
        let peer = await Peer.connect(hub)
        await peer.subscribe(logTopic)
        peer.send(.pause(subID: 1))
        try await Task.sleep(for: .milliseconds(20))
        for _ in 0..<40 { try await hub.publish(logTopic, String(repeating: "x", count: 100)) }
        await eventually { peer.closeCode != nil }
        #expect(peer.closeCode == .slowConsumer)
    }

    @Test("a publish never waits for a stalled client")
    func publishNeverBlocks() async throws {
        let hub = hub()
        try await hub.host(statsTopic, initial: [:])
        for _ in 0..<20 {
            let peer = await Peer.connect(hub, stalled: true, hello: false)
            peer.send(.hello(protoMin: 1, protoMax: 1, build: "t", clientID: UUID().uuidString))
        }
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            for index in 0..<1000 { try? await hub.publish(statsTopic, ["a": Double(index)]) }
        }
        #expect(elapsed < .seconds(5))
    }
}

@Suite("LiveBus seam")
struct LiveBusTests {
    final class HoldingBus: LiveBus, @unchecked Sendable {
        private let lock = NSLock()
        private var handlers: [@Sendable (LiveBusMessage) async -> Void] = []
        private var held: [LiveBusMessage] = []

        func publish(_ message: LiveBusMessage) async throws {
            lock.withLock { held.append(message) }
        }

        func subscribe(_ handler: @escaping @Sendable (LiveBusMessage) async -> Void) async {
            lock.withLock { handlers.append(handler) }
        }

        func release() async {
            let (messages, targets) = lock.withLock { () -> ([LiveBusMessage], [@Sendable (LiveBusMessage) async -> Void]) in
                defer { held = [] }
                return (held, handlers)
            }
            for message in messages {
                for target in targets { await target(message) }
            }
        }
    }

    @Test("state only changes when the bus delivers, so the bus owns ordering")
    func busOwnsDelivery() async throws {
        let bus = HoldingBus()
        let hub = LiveHub(configuration: fastConfiguration(), authorizer: AllowAll(), bus: bus)
        let instance = try await hub.host(statsTopic, initial: [:])
        try await hub.publish(statsTopic, ["a": 1])
        #expect(instance.ring.headSeq == 0)
        await bus.release()
        #expect(instance.ring.headSeq == 1)
    }

    @Test("a message from another replica reaches local subscribers")
    func fanIn() async throws {
        let bus = InProcessLiveBus()
        let hub = LiveHub(configuration: fastConfiguration(), authorizer: AllowAll(), bus: bus)
        try await hub.host(statsTopic, initial: [:])
        let peer = await Peer.connect(hub)
        await peer.subscribe(statsTopic)
        try await bus.publish(LiveBusMessage(key: statsTopic.key, sampleTimeMs: 1, payload: encoded(["a": 1.0])))
        await eventually { !peer.deltas.isEmpty }
        #expect(peer.deltas.count == 1)
    }

    @Test("an undecodable bus payload is dropped without advancing the sequence")
    func badPayload() async throws {
        let bus = InProcessLiveBus()
        let hub = LiveHub(configuration: fastConfiguration(), authorizer: AllowAll(), bus: bus)
        let instance = try await hub.host(statsTopic, initial: [:])
        try await bus.publish(LiveBusMessage(key: statsTopic.key, sampleTimeMs: 1, payload: Data("not json".utf8)))
        #expect(instance.ring.headSeq == 0)
    }
}

final class DemandLog: @unchecked Sendable {
    private let lock = NSLock()
    private var _events: [Bool] = []
    var events: [Bool] { lock.withLock { _events } }
    func add(_ event: Bool) { lock.withLock { _events.append(event) } }
}
