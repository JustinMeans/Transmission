import Foundation
import NIOConcurrencyHelpers
import NIOCore
import Testing
import TransmissionTestSupport
@testable import TransmissionLive

// MARK: Topics

typealias Stats = [String: Double]

let statsTopic = Topic<Stats, Stats>(
    name: "stats", kind: .state,
    merge: { earlier, later in earlier.merge(later) { _, new in new } },
    apply: { state, delta in state.merge(delta) { _, new in new } })

/// No `merge`, so overflow must fall back to a snapshot.
let plainTopic = Topic<Stats, Stats>(
    name: "plain", kind: .state,
    apply: { state, delta in state.merge(delta) { _, new in new } })

let seriesTopic = Topic<[Double], Double>(
    name: "series", kind: .series,
    apply: { state, point in state.append(point) })

let logTopic = Topic<[String], String>(
    name: "logs", kind: .log,
    apply: { state, line in state.append(line) })

/// Carries the publish time so a receiver can measure latency.
struct Tick: Codable, Sendable { var sentAt: UInt64; var padding: String = "" }

let tickTopic = Topic<Tick, Tick>(name: "tick", kind: .state, apply: { $0 = $1 })

func nowNanos() -> UInt64 { DispatchTime.now().uptimeNanoseconds }

// MARK: Auth

struct AllowAll: LiveAuthorizer {
    func authenticate(token: String) async throws -> LiveIdentity { LiveIdentity(subject: token) }
    func authorize(_ identity: LiveIdentity, topic: TopicKey) async -> Bool { true }
    func revalidate(_ identity: LiveIdentity) async -> Bool { true }
}

struct Rules: LiveAuthorizer {
    var denied: Set<String> = []
    var revalidates = true
    func authenticate(token: String) async throws -> LiveIdentity {
        guard token != "bad" else { throw LiveTransportClosed(code: nil) }
        return LiveIdentity(subject: token)
    }
    func authorize(_ identity: LiveIdentity, topic: TopicKey) async -> Bool { !denied.contains(topic.name) }
    func revalidate(_ identity: LiveIdentity) async -> Bool { revalidates }
}

func fastConfiguration() -> LiveHubConfiguration {
    var configuration = LiveHubConfiguration()
    configuration.tickInterval = .milliseconds(40)
    return configuration
}

// MARK: Server end driven directly

/// A connection with a test standing in for the client.
final class Peer: LiveServerTransport, @unchecked Sendable {
    private let lock = NIOLock()
    private var _frames: [LiveFrame] = []
    private var _raw: [LiveWireFrame] = []
    private var _closeCode: LiveCloseCode?
    private var _writes = 0
    private let stalled: Bool
    private var waiters: [CheckedContinuation<Void, any Error>] = []
    var connection: LiveConnection!

    init(stalled: Bool = false) { self.stalled = stalled }

    static func connect(
        _ hub: LiveHub,
        identity: LiveIdentity? = LiveIdentity(subject: "u"),
        clientID: String = UUID().uuidString,
        stalled: Bool = false,
        hello: Bool = true
    ) async -> Peer {
        let peer = Peer(stalled: stalled)
        peer.connection = LiveConnection(hub: hub, transport: peer, identity: identity)
        await peer.connection.start()
        if hello {
            peer.send(.hello(protoMin: 1, protoMax: 1, build: "t", clientID: clientID))
            await eventually { peer.frames.contains { if case .welcome = $0 { true } else { false } } || peer.closeCode != nil }
        }
        return peer
    }

    func send(_ frame: LiveFrame) {
        connection.receive(Array(LiveCodec.encode(frame)))
    }

    func write(_ frame: LiveWireFrame) async throws {
        lock.withLock { _writes += 1 }
        if stalled {
            try await withCheckedThrowingContinuation { continuation in
                lock.withLock { waiters.append(continuation) }
            }
        }
        let decoded = try LiveCodec.decode(frame.assembled)
        lock.withLock {
            _frames.append(decoded)
            _raw.append(frame)
        }
    }

    func close(code: LiveCloseCode) async {
        let pending = lock.withLock { () -> [CheckedContinuation<Void, any Error>] in
            _closeCode = code
            defer { waiters = [] }
            return waiters
        }
        pending.forEach { $0.resume(throwing: LiveTransportClosed(code: code)) }
    }

    var frames: [LiveFrame] { lock.withLock { _frames } }
    var raw: [LiveWireFrame] { lock.withLock { _raw } }
    var closeCode: LiveCloseCode? { lock.withLock { _closeCode } }
    var writes: Int { lock.withLock { _writes } }

    var deltas: [(seq: UInt64, span: UInt64, payload: Data)] {
        frames.compactMap { if case let .delta(_, seq, _, span, payload) = $0 { (seq, span, payload) } else { nil } }
    }
    var snapshots: [(seq: UInt64, part: UInt64, last: Bool, payload: Data)] {
        frames.compactMap { if case let .snapshot(_, _, seq, _, part, last, payload) = $0 { (seq, part, last, payload) } else { nil } }
    }
    var gaps: [(from: UInt64, to: UInt64, dropped: UInt64)] {
        frames.compactMap { if case let .gap(_, from, to, dropped, _) = $0 { (from, to, dropped) } else { nil } }
    }
    var errors: [LiveErrorCode] {
        frames.compactMap { if case let .error(_, code, _, _) = $0 { code } else { nil } }
    }
    var acks: [(epoch: UInt64, head: UInt64, replay: Bool)] {
        frames.compactMap { if case let .subAck(_, epoch, head, _, replay) = $0 { (epoch, head, replay) } else { nil } }
    }

    func subscribe<S, D>(
        _ topic: Topic<S, D>, subID: UInt64 = 1, maxMilliHz: UInt64 = 0, resume: LiveCursor? = nil, schema: UInt64? = nil
    ) async {
        send(.sub(
            subID: subID, topic: topic.name, params: topic.params, schema: schema ?? UInt64(topic.schema),
            maxMilliHz: maxMilliHz, resume: resume))
        await eventually { self.frames.contains { frame in
            switch frame {
            case .subAck(subID, _, _, _, _), .error(subID, _, _, _): return true
            default: return false
            }
        } }
    }
}

// MARK: In-memory client transport

/// Connects `LiveClient` straight to a hub with no socket in between.
final class LoopbackTransport: LiveTransport, @unchecked Sendable {
    let hub: LiveHub
    private let lock = NIOLock()
    private var _connections: [LoopbackConnection] = []
    private var _attempts = 0
    private var _refuse = false
    private var _headers: [[String: String]] = []
    var identity = LiveIdentity(subject: "u")
    /// After this many writes the socket hangs, like a peer that stopped reading.
    var stallAfterWrites: Int?

    init(hub: LiveHub) { self.hub = hub }

    var attempts: Int { lock.withLock { _attempts } }
    var headers: [[String: String]] { lock.withLock { _headers } }
    var refuse: Bool {
        get { lock.withLock { _refuse } }
        set { lock.withLock { _refuse = newValue } }
    }
    var latest: LoopbackConnection? { lock.withLock { _connections.last } }

    func connect(_ request: LiveConnectRequest) async throws -> any LiveTransportConnection {
        let refused = lock.withLock { () -> Bool in
            _attempts += 1
            _headers.append(request.headers)
            return _refuse
        }
        if refused { throw LiveTransportError.rejected(status: 503) }
        let client = LoopbackConnection(stallAfterWrites: stallAfterWrites)
        let server = LiveConnection(hub: hub, transport: client, identity: identity)
        client.server = server
        await server.start()
        lock.withLock { _connections.append(client) }
        return client
    }
}

final class LoopbackConnection: LiveTransportConnection, LiveServerTransport, @unchecked Sendable {
    private let stream: AsyncThrowingStream<Data, any Error>
    private let sink: AsyncThrowingStream<Data, any Error>.Continuation
    private let stallAfterWrites: Int?
    private var writes = 0
    private let lock = NIOLock()
    private var waiters: [CheckedContinuation<Void, any Error>] = []
    private var iterator: AsyncThrowingStream<Data, any Error>.AsyncIterator
    private var _closed = false
    var server: LiveConnection!
    private(set) var sent: [LiveFrame] = []

    init(stallAfterWrites: Int?) {
        (stream, sink) = AsyncThrowingStream.makeStream(of: Data.self)
        iterator = stream.makeAsyncIterator()
        self.stallAfterWrites = stallAfterWrites
    }

    var sentFrames: [LiveFrame] { lock.withLock { sent } }

    // Client side.
    func send(_ data: Data) async throws {
        let frame = try LiveCodec.decode(data)
        lock.withLock { sent.append(frame) }
        server.receive(Array(data))
    }

    func receive() async throws -> Data {
        var local = lock.withLock { iterator }
        guard let data = try await local.next() else { throw LiveTransportClosed(code: nil) }
        return data
    }

    /// Either end calling this ends both.
    func close(code: LiveCloseCode) async {
        finish(code: code)
        server.transportClosed()
    }

    // Server side.
    func write(_ frame: LiveWireFrame) async throws {
        let stall = lock.withLock { () -> Bool in
            writes += 1
            return stallAfterWrites.map { writes > $0 } ?? false
        }
        if stall {
            try await withCheckedThrowingContinuation { continuation in
                lock.withLock { waiters.append(continuation) }
            }
        }
        sink.yield(Data(frame.assembled))
    }

    private func finish(code: LiveCloseCode) {
        let pending = lock.withLock { () -> [CheckedContinuation<Void, any Error>] in
            defer { waiters = [] }
            return waiters
        }
        pending.forEach { $0.resume(throwing: LiveTransportClosed(code: code)) }
        sink.finish(throwing: LiveTransportClosed(code: code))
    }
}

// MARK: Scripted client transport

/// A fake server the test types into, for sequences no real hub would produce.
final class ScriptedTransport: LiveTransport, LiveTransportConnection, @unchecked Sendable {
    private let lock = NIOLock()
    private var inbox: AsyncThrowingStream<Data, any Error>
    private var sink: AsyncThrowingStream<Data, any Error>.Continuation
    private var _sent: [LiveFrame] = []
    private var _connects = 0

    init() {
        (inbox, sink) = AsyncThrowingStream.makeStream(of: Data.self)
    }

    var sent: [LiveFrame] { lock.withLock { _sent } }
    var connects: Int { lock.withLock { _connects } }
    var subs: [LiveFrame] { sent.filter { if case .sub = $0 { true } else { false } } }

    func connect(_ request: LiveConnectRequest) async throws -> any LiveTransportConnection {
        lock.withLock {
            _connects += 1
            (inbox, sink) = AsyncThrowingStream.makeStream(of: Data.self)
        }
        return self
    }

    func server(_ frame: LiveFrame) {
        lock.withLock { sink }.yield(LiveCodec.encode(frame))
    }

    func serverRaw(_ bytes: [UInt8]) {
        lock.withLock { sink }.yield(Data(bytes))
    }

    func serverClose(_ code: LiveCloseCode?) {
        lock.withLock { sink }.finish(throwing: LiveTransportClosed(code: code))
    }

    func welcome(heartbeatMs: UInt64 = 25_000) {
        server(.welcome(proto: 1, epoch: 1, serverTimeMs: 0, heartbeatMs: heartbeatMs, maxFrame: 1 << 20, topics: []))
    }

    func send(_ data: Data) async throws {
        let frame = try LiveCodec.decode(data)
        lock.withLock { _sent.append(frame) }
    }

    func receive() async throws -> Data {
        let stream = lock.withLock { inbox }
        var iterator = stream.makeAsyncIterator()
        guard let data = try await iterator.next() else { throw LiveTransportClosed(code: nil) }
        return data
    }

    func close(code: LiveCloseCode) async {
        serverClose(code)
    }
}

func encoded(_ value: some Encodable) -> Data {
    try! JSONEncoder().encode(value)
}

func clientConfiguration(id: String = UUID().uuidString) -> LiveClientConfiguration {
    var configuration = LiveClientConfiguration(
        url: URL(string: "ws://127.0.0.1/v3/live")!, clientID: id, build: "test")
    configuration.reconnect = LiveReconnectPolicy(minimumDelay: .milliseconds(10), maximumDelay: .milliseconds(100))
    return configuration
}

/// Collects a subscription's updates on a background task.
final class UpdateLog<State: Codable & Sendable, Delta: Codable & Sendable>: @unchecked Sendable {
    private let lock = NIOLock()
    private var _all: [LiveUpdate<State, Delta>] = []
    private var task: Task<Void, Never>?

    init(_ subscription: LiveSubscription<State, Delta>) {
        task = Task {
            for await update in subscription.updates {
                self.lock.withLock { self._all.append(update) }
            }
        }
    }

    var all: [LiveUpdate<State, Delta>] { lock.withLock { _all } }
    var last: LiveUpdate<State, Delta>? { all.last }
    func stop() { task?.cancel() }
}
