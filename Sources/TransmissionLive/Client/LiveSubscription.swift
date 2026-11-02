import Foundation

/// How far a value can be trusted. A number that stopped updating must not look live.
public enum LiveFreshness: Sendable, Equatable {
    /// Connected and caught up.
    case live
    /// Not connected. The state is what it was at `since`.
    case stale(since: Date)
    /// Connected and catching up, or waiting for the first snapshot.
    case resyncing
}

public struct LiveUpdate<State: Sendable, Delta: Sendable>: Sendable {
    public let state: State
    public let freshness: LiveFreshness
    public let seq: UInt64
    /// Set when this update came from one delta rather than a snapshot.
    public let delta: Delta?
    /// Log topics: lines lost since the previous update. Show this to the user.
    public let dropped: UInt64
}

/// One subscription. It survives reconnects: the client re-issues it with its
/// cursor. Cancel it, or call `LiveClient.unsubscribe`, when the view goes away.
public struct LiveSubscription<State: Codable & Sendable, Delta: Codable & Sendable>: Sendable {
    public let updates: AsyncStream<LiveUpdate<State, Delta>>
    let subID: UInt64
    let client: LiveClient

    public func cancel() async {
        await client.unsubscribe(subID: subID)
    }

    public func pause() async {
        await client.setPaused(subID: subID, paused: true)
    }

    public func resume() async {
        await client.setPaused(subID: subID, paused: false)
    }
}

/// The typed side of a subscription, erased so `LiveClient` need not be generic.
protocol LiveClientSubscription: AnyObject, Sendable {
    var subID: UInt64 { get }
    var frame: (topic: String, params: [String: String], schema: UInt64, maxMilliHz: UInt64, kind: TopicKind) { get }
    var cursor: LiveCursor? { get }
    var isPaused: Bool { get set }
    func beginResync()
    func connectionLost(at date: Date)
    func handleAck(epoch: UInt64, headSeq: UInt64, replay: Bool)
    /// Returns false when the sequence shows a hole, so the caller re-subscribes.
    func handleSnapshot(epoch: UInt64, seq: UInt64, part: UInt64, last: Bool, payload: Data) -> Bool
    func handleDelta(seq: UInt64, span: UInt64, payload: Data) -> Bool
    func handleGap(fromSeq: UInt64, toSeq: UInt64, dropped: UInt64) -> Bool
    func shouldAck(every interval: Int) -> UInt64?
    func finish()
}

/// Confined to the owning `LiveClient` actor, which is what makes it `Sendable`.
final class TypedClientSubscription<State: Codable & Sendable, Delta: Codable & Sendable>: LiveClientSubscription, @unchecked Sendable {
    let subID: UInt64
    private let topic: Topic<State, Delta>
    private let maxMilliHz: UInt64
    private let continuation: AsyncStream<LiveUpdate<State, Delta>>.Continuation
    private let decoder = JSONDecoder()

    private var state: State?
    private var epoch: UInt64?
    private var lastSeq: UInt64 = 0
    private var freshness = LiveFreshness.resyncing
    private var targetSeq: UInt64?
    private var parts = Data()
    private var nextPart: UInt64 = 0
    private var unackedDeltas = 0
    /// Between finding a hole and the server's `SUBACK`, deltas are stale.
    private var awaitingAck = false
    var isPaused = false

    init(subID: UInt64, topic: Topic<State, Delta>, maxMilliHz: UInt64, continuation: AsyncStream<LiveUpdate<State, Delta>>.Continuation) {
        self.subID = subID
        self.topic = topic
        self.maxMilliHz = maxMilliHz
        self.continuation = continuation
    }

    var frame: (topic: String, params: [String: String], schema: UInt64, maxMilliHz: UInt64, kind: TopicKind) {
        (topic.name, topic.params, UInt64(topic.schema), maxMilliHz, topic.kind)
    }

    var cursor: LiveCursor? {
        guard state != nil, let epoch else { return nil }
        return LiveCursor(epoch: epoch, seq: lastSeq)
    }

    func beginResync() {
        awaitingAck = true
        parts = Data()
        nextPart = 0
        setFreshness(.resyncing)
    }

    func connectionLost(at date: Date) {
        if case .stale = freshness { return }
        setFreshness(.stale(since: date))
    }

    func handleAck(epoch: UInt64, headSeq: UInt64, replay: Bool) {
        awaitingAck = false
        targetSeq = headSeq
        // The epoch changes only with a snapshot: a cursor must never pair a new
        // epoch with a sequence from the old one.
        if replay {
            settle()
        }
    }

    func handleSnapshot(epoch: UInt64, seq: UInt64, part: UInt64, last: Bool, payload: Data) -> Bool {
        guard !awaitingAck else { return true }
        guard part == nextPart else {
            parts = Data()
            nextPart = 0
            return false
        }
        parts.append(payload)
        nextPart += 1
        guard last else { return true }
        defer {
            parts = Data()
            nextPart = 0
        }
        guard let decoded = try? decoder.decode(State.self, from: parts) else {
            return false
        }
        state = decoded
        self.epoch = epoch
        lastSeq = seq
        targetSeq = max(targetSeq ?? seq, seq)
        freshness = .live
        emit(delta: nil, dropped: 0)
        return true
    }

    func handleDelta(seq: UInt64, span: UInt64, payload: Data) -> Bool {
        guard !awaitingAck, state != nil else { return true }
        guard seq > lastSeq else { return true }
        guard seq >= span, seq - span == lastSeq else { return false }
        guard let delta = try? decoder.decode(Delta.self, from: payload) else { return false }
        topic.apply(&state!, delta)
        lastSeq = seq
        unackedDeltas += 1
        settle(emitting: false)
        emit(delta: delta, dropped: 0)
        return true
    }

    func handleGap(fromSeq: UInt64, toSeq: UInt64, dropped: UInt64) -> Bool {
        guard !awaitingAck else { return true }
        guard topic.kind == .log || topic.kind == .event else { return false }
        guard toSeq > lastSeq else { return true }
        lastSeq = toSeq
        settle(emitting: false)
        emit(delta: nil, dropped: dropped)
        return true
    }

    func shouldAck(every interval: Int) -> UInt64? {
        guard unackedDeltas >= interval else { return nil }
        unackedDeltas = 0
        return lastSeq
    }

    func finish() {
        continuation.finish()
    }

    private func settle(emitting: Bool = true) {
        guard state != nil, freshness != .live, lastSeq >= (targetSeq ?? .max) else { return }
        if emitting {
            setFreshness(.live)
        } else {
            freshness = .live
        }
    }

    private func setFreshness(_ value: LiveFreshness) {
        guard value != freshness else { return }
        freshness = value
        if state != nil { emit(delta: nil, dropped: 0) }
    }

    private func emit(delta: Delta?, dropped: UInt64) {
        guard let state else { return }
        continuation.yield(LiveUpdate(state: state, freshness: freshness, seq: lastSeq, delta: delta, dropped: dropped))
    }
}
