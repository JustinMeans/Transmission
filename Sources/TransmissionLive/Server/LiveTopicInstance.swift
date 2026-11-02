import Foundation
import NIOConcurrencyHelpers
import NIOCore

/// What a mailbox gets when it asks a state or series topic to catch it up.
enum TopicCatchUp {
    case delta(seq: UInt64, body: ByteBuffer, flags: UInt8)
    case snapshot(seq: UInt64, parts: [SnapshotPart])
}

struct SnapshotPart: Sendable {
    let body: ByteBuffer
    let flags: UInt8
}

/// The typed side of a topic, erased so the hub and connections need not be generic.
protocol TopicCore: Sendable {
    /// Applies an encoded delta to the current state.
    func apply(payload: Data) throws
    /// Encodes the current state.
    func encodeState() throws -> Data
    /// Folds encoded deltas into one, or nil if this topic cannot merge.
    func merged(_ payloads: [Data]) -> Data?
}

private final class TypedTopicCore<State: Codable & Sendable, Delta: Codable & Sendable>: TopicCore, @unchecked Sendable {
    private let topic: Topic<State, Delta>
    private var state: State
    private let lock = NIOLock()
    private let encoder = JSONEncoder()
    private let decoder = JSONDecoder()

    init(topic: Topic<State, Delta>, initial: State) {
        self.topic = topic
        self.state = initial
    }

    func apply(payload: Data) throws {
        try lock.withLock {
            topic.apply(&state, try decoder.decode(Delta.self, from: payload))
        }
    }

    func encodeState() throws -> Data {
        try lock.withLock { try encoder.encode(state) }
    }

    func merged(_ payloads: [Data]) -> Data? {
        guard let merge = topic.merge, let first = payloads.first else { return nil }
        return lock.withLock {
            guard var combined = try? decoder.decode(Delta.self, from: first) else { return nil }
            for payload in payloads.dropFirst() {
                guard let next = try? decoder.decode(Delta.self, from: payload) else { return nil }
                merge(&combined, next)
            }
            return try? encoder.encode(combined)
        }
    }
}

/// One hosted topic: its ring, its state and every encoded form it has shared.
public final class LiveTopicInstance: Sendable {
    public let key: TopicKey
    public let kind: TopicKind
    public let ring: TopicRing
    private let core: any TopicCore
    private let maxFrame: Int

    private struct Caches {
        var snapshot: (seq: UInt64, parts: [SnapshotPart])?
        var coalesced: [UInt64: ByteBuffer] = [:]
        var coalescedHead: UInt64 = 0
        var encodeCount = 0
    }
    /// Held across state mutation too, so a snapshot's state and seq always agree.
    private let caches = NIOLockedValueBox(Caches())

    init<State, Delta>(topic: Topic<State, Delta>, initial: State, limits: TopicRing.Limits, maxFrame: Int) {
        key = topic.key
        kind = topic.kind
        ring = TopicRing(limits: limits)
        core = TypedTopicCore(topic: topic, initial: initial)
        self.maxFrame = maxFrame
    }

    /// How many times this topic has been encoded for the wire: once per snapshot
    /// and once per coalesced range, never per subscriber.
    public var encodeCount: Int {
        caches.withLockedValue { $0.encodeCount }
    }

    func ingest(payload: Data, sampleTimeMs: UInt64) throws {
        try caches.withLockedValue { _ in
            try core.apply(payload: payload)
            ring.append(payload: payload, sampleTimeMs: sampleTimeMs)
        }
    }

    /// The current snapshot, encoded at most once per sequence.
    func snapshot() throws -> (seq: UInt64, parts: [SnapshotPart]) {
        try caches.withLockedValue { caches in
            let head = ring.headSeq
            if let cached = caches.snapshot, cached.seq == head { return cached }
            let parts = try makeParts(payload: core.encodeState(), seq: head)
            caches.encodeCount += 1
            caches.snapshot = (head, parts)
            return (head, parts)
        }
    }

    private func makeParts(payload: Data, seq: UInt64) -> [SnapshotPart] {
        // Room for the header and the snapshot's own fields.
        let chunkSize = max(maxFrame - 64, 1)
        var parts: [SnapshotPart] = []
        var offset = 0
        var index: UInt64 = 0
        repeat {
            let end = min(offset + chunkSize, payload.count)
            var writer = LiveWriter(capacity: end - offset + 32)
            writer.varint(ring.epoch)
            writer.varint(seq)
            writer.varint(UInt64(key.schema))
            writer.varint(index)
            writer.payload(payload.subdata(in: offset..<end))
            parts.append(SnapshotPart(
                body: ByteBuffer(bytes: writer.bytes),
                flags: end == payload.count ? FrameFlag.last : 0))
            offset = end
            index += 1
        } while offset < payload.count
        return parts
    }

    /// Brings a state or series consumer from `cursor` to the head in one step.
    func catchUp(from cursor: UInt64) throws -> TopicCatchUp? {
        let head = ring.headSeq
        guard cursor < head else { return nil }
        if head == cursor + 1, let entry = ring.entry(seq: head) {
            return .delta(seq: head, body: entry.body, flags: 0)
        }
        if ring.canReplay(from: cursor) {
            let entries = ring.entries(after: cursor, through: head)
            if let body = coalescedBody(entries, cursor: cursor, head: head) {
                return .delta(seq: head, body: body, flags: FrameFlag.span)
            }
        }
        let snapshot = try snapshot()
        return .snapshot(seq: snapshot.seq, parts: snapshot.parts)
    }

    /// Encoded once per `(cursor, head)`, so every client lagging by the same
    /// amount shares one buffer.
    private func coalescedBody(_ entries: [TopicRing.Entry], cursor: UInt64, head: UInt64) -> ByteBuffer? {
        caches.withLockedValue { caches in
            if caches.coalescedHead != head {
                caches.coalesced.removeAll(keepingCapacity: true)
                caches.coalescedHead = head
            }
            if let cached = caches.coalesced[cursor] { return cached }
            let payload: Data
            if let merged = core.merged(entries.map(\.payload)) {
                payload = merged
            } else if kind == .series, let latest = entries.last {
                payload = latest.payload
            } else {
                return nil
            }
            var writer = LiveWriter(capacity: payload.count + 24)
            writer.varint(head)
            writer.varint(entries.last?.sampleTimeMs ?? 0)
            writer.varint(head - cursor)
            writer.payload(payload)
            let body = ByteBuffer(bytes: writer.bytes)
            caches.coalesced[cursor] = body
            caches.encodeCount += 1
            return body
        }
    }
}
