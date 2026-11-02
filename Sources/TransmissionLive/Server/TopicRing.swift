import Foundation
import NIOConcurrencyHelpers
import NIOCore

/// Epoch, sequence and bounded history of one topic instance.
///
/// Consumers read straight from here under its lock, without going through
/// `LiveHub`, so a publish and N slow readers never queue behind each other.
public final class TopicRing: Sendable {
    public struct Limits: Sendable, Equatable {
        public var maxEntries: Int
        public var maxBytes: Int
        public var maxAge: Duration?

        public init(maxEntries: Int, maxBytes: Int, maxAge: Duration? = nil) {
            self.maxEntries = maxEntries
            self.maxBytes = maxBytes
            self.maxAge = maxAge
        }

        /// 60 seconds of metrics.
        public static let metrics = Limits(maxEntries: 600, maxBytes: 1 << 20, maxAge: .seconds(60))
        /// 5,000 lines or 1 MiB.
        public static let logs = Limits(maxEntries: 5000, maxBytes: 1 << 20)
    }

    public struct Entry: Sendable {
        public let seq: UInt64
        public let sampleTimeMs: UInt64
        /// The JSON payload, shared by reference.
        public let payload: Data
        /// Encoded `DELTA` body for span 1, shared by every subscriber.
        public let body: ByteBuffer
        let appendedAt: ContinuousClock.Instant

        var byteCount: Int { body.readableBytes }
    }

    private struct Storage {
        var entries: [Entry] = []
        var head = 0
        var headSeq: UInt64 = 0
        var bytes = 0

        var count: Int { entries.count - head }

        mutating func dropOldest() {
            bytes -= entries[head].byteCount
            head += 1
            if head > 64, head * 2 >= entries.count {
                entries.removeFirst(head)
                head = 0
            }
        }
    }

    public let epoch: UInt64
    public let limits: Limits
    private let storage = NIOLockedValueBox(Storage())

    public init(limits: Limits, epoch: UInt64 = TopicRing.makeEpoch()) {
        self.limits = limits
        self.epoch = epoch
    }

    /// Random per instance start. Kept under 2^53 so it survives any JSON tooling.
    public static func makeEpoch() -> UInt64 {
        UInt64.random(in: 1...(1 << 53))
    }

    /// Assigns the next sequence. Called once per delta, at the source.
    @discardableResult
    public func append(payload: Data, sampleTimeMs: UInt64) -> UInt64 {
        let now = ContinuousClock.now
        return storage.withLockedValue { state in
            state.headSeq += 1
            let seq = state.headSeq
            var writer = LiveWriter(capacity: payload.count + 16)
            writer.varint(seq)
            writer.varint(sampleTimeMs)
            writer.payload(payload)
            let entry = Entry(
                seq: seq, sampleTimeMs: sampleTimeMs, payload: payload,
                body: ByteBuffer(bytes: writer.bytes), appendedAt: now)
            state.entries.append(entry)
            state.bytes += entry.byteCount
            // The newest entry always stays, however large it is.
            while state.count > 1, state.count > limits.maxEntries || state.bytes > limits.maxBytes {
                state.dropOldest()
            }
            if let maxAge = limits.maxAge {
                while state.count > 1, state.entries[state.head].appendedAt.duration(to: now) > maxAge {
                    state.dropOldest()
                }
            }
            return seq
        }
    }

    public var headSeq: UInt64 {
        storage.withLockedValue { $0.headSeq }
    }

    /// The oldest retained sequence, or `headSeq + 1` when the ring is empty.
    public var tailSeq: UInt64 {
        storage.withLockedValue { $0.count == 0 ? $0.headSeq + 1 : $0.entries[$0.head].seq }
    }

    public var count: Int {
        storage.withLockedValue { $0.count }
    }

    public var byteCount: Int {
        storage.withLockedValue { $0.bytes }
    }

    public func entry(seq: UInt64) -> Entry? {
        storage.withLockedValue { state in
            guard state.count > 0 else { return nil }
            let tail = state.entries[state.head].seq
            guard seq >= tail, seq <= state.headSeq else { return nil }
            return state.entries[state.head + Int(seq - tail)]
        }
    }

    /// Retained entries in `(after, through]`.
    public func entries(after: UInt64, through: UInt64) -> [Entry] {
        storage.withLockedValue { state in
            guard state.count > 0, through > after else { return [] }
            let tail = state.entries[state.head].seq
            let first = max(after + 1, tail)
            let last = min(through, state.headSeq)
            guard first <= last else { return [] }
            let start = state.head + Int(first - tail)
            return Array(state.entries[start...(start + Int(last - first))])
        }
    }

    /// Whether a consumer at `cursor` could receive every later delta.
    public func canReplay(from cursor: UInt64) -> Bool {
        storage.withLockedValue { state in
            guard cursor <= state.headSeq else { return false }
            if cursor == state.headSeq { return true }
            guard state.count > 0 else { return false }
            return cursor + 1 >= state.entries[state.head].seq
        }
    }

    /// Bytes a consumer at `cursor` is behind by, as far as the ring remembers.
    public func bytesBehind(_ cursor: UInt64) -> Int {
        entries(after: cursor, through: .max).reduce(0) { $0 + $1.byteCount }
    }
}
