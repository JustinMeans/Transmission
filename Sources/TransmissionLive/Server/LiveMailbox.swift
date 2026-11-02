import Foundation
import NIOCore

/// One subscription's queue. It is a cursor into the shared ring rather than a
/// list of frames, so a stalled client costs a few integers however much is
/// published, and overflow is decided at the moment the writer can send.
struct LiveMailbox {
    enum Pull {
        case frame(LiveWireFrame)
        case idle
        case wake(at: ContinuousClock.Instant)
    }

    let subID: UInt64
    let instance: LiveTopicInstance
    let capacity: Int
    let minimumInterval: Duration?
    private(set) var cursor: UInt64
    private(set) var pending: [LiveWireFrame]
    private var nextAllowed: ContinuousClock.Instant?
    var paused = false
    var ackedSeq: UInt64 = 0

    init(
        subID: UInt64,
        instance: LiveTopicInstance,
        capacity: Int,
        maxMilliHz: UInt64,
        cursor: UInt64,
        pending: [LiveWireFrame]
    ) {
        self.subID = subID
        self.instance = instance
        self.capacity = max(capacity, 1)
        self.cursor = cursor
        self.pending = pending
        minimumInterval = maxMilliHz == 0 ? nil : .nanoseconds(Int64(1_000_000_000_000 / maxMilliHz))
    }

    var bytesBehind: Int {
        switch instance.kind {
        // Latest wins: whatever is behind merges away rather than queueing.
        case .state, .series: return 0
        case .log, .event: return instance.ring.bytesBehind(cursor)
        }
    }

    mutating func pull(now: ContinuousClock.Instant) -> Pull {
        guard !paused else { return .idle }
        if !pending.isEmpty {
            return .frame(pending.removeFirst())
        }
        switch instance.kind {
        case .state, .series:
            return pullCoalesced(now: now)
        case .log, .event:
            return pullNextEntry()
        }
    }

    private mutating func pullCoalesced(now: ContinuousClock.Instant) -> Pull {
        guard cursor < instance.ring.headSeq else { return .idle }
        if let nextAllowed, now < nextAllowed {
            return .wake(at: nextAllowed)
        }
        let catchUp: TopicCatchUp?
        do {
            catchUp = try instance.catchUp(from: cursor)
        } catch {
            return .idle
        }
        if let minimumInterval { nextAllowed = now.advanced(by: minimumInterval) }
        switch catchUp {
        case nil:
            return .idle
        case .delta(let seq, let body, let flags):
            cursor = seq
            return .frame(LiveWireFrame(type: FrameType.delta, flags: flags, subID: subID, body: body))
        case .snapshot(let seq, let parts):
            cursor = seq
            pending = parts.dropFirst().map {
                LiveWireFrame(type: FrameType.snapshot, flags: $0.flags, subID: subID, body: $0.body)
            }
            let first = parts[0]
            return .frame(LiveWireFrame(type: FrameType.snapshot, flags: first.flags, subID: subID, body: first.body))
        }
    }

    private mutating func pullNextEntry() -> Pull {
        let ring = instance.ring
        let head = ring.headSeq
        guard cursor < head else { return .idle }
        let oldestKept = max(ring.tailSeq, head - min(UInt64(capacity), head) + 1)
        if cursor + 1 < oldestKept {
            let reason: LiveGapReason = cursor + 1 < ring.tailSeq ? .outsideRing : .overflow
            let from = cursor + 1
            let to = oldestKept - 1
            cursor = to
            return .frame(LiveCodec.wireFrame(.gap(
                subID: subID, fromSeq: from, toSeq: to, droppedCount: to - from + 1, reason: reason)))
        }
        guard let entry = ring.entry(seq: cursor + 1) else { return .idle }
        cursor = entry.seq
        return .frame(LiveWireFrame(type: FrameType.delta, subID: subID, body: entry.body))
    }
}
