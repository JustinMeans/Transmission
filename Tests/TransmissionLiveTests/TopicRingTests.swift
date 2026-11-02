import Foundation
import Testing
@testable import TransmissionLive

@Suite("TopicRing")
struct TopicRingTests {
    private func ring(_ limits: TopicRing.Limits) -> TopicRing { TopicRing(limits: limits) }

    @Test("sequences start at 1 and only go up")
    func sequences() {
        let ring = ring(.init(maxEntries: 10, maxBytes: 1 << 20))
        #expect(ring.headSeq == 0)
        #expect(ring.append(payload: Data([1]), sampleTimeMs: 0) == 1)
        #expect(ring.append(payload: Data([2]), sampleTimeMs: 0) == 2)
        #expect(ring.headSeq == 2)
        #expect(ring.tailSeq == 1)
    }

    @Test("each instance gets its own epoch")
    func epochs() {
        #expect(ring(.metrics).epoch != ring(.metrics).epoch)
    }

    @Test("an empty ring reports its tail just past the head")
    func emptyTail() {
        #expect(ring(.metrics).tailSeq == 1)
    }

    @Test("oldest entries go past maxEntries")
    func entryLimit() {
        let ring = ring(.init(maxEntries: 3, maxBytes: 1 << 20))
        for _ in 0..<10 { ring.append(payload: Data([0]), sampleTimeMs: 0) }
        #expect(ring.count == 3)
        #expect(ring.tailSeq == 8)
        #expect(ring.headSeq == 10)
        #expect(ring.entry(seq: 7) == nil)
        #expect(ring.entry(seq: 8)?.seq == 8)
        #expect(ring.entry(seq: 10)?.seq == 10)
        #expect(ring.entry(seq: 11) == nil)
    }

    @Test("oldest entries go past maxBytes, and the newest always stays")
    func byteLimit() {
        let ring = ring(.init(maxEntries: 100, maxBytes: 100))
        for _ in 0..<5 { ring.append(payload: Data(repeating: 0, count: 40), sampleTimeMs: 0) }
        #expect(ring.byteCount <= 100)
        ring.append(payload: Data(repeating: 0, count: 500), sampleTimeMs: 0)
        #expect(ring.count == 1)
        #expect(ring.tailSeq == ring.headSeq)
    }

    @Test("oldest entries go past maxAge")
    func ageLimit() async throws {
        let ring = ring(.init(maxEntries: 100, maxBytes: 1 << 20, maxAge: .milliseconds(20)))
        ring.append(payload: Data([1]), sampleTimeMs: 0)
        try await Task.sleep(for: .milliseconds(40))
        ring.append(payload: Data([2]), sampleTimeMs: 0)
        #expect(ring.count == 1)
        #expect(ring.tailSeq == 2)
    }

    @Test("byte accounting stays exact through heavy churn")
    func churn() {
        let ring = ring(.init(maxEntries: 5, maxBytes: 1 << 20))
        for index in 0..<10_000 { ring.append(payload: Data(repeating: 1, count: index % 7), sampleTimeMs: 0) }
        #expect(ring.count == 5)
        #expect(ring.tailSeq == 9_996)
        #expect(ring.entries(after: 9_995, through: 10_000).map(\.seq) == [9_996, 9_997, 9_998, 9_999, 10_000])
        #expect(ring.byteCount == ring.entries(after: 0, through: .max).reduce(0) { $0 + $1.byteCount })
    }

    @Test("entries(after:through:) clamps to what is retained")
    func entriesRange() {
        let ring = ring(.init(maxEntries: 3, maxBytes: 1 << 20))
        for _ in 0..<6 { ring.append(payload: Data(), sampleTimeMs: 0) }
        #expect(ring.entries(after: 0, through: 100).map(\.seq) == [4, 5, 6])
        #expect(ring.entries(after: 4, through: 5).map(\.seq) == [5])
        #expect(ring.entries(after: 6, through: 6).isEmpty)
    }

    @Test("canReplay is true only when nothing would be missed")
    func canReplay() {
        let ring = ring(.init(maxEntries: 3, maxBytes: 1 << 20))
        #expect(ring.canReplay(from: 0))
        for _ in 0..<6 { ring.append(payload: Data(), sampleTimeMs: 0) }
        #expect(!ring.canReplay(from: 0))
        #expect(!ring.canReplay(from: 2))
        #expect(ring.canReplay(from: 3))
        #expect(ring.canReplay(from: 6))
        #expect(!ring.canReplay(from: 7))
    }

    @Test("a delta body decodes as a DELTA frame and shares its storage between reads")
    func sharedBody() throws {
        let ring = ring(.metrics)
        ring.append(payload: Data("{}".utf8), sampleTimeMs: 5)
        let first = try #require(ring.entry(seq: 1))
        let second = try #require(ring.entry(seq: 1))
        let a = first.body.withUnsafeReadableBytes { UInt(bitPattern: $0.baseAddress) }
        let b = second.body.withUnsafeReadableBytes { UInt(bitPattern: $0.baseAddress) }
        #expect(a == b)
        let frame = LiveWireFrame(type: FrameType.delta, subID: 9, body: first.body)
        #expect(try LiveCodec.decode(frame.assembled) == .delta(subID: 9, seq: 1, sampleTimeMs: 5, span: 1, payload: Data("{}".utf8)))
    }
}
