import Testing
import Foundation
@testable import Transmission

/// Collection-count bound robustness for the compact wire format.
///
/// `WireEnvelope.decodeCompact(from:)` runs directly on raw bytes pulled off the
/// WebSocket transport. A `.call` frame carries two length-prefixed collections:
/// the `genericSubs` string array and the `args` `Data` array. Each declared
/// count is a varint read from the (untrusted) frame, and the decoder calls
/// `reserveCapacity(count)` before reading any element.
///
/// A count validated only against `Int.max` lets a tiny adversarial frame (a few
/// bytes carrying a large varint) coerce the decoder into a multi-gigabyte
/// allocation — an out-of-band crash (OOM / DoS) on the inbound path, distinct
/// from the byte-over-read cases pinned in `WireTruncationTests`.
///
/// The invariant pinned here: because every element consumes at least one byte
/// from the buffer (a 1-byte varint length of an empty string / `Data`), any
/// declared count larger than the bytes actually remaining is provably
/// unsatisfiable and MUST throw `TransmissionError.decodingFailed` — before any
/// allocation — rather than trapping or attempting the reservation.
@Suite("Wire Collection Count Bounds")
struct WireCountBoundTests {

    /// Builds the fixed prefix of a `.call` frame up to (but not including) the
    /// genericSubs count varint: type byte, call UUID, actor-ID string, absent
    /// node, target string, and priority byte.
    private func callPrefixThroughTarget() -> CompactEncoder {
        var encoder = CompactEncoder()
        encoder.writeUInt8(0)               // envelope type: call
        encoder.writeUUID(UUID())           // call ID
        encoder.writeString("svc")          // recipient actor ID
        encoder.writeOptionalString(nil)    // recipient node absent
        encoder.writeString("do()")         // target
        encoder.writeUInt8(0)               // priority: realtime
        return encoder
    }

    /// A genericSubs count far larger than the remaining bytes must throw, not
    /// attempt a giant `reserveCapacity`. The frame ends right after the count,
    /// so zero bytes remain for any element.
    @Test("Oversized genericSubs count throws before allocating")
    func oversizedGenericSubsCountThrows() {
        var encoder = callPrefixThroughTarget()
        encoder.writeVarint(0xFFFF_FFFF)    // ~4.29 billion declared substitutions
        let raw = encoder.data              // no element bytes follow

        #expect(throws: (any Error).self,
                "A genericSubs count exceeding remaining bytes must throw") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A near-`UInt64.max` genericSubs count (the largest a varint can encode)
    /// must throw rather than overflow or allocate.
    @Test("Near-max genericSubs count throws")
    func nearMaxGenericSubsCountThrows() {
        var encoder = callPrefixThroughTarget()
        encoder.writeVarint(UInt64.max - 1) // absurd declared count
        let raw = encoder.data

        #expect(throws: (any Error).self,
                "A near-UInt64.max genericSubs count must throw") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// An args count far larger than the remaining bytes must throw. Here the
    /// genericSubs section is empty (count 0) and the args count is oversized.
    @Test("Oversized args count throws before allocating")
    func oversizedArgsCountThrows() {
        var encoder = callPrefixThroughTarget()
        encoder.writeVarint(0)              // genericSubs count: 0
        encoder.writeVarint(0xFFFF_FFFF)    // ~4.29 billion declared args
        let raw = encoder.data              // no arg bytes follow

        #expect(throws: (any Error).self,
                "An args count exceeding remaining bytes must throw") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// A near-`UInt64.max` args count must throw.
    @Test("Near-max args count throws")
    func nearMaxArgsCountThrows() {
        var encoder = callPrefixThroughTarget()
        encoder.writeVarint(0)              // genericSubs count: 0
        encoder.writeVarint(UInt64.max - 1) // absurd declared count
        let raw = encoder.data

        #expect(throws: (any Error).self,
                "A near-UInt64.max args count must throw") {
            _ = try WireEnvelope.decodeCompact(from: raw)
        }
    }

    /// The bound must be exact, not over-tight: a count exactly equal to the
    /// bytes remaining is the largest satisfiable value and must NOT be rejected
    /// by the count guard. Here genericSubs declares 3 entries and exactly three
    /// 1-byte empty-string lengths follow, so the frame is well-formed and the
    /// args section (count 0) closes it out.
    @Test("genericSubs count equal to remaining bytes decodes")
    func genericSubsCountAtBoundDecodes() throws {
        var encoder = callPrefixThroughTarget()
        encoder.writeVarint(3)              // three substitutions
        encoder.writeVarint(0)              // sub[0]: empty string (1 byte)
        encoder.writeVarint(0)              // sub[1]: empty string (1 byte)
        encoder.writeVarint(0)              // sub[2]: empty string (1 byte)
        encoder.writeVarint(0)              // args count: 0
        let raw = encoder.data

        let decoded = try WireEnvelope.decodeCompact(from: raw)
        guard case .call(let call) = decoded else {
            Issue.record("Expected a call envelope")
            return
        }
        #expect(call.genericSubs == ["", "", ""])
        #expect(call.args.isEmpty)
    }

    /// A fully well-formed `.call` with real substitutions and args still round
    /// trips — the new guard must not regress the happy path.
    @Test("Well-formed call with subs and args round trips")
    func wellFormedCallRoundTrips() throws {
        let original = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "data-service", node: NodeIdentity(id: "worker-7")),
            target: "process(input:)",
            genericSubs: ["Swift.String", "Swift.Int"],
            args: [Data([0x01, 0x02]), Data(repeating: 0xAB, count: 8)],
            priority: .high
        )
        let raw = WireEnvelope.call(original).encodeCompact()

        let decoded = try WireEnvelope.decodeCompact(from: raw)
        guard case .call(let call) = decoded else {
            Issue.record("Expected a call envelope")
            return
        }
        #expect(call.genericSubs == ["Swift.String", "Swift.Int"])
        #expect(call.args == [Data([0x01, 0x02]), Data(repeating: 0xAB, count: 8)])
        #expect(call.target == "process(input:)")
    }
}
