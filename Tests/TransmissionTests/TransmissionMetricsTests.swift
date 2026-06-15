import Testing
import Foundation
@testable import Transmission

// ---------------------------------------------------------------------------
// TransmissionMetrics – nanosecond duration calculation
//
// Root-cause: `recordCallDuration` previously computed nanoseconds as:
//
//     Int64(duration.components.attoseconds / 1_000_000_000)
//
// `Duration.components.attoseconds` is the *sub-second* attosecond remainder
// (0 to 999_999_999_999_999_999), NOT total elapsed attoseconds.  The whole-
// second part lives in `duration.components.seconds` and was silently
// discarded, causing any call longer than ~1 s to be reported as its
// fractional remainder only.
//
// Example: a 2.5-second call
//   .seconds    = 2
//   .attoseconds = 500_000_000_000_000_000   (0.5 s in attoseconds)
//   Bug:  500_000_000 ns  (0.5 s)   ← wrong: drops the 2 s
//   Fix: 2_500_000_000 ns (2.5 s)   ← correct
//
// Mutation check: `#expect(buggyValue != expectedNs)` documents that the old
// single-expression formula produces an observably wrong answer.  After the
// fix the formula in `TransmissionMetrics.recordCallDuration` reads:
//   c.seconds * 1_000_000_000 + c.attoseconds / 1_000_000_000
// ---------------------------------------------------------------------------

@Suite("TransmissionMetrics – duration nanoseconds")
struct TransmissionMetricsTests {

    // Converts a Duration to nanoseconds using the FIXED formula.
    private func nanoseconds(for duration: Duration) -> Int64 {
        let c = duration.components
        return c.seconds * 1_000_000_000 + c.attoseconds / 1_000_000_000
    }

    // Converts a Duration to nanoseconds using the BUGGY formula (pre-fix).
    private func buggyNanoseconds(for duration: Duration) -> Int64 {
        let c = duration.components
        return Int64(c.attoseconds / 1_000_000_000)
    }

    // -----------------------------------------------------------------------
    // 1. Sub-second duration — both formulas agree (regression guard).
    // -----------------------------------------------------------------------
    @Test("Sub-second duration: both formulas produce the same nanoseconds")
    func subSecondDurationAgreement() {
        let duration = Duration.milliseconds(750)   // 0.75 s
        let expected: Int64 = 750_000_000           // 750 ms in ns

        #expect(nanoseconds(for: duration) == expected)
        #expect(buggyNanoseconds(for: duration) == expected)
    }

    // -----------------------------------------------------------------------
    // 2. Multi-second duration — the two formulas diverge.
    //    The BUGGY formula returns only the fractional part; the fixed formula
    //    returns the complete elapsed nanoseconds.
    // -----------------------------------------------------------------------
    @Test("2.5-second call: fixed formula returns full nanoseconds, buggy drops seconds")
    func multiSecondDurationFixed() {
        let duration = Duration.seconds(2) + Duration.milliseconds(500)
        let expectedNs: Int64 = 2_500_000_000   // 2.5 s in ns

        // Fixed formula — must equal 2.5 s expressed in nanoseconds.
        #expect(nanoseconds(for: duration) == expectedNs)

        // Mutation check: the OLD formula returns 500_000_000 (0.5 s), not 2.5 s.
        let buggyResult = buggyNanoseconds(for: duration)
        #expect(buggyResult != expectedNs,
            "Mutation check: buggy formula must NOT equal the correct nanosecond value")
        #expect(buggyResult == 500_000_000,
            "Mutation check: buggy formula returns only the sub-second component")
    }

    // -----------------------------------------------------------------------
    // 3. Whole-second duration with no sub-second component.
    // -----------------------------------------------------------------------
    @Test("Whole-second duration: fixed formula accounts for full seconds")
    func wholeSecondDuration() {
        let duration = Duration.seconds(5)          // 5.0 s, zero sub-second part
        let expectedNs: Int64 = 5_000_000_000

        #expect(nanoseconds(for: duration) == expectedNs)

        // Buggy formula: attoseconds == 0 → returns 0 ns for a 5-second call.
        let buggyResult = buggyNanoseconds(for: duration)
        #expect(buggyResult == 0,
            "Mutation check: buggy formula returns 0 for whole-second durations")
        #expect(buggyResult != expectedNs)
    }

    // -----------------------------------------------------------------------
    // 4. Very short (sub-millisecond) duration — nanosecond precision is
    //    preserved and both formulas agree.
    // -----------------------------------------------------------------------
    @Test("Nanosecond-precision sub-millisecond duration round-trips correctly")
    func nanosecondPrecision() {
        // 123_456_789 ns = 0.123456789 s (sub-second only)
        let ns: Int64 = 123_456_789
        let duration = Duration.nanoseconds(ns)

        #expect(nanoseconds(for: duration) == ns)
        #expect(buggyNanoseconds(for: duration) == ns)
    }

    // -----------------------------------------------------------------------
    // 5. Large multi-second duration — ensures no silent integer truncation.
    // -----------------------------------------------------------------------
    @Test("30-second duration: no integer truncation in nanosecond calculation")
    func largeSecondDuration() {
        let duration = Duration.seconds(30) + Duration.milliseconds(250)
        let expectedNs: Int64 = 30_250_000_000

        #expect(nanoseconds(for: duration) == expectedNs)
        // Buggy result would be 250_000_000 (0.25 s only).
        #expect(buggyNanoseconds(for: duration) != expectedNs)
    }
}
