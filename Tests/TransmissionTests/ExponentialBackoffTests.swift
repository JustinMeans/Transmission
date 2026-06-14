import Testing
import Foundation
@testable import Transmission

@Suite("ExponentialBackoff Tests")
struct ExponentialBackoffTests {

    @Test("Initial delay is zero")
    func initialDelay() {
        var backoff = ExponentialBackoff()
        let first = backoff.next()!

        #expect(first >= -0.25 && first <= 0.25)
    }

    @Test("Delays increase exponentially")
    func exponentialIncrease() {
        var backoff = ExponentialBackoff(initial: 1.0, minimum: 0.5, maximum: 100, jitter: 0)

        let first = backoff.next()!
        let second = backoff.next()!
        let third = backoff.next()!

        #expect(second > first)
        #expect(third > second)
    }

    @Test("Delays respect maximum")
    func respectsMaximum() {
        var backoff = ExponentialBackoff(initial: 10.0, minimum: 1.0, maximum: 15.0, jitter: 0)

        for _ in 0..<20 {
            let delay = backoff.next()!
            #expect(delay <= 15.0)
        }
    }

    @Test("Reset returns to initial")
    func resetBehavior() {
        var backoff = ExponentialBackoff(initial: 0, jitter: 0)

        _ = backoff.next()
        _ = backoff.next()
        _ = backoff.next()

        backoff.reset()
        let afterReset = backoff.next()!

        #expect(afterReset == 0)
    }

    @Test("Jitter adds variance")
    func jitterVariance() {
        var backoff = ExponentialBackoff(initial: 10.0, jitter: 0.5)

        let delays = (0..<100).compactMap { _ -> Double? in
            var b = backoff
            _ = b.next()
            return b.next()
        }

        let unique = Set(delays)
        #expect(unique.count > 1)
    }

    @Test("Standard preset exists")
    func standardPreset() {
        var backoff = ExponentialBackoff.standard
        #expect(backoff.next() != nil)
    }

    @Test("Aggressive preset has faster growth")
    func aggressivePreset() {
        var standard = ExponentialBackoff.standard
        var aggressive = ExponentialBackoff.aggressive

        _ = standard.next()
        _ = aggressive.next()

        let standardSecond = standard.next()!
        let aggressiveSecond = aggressive.next()!

        #expect(aggressive.maximum < standard.maximum)
    }

    @Test("Sequence protocol conformance")
    func sequenceConformance() {
        let backoff = ExponentialBackoff(initial: 1.0, maximum: 5.0, jitter: 0)
        var count = 0

        for delay in backoff {
            count += 1
            if count > 10 { break }
            #expect(delay <= 5.0)
        }

        #expect(count == 11)
    }

    /// Jitter must not push the returned delay above `maximum`.
    ///
    /// Before the fix, `next()` returned `max(0, value + randomJitter)` without an
    /// upper clamp. At saturation (`current == maximum`), `jitterRange = maximum * jitter`,
    /// so the return value could reach `maximum + maximum * jitter` — e.g. 37.5 s
    /// instead of the declared 30 s cap. This test forces the backoff to saturation
    /// and runs 10 000 samples to confirm no delay exceeds `maximum`.
    @Test("Jitter never pushes delay above maximum")
    func jitterDoesNotExceedMaximum() {
        // Use a high jitter and a low maximum so any overshoot is easily detected.
        let cap = 10.0
        var backoff = ExponentialBackoff(
            initial: cap,       // start already at cap so every call is at saturation
            minimum: 1.0,
            maximum: cap,
            multiplier: 2.0,
            jitter: 0.5         // ±50 % jitter — would add up to 5 s over cap before fix
        )

        for _ in 0..<10_000 {
            let delay = backoff.next()!
            #expect(delay <= cap, "delay \(delay) exceeded maximum \(cap)")
        }
    }
}
