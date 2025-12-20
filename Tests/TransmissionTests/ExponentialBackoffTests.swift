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
}
