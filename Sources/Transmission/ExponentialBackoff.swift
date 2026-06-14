import Foundation

/// Exponential backoff iterator for retry logic.
public struct ExponentialBackoff: Sendable, Sequence, IteratorProtocol {
    public var current: Double
    public let minimum: Double
    public let maximum: Double
    public let multiplier: Double
    public let jitter: Double

    public init(
        initial: Double = 0,
        minimum: Double = 0.5,
        maximum: Double = 30.0,
        multiplier: Double = 1.618, // Golden ratio
        jitter: Double = 0.25
    ) {
        self.current = initial
        self.minimum = minimum
        self.maximum = maximum
        self.multiplier = multiplier
        self.jitter = jitter
    }

    public static let standard = ExponentialBackoff()

    public static let aggressive = ExponentialBackoff(
        initial: 0,
        minimum: 0.1,
        maximum: 5.0,
        multiplier: 2.0,
        jitter: 0.1
    )

    public mutating func next() -> Double? {
        let value = current
        current = Swift.min(Swift.max(current * multiplier, minimum), maximum)

        let jitterRange = value * jitter
        let randomJitter = Double.random(in: -jitterRange...jitterRange)

        // Clamp to [0, maximum]: jitter is applied after the maximum clamp on
        // `current`, so without this clamp the returned delay can exceed `maximum`
        // by up to `maximum * jitter` seconds — breaking the contract of the
        // parameter (e.g. standard backoff can return up to 37.5 s instead of 30 s).
        return Swift.min(maximum, Swift.max(0, value + randomJitter))
    }

    public mutating func reset() {
        current = 0
    }
}
