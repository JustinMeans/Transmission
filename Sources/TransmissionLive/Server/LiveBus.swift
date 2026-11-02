import Foundation
import NIOConcurrencyHelpers

public struct LiveBusMessage: Sendable {
    public let key: TopicKey
    public let sampleTimeMs: UInt64
    public let payload: Data

    public init(key: TopicKey, sampleTimeMs: UInt64, payload: Data) {
        self.key = key
        self.sampleTimeMs = sampleTimeMs
        self.payload = payload
    }
}

/// The seam for running more than one replica. Everything published passes
/// through here, and a hub only applies what the bus delivers back to it, so a
/// Redis Streams implementation slots in without touching `LiveHub`.
public protocol LiveBus: Sendable {
    func publish(_ message: LiveBusMessage) async throws
    func subscribe(_ handler: @escaping @Sendable (LiveBusMessage) async -> Void) async
}

/// The only implementation today: production runs one replica.
public final class InProcessLiveBus: LiveBus {
    private let handlers = NIOLockedValueBox<[@Sendable (LiveBusMessage) async -> Void]>([])

    public init() {}

    public func publish(_ message: LiveBusMessage) async throws {
        for handler in handlers.withLockedValue({ $0 }) {
            await handler(message)
        }
    }

    public func subscribe(_ handler: @escaping @Sendable (LiveBusMessage) async -> Void) async {
        handlers.withLockedValue { $0.append(handler) }
    }
}
