import Foundation
import NIOConcurrencyHelpers
import Testing
import TransmissionTestSupport
@testable import TransmissionLive

/// Latency samples in milliseconds.
final class Samples: @unchecked Sendable {
    private let box = NIOLockedValueBox<[Double]>([])
    func add(_ value: Double) { box.withLockedValue { $0.append(value) } }
    func take() -> [Double] { box.withLockedValue { values in defer { values = [] }; return values } }
}

/// Reads a healthy client's updates and records publish-to-consumer latency.
func measure(_ subscription: LiveSubscription<Tick, Tick>, into samples: Samples) -> Task<Void, Never> {
    Task {
        var last: UInt64 = 0
        for await update in subscription.updates where update.freshness == .live {
            guard update.state.sentAt != last, update.state.sentAt != 0 else { continue }
            last = update.state.sentAt
            samples.add(Double(nowNanos() - update.state.sentAt) / 1_000_000)
        }
    }
}

struct LoadPhase {
    let p50: Double
    let p99: Double
    let count: Int

    init(_ values: [Double]) {
        p50 = percentile(values, 50)
        p99 = percentile(values, 99)
        count = values.count
    }
}

@Suite("Load", .serialized)
struct LiveLoadTests {
    static let healthyCount = 70
    static let stalledCount = 15
    static let flappingCount = 15

    private func publish(_ hub: LiveHub, hertz: Int, for duration: Duration, padding: String = "") async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: duration)
        let interval = Duration.nanoseconds(1_000_000_000 / hertz)
        var next = clock.now
        while clock.now < deadline {
            try await hub.publish(tickTopic, Tick(sentAt: nowNanos(), padding: padding))
            next = next.advanced(by: interval)
            try await clock.sleep(until: next)
        }
    }

    @Test("100 clients in memory: stalled and flapping peers do not move healthy latency")
    func inMemory() async throws {
        var configuration = fastConfiguration()
        configuration.stallLimit = .milliseconds(300)
        let hub = LiveHub(configuration: configuration, authorizer: AllowAll())
        try await hub.host(tickTopic, initial: Tick(sentAt: 0))

        let samples = Samples()
        var healthy: [LiveClient] = []
        var transports: [LoopbackTransport] = []
        var readers: [Task<Void, Never>] = []
        for _ in 0..<Self.healthyCount {
            let transport = LoopbackTransport(hub: hub)
            let client = LiveClient(configuration: clientConfiguration(), transport: transport)
            await client.start()
            readers.append(measure(await client.subscribe(tickTopic), into: samples))
            healthy.append(client)
            transports.append(transport)
        }
        let ready = healthy
        await eventually(within: .seconds(10)) {
            for client in ready where await client.state != .connected { return false }
            return true
        }
        await eventually(within: .seconds(10)) { await hub.subscriberCount(tickTopic.key) == Self.healthyCount }

        try await publish(hub, hertz: 50, for: .seconds(2))
        try await Task.sleep(for: .milliseconds(50))
        let baseline = LoadPhase(samples.take())

        // Stalled: the socket hangs right after the snapshot, then reconnects and
        // hangs again, so the hub meets a fresh stall every 300 ms.
        var stalled: [LiveClient] = []
        var stalledTransports: [LoopbackTransport] = []
        for _ in 0..<Self.stalledCount {
            let transport = LoopbackTransport(hub: hub)
            transport.stallAfterWrites = 3
            let client = LiveClient(configuration: clientConfiguration(), transport: transport)
            await client.start()
            _ = await client.subscribe(tickTopic)
            stalled.append(client)
            stalledTransports.append(transport)
        }
        // Flapping: connect, subscribe, drop, repeat.
        let flapCycles = Counter()
        let flappers = (0..<Self.flappingCount).map { _ in
            Task {
                let transport = LoopbackTransport(hub: hub)
                let client = LiveClient(configuration: clientConfiguration(), transport: transport)
                await client.start()
                _ = await client.subscribe(tickTopic)
                while !Task.isCancelled {
                    try? await Task.sleep(for: .milliseconds(Int.random(in: 20...80)))
                    await client.reconnectNow(dropExisting: true)
                    flapCycles.increment()
                }
                await client.stop()
            }
        }

        try await Task.sleep(for: .milliseconds(100))
        _ = samples.take()
        try await publish(hub, hertz: 50, for: .seconds(2))
        try await Task.sleep(for: .milliseconds(50))
        let adversarial = LoadPhase(samples.take())

        flappers.forEach { $0.cancel() }
        for task in flappers { await task.value }
        let slowDrops = stalledTransports.reduce(0) { $0 + $1.attempts } - Self.stalledCount
        print("LOAD in-memory baseline p50=\(baseline.p50)ms p99=\(baseline.p99)ms n=\(baseline.count)")
        print("LOAD in-memory adversarial p50=\(adversarial.p50)ms p99=\(adversarial.p99)ms n=\(adversarial.count) flaps=\(flapCycles.value) slowDrops=\(slowDrops)")

        for client in healthy { #expect(await client.state == .connected) }
        #expect(transports.allSatisfy { $0.attempts == 1 }, "a healthy client reconnected")
        #expect(baseline.count > Self.healthyCount * 40)
        #expect(adversarial.count > Int(Double(baseline.count) * 0.9), "healthy clients lost updates")
        #expect(flapCycles.value >= 50)
        #expect(slowDrops >= Self.stalledCount, "every stalled client was dropped at least once")
        // Debug build on loopback, so it compares against itself. The slack is
        // 5 ms because one scheduler quantum at p99 is not a regression.
        #expect(adversarial.p99 <= max(baseline.p99 * 3, baseline.p99 + 5),
                "p99 moved: \(baseline.p99) -> \(adversarial.p99)")

        for client in healthy + stalled { await client.stop() }
        readers.forEach { $0.cancel() }
    }
}

final class Counter: @unchecked Sendable {
    private let box = NIOLockedValueBox(0)
    var value: Int { box.withLockedValue { $0 } }
    func increment() { box.withLockedValue { $0 += 1 } }
}
