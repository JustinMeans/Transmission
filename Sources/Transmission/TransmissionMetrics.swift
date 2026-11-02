import Foundation
import Metrics

/// Metrics collection for Transmission.
public final class TransmissionMetrics: @unchecked Sendable {
    private let nodeID: NodeIdentity

    private let callCounter: Counter
    private let callDuration: Timer
    private let activeConnections: Gauge
    private let messagesSent: Counter
    private let messagesReceived: Counter
    private let bytesIn: Counter
    private let bytesOut: Counter
    private let errors: Counter
    private let lock = NSLock()
    private var connectionCount = 0

    init(nodeID: NodeIdentity) {
        self.nodeID = nodeID

        let labels = [("node", nodeID.id)]

        self.callCounter = Counter(label: "transmission.calls.total", dimensions: labels)
        self.callDuration = Timer(label: "transmission.calls.duration", dimensions: labels)
        self.activeConnections = Gauge(label: "transmission.connections.active", dimensions: labels)
        self.messagesSent = Counter(label: "transmission.messages.sent", dimensions: labels)
        self.messagesReceived = Counter(label: "transmission.messages.received", dimensions: labels)
        self.bytesIn = Counter(label: "transmission.bytes.in", dimensions: labels)
        self.bytesOut = Counter(label: "transmission.bytes.out", dimensions: labels)
        self.errors = Counter(label: "transmission.errors.total", dimensions: labels)
    }

    func recordCall(target: String) {
        callCounter.increment()
    }

    func recordCallDuration(_ duration: Duration, target: String) {
        let c = duration.components
        // Combine whole-second and sub-second parts. Using only c.attoseconds (the
        // sub-second component) previously discarded the seconds portion entirely,
        // making any call longer than ~1 s report its fractional remainder instead
        // of its true elapsed time.
        let nanoseconds = c.seconds * 1_000_000_000 + c.attoseconds / 1_000_000_000
        callDuration.recordNanoseconds(nanoseconds)
    }

    /// `Gauge.record` sets the value rather than adding to it.
    public func connectionOpened() {
        activeConnections.record(adjustConnections(by: 1))
    }

    public func connectionClosed() {
        activeConnections.record(adjustConnections(by: -1))
    }

    private func adjustConnections(by delta: Int) -> Int {
        lock.withLock {
            connectionCount += delta
            return connectionCount
        }
    }

    var activeConnectionCount: Int {
        lock.withLock { connectionCount }
    }

    func recordMessageSent(bytes: Int) {
        messagesSent.increment()
        bytesOut.increment(by: bytes)
    }

    func recordMessageReceived(bytes: Int) {
        messagesReceived.increment()
        bytesIn.increment(by: bytes)
    }

    func recordError(_ error: any Error) {
        errors.increment()
    }
}
