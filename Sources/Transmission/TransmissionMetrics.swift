import Foundation
import Metrics

/// Metrics collection for Transmission.
public final class TransmissionMetrics: Sendable {
    private let nodeID: NodeIdentity

    private let callCounter: Counter
    private let callDuration: Timer
    private let activeConnections: Gauge
    private let messagesSent: Counter
    private let messagesReceived: Counter
    private let bytesIn: Counter
    private let bytesOut: Counter
    private let errors: Counter

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
        callDuration.recordNanoseconds(Int64(duration.components.attoseconds / 1_000_000_000))
    }

    func connectionOpened() {
        activeConnections.record(1)
    }

    func connectionClosed() {
        activeConnections.record(-1)
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
