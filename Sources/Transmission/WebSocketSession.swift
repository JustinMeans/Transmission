import Foundation
import NIO
import NIOWebSocket

/// The frame loop every NIO connection runs, client or server.
///
/// It never awaits a handler: complete messages go to the `InboundDispatcher`
/// and the loop goes straight back to reading.
enum WebSocketSession {
    final class ActivityClock: @unchecked Sendable {
        private let lock = NSLock()
        private let clock = ContinuousClock()
        private var last: ContinuousClock.Instant

        init() {
            last = clock.now
        }

        func touch() {
            let now = clock.now
            lock.withLock { last = now }
        }

        var silence: Duration {
            let last = lock.withLock { self.last }
            return last.duration(to: clock.now)
        }
    }

    static func run(
        inbound: NIOAsyncChannelInboundStream<WebSocketFrame>,
        outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>,
        node: RemoteNode,
        dispatcher: InboundDispatcher,
        maxMessageSize: Int,
        heartbeat: HeartbeatPolicy?
    ) async throws {
        let activity = ActivityClock()
        let writer = UncheckedWriter(outbound: outbound)
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                try await read(
                    inbound: inbound, writer: writer, node: node, dispatcher: dispatcher,
                    maxMessageSize: maxMessageSize, activity: activity)
            }
            if let heartbeat {
                group.addTask {
                    while true {
                        try await Task.sleep(for: heartbeat.interval)
                        try await node.ping()
                    }
                }
                // Separate from the pinger: a ping stuck behind a full socket must
                // not stop the deadline from firing.
                group.addTask {
                    while true {
                        try await Task.sleep(for: min(heartbeat.interval, heartbeat.timeout / 4))
                        if activity.silence >= heartbeat.timeout {
                            throw TransmissionError.connectionFailed("Peer silent for \(heartbeat.timeout)")
                        }
                    }
                }
            }
            defer { group.cancelAll() }
            try await group.next()
        }
    }

    /// `NIOAsyncChannelOutboundWriter` is safe to use from sibling child tasks.
    private struct UncheckedWriter: @unchecked Sendable {
        let outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>
    }

    private static func read(
        inbound: NIOAsyncChannelInboundStream<WebSocketFrame>,
        writer: UncheckedWriter,
        node: RemoteNode,
        dispatcher: InboundDispatcher,
        maxMessageSize: Int,
        activity: ActivityClock
    ) async throws {
        var accumulator = FrameAccumulator(maxMessageSize: maxMessageSize)
        for try await frame in inbound {
            activity.touch()
            switch frame.opcode {
            case .text, .binary, .continuation:
                if let message = try accumulator.feed(frame) {
                    dispatcher.receive(message)
                }
            case .ping:
                try await node.writeControl(.pong, payload: frame.unmaskedData)
            case .pong:
                break
            case .connectionClose:
                // RFC 6455 section 5.5.1: answer a Close with a Close before
                // closing, echoing the peer's payload, or 1002 if it is malformed.
                let echo = webSocketCloseEchoFrame(for: frame)
                try await node.writeControl(.connectionClose, payload: echo.data)
                return
            default:
                break
            }
        }
    }
}
