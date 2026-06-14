import Testing
import Foundation
@testable import Transmission

/// Thread-safe counter for use in concurrent test closures.
final class AtomicCounter: @unchecked Sendable {
    private var _value: Int
    private let lock = NSLock()

    init(_ value: Int = 0) {
        self._value = value
    }

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return _value
    }

    @discardableResult
    func increment() -> Int {
        lock.lock()
        defer { lock.unlock() }
        _value += 1
        return _value
    }
}

/// Thread-safe boolean flag for use in concurrent test closures.
final class AtomicBool: @unchecked Sendable {
    private var _value: Bool
    private let lock = NSLock()

    init(_ value: Bool = false) {
        self._value = value
    }

    var value: Bool {
        get {
            lock.lock()
            defer { lock.unlock() }
            return _value
        }
        set {
            lock.lock()
            defer { lock.unlock() }
            _value = newValue
        }
    }
}

/// Thread-safe array for collecting status updates.
final class AtomicArray<T>: @unchecked Sendable {
    private var _values: [T] = []
    private let lock = NSLock()

    func append(_ value: T) {
        lock.lock()
        defer { lock.unlock() }
        _values.append(value)
    }

    var values: [T] {
        lock.lock()
        defer { lock.unlock() }
        return _values
    }

    func contains(where predicate: (T) -> Bool) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return _values.contains(where: predicate)
    }
}

@Suite("ResilientConnection Tests")
struct ResilientConnectionTests {

    @Test("Connection status starts disconnected")
    func initialStatus() async {
        let statusUpdates = AtomicArray<ConnectionStatus>()

        let connection = ResilientConnection(
            onStatusChange: { status in
                statusUpdates.append(status)
            }
        ) {
            try await Task.sleep(for: .seconds(10))
        }

        let status = await connection.currentStatus
        #expect(status == .disconnected)
    }

    @Test("Connection status updates to connecting")
    func connectingStatus() async throws {
        let statusUpdates = AtomicArray<ConnectionStatus>()
        let fulfilled = AtomicBool(false)

        let connection = ResilientConnection(
            onStatusChange: { status in
                statusUpdates.append(status)
                if case .connecting = status {
                    fulfilled.value = true
                }
            }
        ) {
            try await Task.sleep(for: .seconds(10))
        }

        await connection.start()

        // Wait for status update
        try await Task.sleep(for: .milliseconds(500))

        #expect(statusUpdates.contains { status in
            if case .connecting = status { return true }
            return false
        })
    }

    @Test("Stop cancels connection")
    func stopCancelsConnection() async throws {
        let stopped = AtomicBool(false)

        let connection = ResilientConnection {
            defer { stopped.value = true }
            try await Task.sleep(for: .seconds(10))
        }

        await connection.start()
        try await Task.sleep(for: .milliseconds(100))
        await connection.stop()

        let status = await connection.currentStatus
        #expect(status == .disconnected)
    }

    @Test("Clean disconnect does not flash connected status")
    func cleanDisconnectDoesNotFlashConnected() async throws {
        // Regression test: after action() returns without error (clean disconnect),
        // the status must never transition to .connected — the connection is already
        // gone at that point. Before the fix, updateStatus(.connected) was called
        // unconditionally after action() returned, causing a spurious connected flash
        // that misled callers monitoring connection state.
        let statusUpdates = AtomicArray<ConnectionStatus>()

        let connection = ResilientConnection(
            backoff: ExponentialBackoff(initial: 0, minimum: 0.01, maximum: 0.01, jitter: 0),
            onStatusChange: { status in
                statusUpdates.append(status)
            }
        ) {
            // Simulate a connection that establishes and then closes cleanly
            // (returns without throwing) after a brief moment.
            try await Task.sleep(for: .milliseconds(20))
        }

        await connection.start()
        try await Task.sleep(for: .milliseconds(200))
        await connection.stop()

        let updates = statusUpdates.values
        // There must be no .connected transition — that status can only come from
        // an explicit onConnected callback (as ClientManager does). ResilientConnection
        // without an onConnected hook should never emit .connected on its own.
        let connectedUpdates = updates.filter {
            if case .connected = $0 { return true }
            return false
        }
        #expect(connectedUpdates.isEmpty, "Status must not flash .connected after a clean disconnect; got: \(updates)")
    }

    @Test("Failed connection triggers reconnect")
    func failedConnectionReconnects() async throws {
        let attempts = AtomicCounter(0)

        let connection = ResilientConnection(
            backoff: ExponentialBackoff(initial: 0, minimum: 0.01, maximum: 0.1, jitter: 0)
        ) {
            let count = attempts.increment()
            if count >= 3 {
                // Stop trying after 3 attempts
                throw CancellationError()
            }
            throw TransmissionError.connectionFailed("test")
        }

        await connection.start()

        // Wait for multiple attempts
        try await Task.sleep(for: .seconds(1))
        await connection.stop()

        #expect(attempts.value >= 3)
    }

    /// Regression test: stop() must reset backoff so that the next start()
    /// begins with the initial zero delay, not the accumulated delay from the
    /// previous session.
    ///
    /// Before the fix, stop() left backoff.current at its accumulated value
    /// (e.g. 5 s after a long session). The next start() would then sleep for
    /// ~5 s before the first reconnect attempt, violating the contract that a
    /// fresh start always connects immediately.
    ///
    /// The test drives the backoff to its maximum (large delay) via repeated
    /// failures in the first session, then stops and starts a second session.
    /// If backoff was NOT reset the second session's first retry delay would
    /// be the accumulated maximum (0.2 s in this test) and the action would
    /// not be called within 50 ms. With the fix the second session starts
    /// immediately and the action is called within 50 ms.
    @Test("stop() resets backoff so next start() connects immediately")
    func stopResetsBackoffForNextStart() async throws {
        // Use a very small maximum so the backoff saturates quickly.
        // jitter=0 makes the timing deterministic.
        let backoff = ExponentialBackoff(
            initial: 0,
            minimum: 0.05,
            maximum: 0.2,
            multiplier: 2.0,
            jitter: 0
        )

        let firstSessionAttempts = AtomicCounter(0)
        let secondSessionAttempts = AtomicCounter(0)
        let useFirstCounter = AtomicBool(true)

        let connection = ResilientConnection(backoff: backoff) {
            if useFirstCounter.value {
                firstSessionAttempts.increment()
            } else {
                secondSessionAttempts.increment()
            }
            throw TransmissionError.connectionFailed("test")
        }

        // First session: let the backoff saturate to its maximum.
        await connection.start()
        // 4 failures at 0, 0.05, 0.1, 0.2 s intervals = ~0.35 s total; wait 0.5 s
        try await Task.sleep(for: .milliseconds(500))
        await connection.stop()   // fix: resets backoff to 0

        // Switch to the second counter and start a new session.
        useFirstCounter.value = false
        await connection.start()

        // If backoff was reset, the action is called immediately (no sleep).
        // Give it 50 ms — well within the first zero-delay attempt.
        // If backoff was NOT reset the next delay would be 0.2 s and the
        // action would NOT have been called within 50 ms.
        try await Task.sleep(for: .milliseconds(50))
        await connection.stop()

        #expect(
            secondSessionAttempts.value >= 1,
            "Second session must attempt at least once within 50 ms — backoff must be reset on stop()"
        )
    }
}
