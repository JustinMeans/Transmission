import Foundation
import Logging

/// Connection status for monitoring.
public enum ConnectionStatus: Sendable, Equatable {
    case disconnected
    case connecting
    case connected
    case reconnecting(attempt: Int)
    case failed(String)
}

/// A resilient connection that automatically reconnects on failure.
public actor ResilientConnection {
    public typealias ConnectionAction = @Sendable () async throws -> Void

    private let action: ConnectionAction
    private let statusHandler: (@Sendable (ConnectionStatus) async -> Void)?
    private var backoff: ExponentialBackoff
    private var task: Task<Void, Never>?
    private var status: ConnectionStatus = .disconnected

    public init(
        backoff: ExponentialBackoff = .standard,
        onStatusChange: (@Sendable (ConnectionStatus) async -> Void)? = nil,
        action: @escaping ConnectionAction
    ) {
        self.backoff = backoff
        self.statusHandler = onStatusChange
        self.action = action
    }

    /// Starts the connection loop.
    public func start() {
        guard task == nil else { return }

        task = Task { [weak self] in
            guard let self else { return }
            await self.connectionLoop()
        }
    }

    /// Stops the connection and resets backoff so that the next `start()` call
    /// begins with the initial zero delay rather than inheriting accumulated
    /// backoff state from the previous session.
    public func stop() {
        task?.cancel()
        task = nil
        backoff.reset()
        updateStatus(.disconnected)
    }

    /// Returns the current connection status.
    public var currentStatus: ConnectionStatus {
        status
    }

    private func connectionLoop() async {
        var attempt = 0

        while !Task.isCancelled {
            attempt += 1
            updateStatus(attempt == 1 ? .connecting : .reconnecting(attempt: attempt))

            var cleanExit = false
            do {
                try await action()
                // action() returned normally, meaning the connection was established
                // and has since closed cleanly. Reset backoff so that any subsequent
                // failure retries start from the initial zero delay rather than from
                // the cursor position left by the previous session's error sequence.
                //
                // Crucially we do NOT call backoff.next() on the clean-exit path:
                // next() advances the internal cursor even when the returned delay is
                // zero (it moves current from 0 to minimum). Calling it here would
                // mean the *first* error in the next session receives a minimum-delay
                // penalty instead of the intended immediate retry.
                backoff.reset()
                attempt = 0
                cleanExit = true
            } catch is CancellationError {
                break
            } catch {
                updateStatus(.failed(error.localizedDescription))
            }

            // Wait before retry — skip on clean exit because backoff was just reset
            // to zero and advancing the cursor here would silently penalise the first
            // failure of the next connection session (see comment above).
            if !cleanExit, let delay = backoff.next(), delay > 0 {
                do {
                    try await Task.sleep(for: .seconds(delay))
                } catch {
                    break
                }
            }
        }

        updateStatus(.disconnected)
    }

    private func updateStatus(_ newStatus: ConnectionStatus) {
        status = newStatus
        if let handler = statusHandler {
            Task {
                await handler(newStatus)
            }
        }
    }
}
