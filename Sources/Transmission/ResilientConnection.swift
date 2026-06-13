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

    /// Stops the connection.
    public func stop() {
        task?.cancel()
        task = nil
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

            do {
                try await action()
                // action() returned normally, meaning the connection was established
                // and has since closed cleanly. Reset backoff for the next attempt
                // but do NOT flash .connected here — the connection is already gone.
                backoff.reset()
                attempt = 0
            } catch is CancellationError {
                break
            } catch {
                updateStatus(.failed(error.localizedDescription))
            }

            // Wait before retry
            if let delay = backoff.next(), delay > 0 {
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
