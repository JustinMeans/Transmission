#if !canImport(FoundationNetworking)
import Foundation
import NIOConcurrencyHelpers

/// `URLSessionWebSocketTask` on Apple platforms: it masks, handles TLS and
/// proxies, and accepts headers. Keep NIO for servers and any Linux client.
public struct URLSessionLiveTransport: LiveTransport {
    private let configuration: URLSessionConfiguration

    public init(configuration: URLSessionConfiguration = .default) {
        self.configuration = configuration
    }

    public func connect(_ request: LiveConnectRequest) async throws -> any LiveTransportConnection {
        var urlRequest = URLRequest(url: request.url)
        for (name, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }
        urlRequest.setValue(request.subprotocols.joined(separator: ", "), forHTTPHeaderField: "Sec-WebSocket-Protocol")

        let connection = URLSessionLiveConnection()
        let session = URLSession(configuration: configuration, delegate: connection, delegateQueue: nil)
        let task = session.webSocketTask(with: urlRequest)
        // The default is 1 MiB, and a message over it fails the whole socket.
        task.maximumMessageSize = request.maxMessageSize
        connection.bind(session: session, task: task)
        try await connection.waitForOpen {
            task.resume()
        }
        return connection
    }
}

final class URLSessionLiveConnection: NSObject, URLSessionWebSocketDelegate, URLSessionTaskDelegate, LiveTransportConnection, @unchecked Sendable {
    private struct State {
        var session: URLSession?
        var task: URLSessionWebSocketTask?
        var open: CheckedContinuation<Void, any Error>?
        var didOpen = false
    }

    private let state = NIOLockedValueBox(State())

    func bind(session: URLSession, task: URLSessionWebSocketTask) {
        state.withLockedValue {
            $0.session = session
            $0.task = task
        }
    }

    func waitForOpen(_ start: () -> Void) async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                state.withLockedValue { $0.open = continuation }
                start()
            }
        } onCancel: {
            self.finishOpening(with: CancellationError())
            self.teardown(.goingAway)
        }
    }

    private func finishOpening(with error: (any Error)?) {
        let continuation = state.withLockedValue { state -> CheckedContinuation<Void, any Error>? in
            if error == nil { state.didOpen = true }
            defer { state.open = nil }
            return state.open
        }
        if let error {
            continuation?.resume(throwing: error)
        } else {
            continuation?.resume()
        }
    }

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didOpenWithProtocol protocol: String?
    ) {
        finishOpening(with: nil)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: (any Error)?) {
        if let response = task.response as? HTTPURLResponse, response.statusCode != 101 {
            finishOpening(with: LiveTransportError.rejected(status: response.statusCode))
        } else {
            finishOpening(with: LiveTransportError.connectFailed(error?.localizedDescription ?? "closed"))
        }
    }

    func send(_ data: Data) async throws {
        guard let task = state.withLockedValue({ $0.task }) else {
            throw LiveTransportClosed(code: nil)
        }
        try await task.send(.data(data))
    }

    func receive() async throws -> Data {
        guard let task = state.withLockedValue({ $0.task }) else {
            throw LiveTransportClosed(code: nil)
        }
        do {
            switch try await task.receive() {
            case .data(let data):
                return data
            case .string(let text):
                return Data(text.utf8)
            @unknown default:
                return Data()
            }
        } catch {
            let raw = UInt16(clamping: task.closeCode.rawValue)
            throw LiveTransportClosed(code: raw == 0 ? nil : LiveCloseCode(rawValue: raw))
        }
    }

    func close(code: LiveCloseCode) async {
        teardown(URLSessionWebSocketTask.CloseCode(rawValue: Int(code.rawValue)) ?? .normalClosure)
    }

    private func teardown(_ code: URLSessionWebSocketTask.CloseCode) {
        let (task, session) = state.withLockedValue { state in
            defer {
                state.task = nil
                state.session = nil
            }
            return (state.task, state.session)
        }
        task?.cancel(with: code, reason: nil)
        // Breaks the session's strong reference to this delegate.
        session?.finishTasksAndInvalidate()
    }
}
#endif
