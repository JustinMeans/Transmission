import Distributed
import Foundation

/// Thread-safe box for capturing results across isolation boundaries.
public final class ResultBox: @unchecked Sendable {
    private var _value: Data = Data()
    private var _failure: RemoteFailure?
    private var _encodingFailed = false
    private let lock = NSLock()

    public init() {}

    public var value: Data {
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

    var failure: RemoteFailure? {
        get { lock.withLock { _failure } }
        set { lock.withLock { _failure = newValue } }
    }

    var encodingFailed: Bool {
        get { lock.withLock { _encodingFailed } }
        set { lock.withLock { _encodingFailed = newValue } }
    }
}

/// What a throwing distributed method left behind.
struct RemoteFailure: Sendable {
    let errorType: String
    let payload: Data?

    func envelope(for call: CallEnvelope) -> WireEnvelope {
        .error(ErrorEnvelope(
            callID: call.callID, sender: call.recipient, code: .thrown,
            errorType: errorType, message: "Remote call threw \(errorType)", payload: payload))
    }
}

/// Handles the result of a distributed method invocation.
public struct TransmissionResultHandler: DistributedTargetInvocationResultHandler {
    public typealias SerializationRequirement = any Codable

    private let callID: CallID
    private let resultBox: ResultBox

    public init(callID: CallID, resultBox: ResultBox) {
        self.callID = callID
        self.resultBox = resultBox
    }

    public func onReturn<Success: Codable>(value: Success) async throws {
        do {
            resultBox.value = try JSONEncoder().encode(value)
        } catch {
            resultBox.encodingFailed = true
            throw error
        }
    }

    public func onReturnVoid() async throws {
        resultBox.value = Data()
    }

    /// Only a `Codable` error's contents cross the wire: conforming is the
    /// statement that they may. Anything else sends its type name alone.
    public func onThrow<Err: Error>(error: Err) async throws {
        let payload = (error as? any Encodable).flatMap { try? JSONEncoder().encode($0) }
        resultBox.failure = RemoteFailure(
            errorType: String(reflecting: type(of: error as Any)),
            payload: payload
        )
    }
}
