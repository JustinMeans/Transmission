import Distributed
import Foundation

/// Thread-safe box for capturing results across isolation boundaries.
public final class ResultBox: @unchecked Sendable {
    private var _value: Data = Data()
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
        let encoder = JSONEncoder()
        let data = try encoder.encode(value)
        resultBox.value = data
    }

    public func onReturnVoid() async throws {
        resultBox.value = Data()
    }

    public func onThrow<Err: Error>(error: Err) async throws {
        resultBox.value = Data()
    }
}
