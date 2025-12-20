import Distributed
import Foundation

/// Encodes distributed method invocations for wire transmission.
public final class TransmissionEncoder: DistributedTargetInvocationEncoder, @unchecked Sendable {
    public typealias SerializationRequirement = any Codable

    private let system: TransmissionSystem
    private let encoder: JSONEncoder

    var genericSubs: [String] = []
    var args: [Data] = []
    var priority: MessagePriority = .normal

    init(system: TransmissionSystem) {
        self.system = system
        self.encoder = JSONEncoder()
        self.encoder.userInfo[.transmissionSystem] = system
    }

    public func recordGenericSubstitution<T>(_ type: T.Type) throws {
        if let name = _mangledTypeName(T.self) {
            genericSubs.append(name)
        }
    }

    public func recordArgument<Value: Codable>(_ argument: RemoteCallArgument<Value>) throws {
        let data = try encoder.encode(argument.value)
        args.append(data)
    }

    public func recordReturnType<R: Codable>(_ type: R.Type) throws {
        // Not needed for wire protocol
    }

    public func recordErrorType<E: Error>(_ type: E.Type) throws {
        // Not needed for wire protocol
    }

    public func doneRecording() throws {
        // Finalization complete
    }

    /// Sets the priority for this invocation.
    public func withPriority(_ priority: MessagePriority) -> Self {
        self.priority = priority
        return self
    }
}
