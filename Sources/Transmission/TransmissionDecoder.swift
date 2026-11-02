import Distributed
import Foundation

/// Decodes distributed method invocations from wire format.
public final class TransmissionDecoder: DistributedTargetInvocationDecoder, @unchecked Sendable {
    public typealias SerializationRequirement = any Codable

    private let envelope: CallEnvelope
    private let system: TransmissionSystem
    private let decoder: JSONDecoder
    private var argIndex: Int = 0

    public init(envelope: CallEnvelope, system: TransmissionSystem) {
        self.envelope = envelope
        self.system = system
        self.decoder = JSONDecoder()
        self.decoder.userInfo[.transmissionSystem] = system
        // The stdlib's key: without it, actor references can't be arguments.
        self.decoder.userInfo[.actorSystemKey] = system
    }

    public func decodeGenericSubstitutions() throws -> [Any.Type] {
        try envelope.genericSubs.map { name in
            guard let type = _typeByName(name) else {
                throw TransmissionError.decodingFailed("Unknown generic substitution: \(name)")
            }
            return type
        }
    }

    public func decodeNextArgument<Argument: Codable>() throws -> Argument {
        guard argIndex < envelope.args.count else {
            throw TransmissionError.decodingFailed("Not enough arguments: expected \(Argument.self) at index \(argIndex)")
        }

        let data = envelope.args[argIndex]
        argIndex += 1

        return try decoder.decode(Argument.self, from: data)
    }

    public func decodeReturnType() throws -> Any.Type? {
        nil
    }

    public func decodeErrorType() throws -> Any.Type? {
        nil
    }
}
