import Testing
import Foundation
import Distributed
@testable import Transmission

@Suite("TransmissionSystem Tests")
struct TransmissionSystemTests {

    @Test("System initialization with custom ID")
    func systemInitWithID() {
        let system = TransmissionSystem(id: "test-node")
        #expect(system.nodeID.id == "test-node")
    }

    @Test("System initialization with random ID")
    func systemInitRandom() {
        let system1 = TransmissionSystem()
        let system2 = TransmissionSystem()

        #expect(system1.nodeID != system2.nodeID)
    }

    @Test("Server factory method")
    func serverFactory() {
        let system = TransmissionSystem.server(id: "main-server")
        #expect(system.nodeID.id == "main-server")
    }

    @Test("Default connection timeout")
    func defaultConnectionTimeout() {
        let system = TransmissionSystem()
        #expect(system.connectionTimeout == .seconds(30))
    }

    @Test("Custom connection timeout")
    func customConnectionTimeout() {
        let system = TransmissionSystem()
        system.connectionTimeout = .seconds(60)
        #expect(system.connectionTimeout == .seconds(60))
    }

    @Test("Logger is configured with node ID")
    func loggerConfiguration() {
        let system = TransmissionSystem(id: "test-node")
        #expect(system.logger.label.contains("test-node"))
    }

    @Test("Actor ID is created with type info")
    func actorIDCreation() {
        // Test that ActorIdentity can be created with type information
        let id = ActorIdentity.random(for: TestDistributedActor.self)
        #expect(id.typeName?.contains("TestDistributedActor") == true)
    }

    @Test("makeInvocationEncoder returns encoder")
    func makeEncoder() {
        let system = TransmissionSystem()
        let encoder = system.makeInvocationEncoder()

        #expect(encoder is TransmissionEncoder)
    }
}

// Test distributed actor for testing purposes
distributed actor TestDistributedActor {
    typealias ActorSystem = TransmissionSystem

    distributed func greet(name: String) -> String {
        "Hello, \(name)!"
    }
}

@Suite("TransmissionEncoder Tests")
struct TransmissionEncoderTests {

    @Test("Records generic substitutions")
    func recordGenericSubs() throws {
        let system = TransmissionSystem()
        let encoder = TransmissionEncoder(system: system)

        try encoder.recordGenericSubstitution(String.self)
        try encoder.recordGenericSubstitution(Int.self)

        #expect(encoder.genericSubs.count == 2)
    }

    @Test("Records arguments as Data")
    func recordArguments() throws {
        let system = TransmissionSystem()
        let encoder = TransmissionEncoder(system: system)

        try encoder.recordArgument(RemoteCallArgument(label: "name", name: "name", value: "Test"))
        try encoder.recordArgument(RemoteCallArgument(label: "count", name: "count", value: 42))

        #expect(encoder.args.count == 2)
    }

    @Test("Default priority is normal")
    func defaultPriority() {
        let system = TransmissionSystem()
        let encoder = TransmissionEncoder(system: system)

        #expect(encoder.priority == .normal)
    }

    @Test("Priority can be set")
    func setPriority() {
        let system = TransmissionSystem()
        let encoder = TransmissionEncoder(system: system)
            .withPriority(.realtime)

        #expect(encoder.priority == .realtime)
    }
}

@Suite("TransmissionDecoder Tests")
struct TransmissionDecoderTests {

    @Test("Decodes arguments in order")
    func decodeArgumentsInOrder() throws {
        let system = TransmissionSystem()

        let arg1 = try JSONEncoder().encode("Hello")
        let arg2 = try JSONEncoder().encode(42)

        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "test"),
            target: "greet(name:count:)",
            genericSubs: [],
            args: [arg1, arg2],
            priority: .normal
        )

        var decoder = TransmissionDecoder(envelope: envelope, system: system)

        let name: String = try decoder.decodeNextArgument()
        let count: Int = try decoder.decodeNextArgument()

        #expect(name == "Hello")
        #expect(count == 42)
    }

    @Test("Throws when arguments exhausted")
    func throwsWhenExhausted() throws {
        let system = TransmissionSystem()

        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "test"),
            target: "method()",
            genericSubs: [],
            args: [],
            priority: .normal
        )

        var decoder = TransmissionDecoder(envelope: envelope, system: system)

        #expect(throws: TransmissionError.self) {
            let _: String = try decoder.decodeNextArgument()
        }
    }

    @Test("Decodes generic substitutions")
    func decodeGenericSubs() throws {
        let system = TransmissionSystem()

        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "test"),
            target: "method()",
            genericSubs: [_mangledTypeName(String.self)!],
            args: [],
            priority: .normal
        )

        var decoder = TransmissionDecoder(envelope: envelope, system: system)
        let types = try decoder.decodeGenericSubstitutions()

        #expect(types.count == 1)
        #expect(types.first == String.self)
    }
}
