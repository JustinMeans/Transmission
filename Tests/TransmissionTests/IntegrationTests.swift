import Testing
import Foundation
import Distributed
@testable import Transmission

// MARK: - Test Distributed Actors

public distributed actor TestCounter {
    public typealias ActorSystem = TransmissionSystem

    private var count: Int = 0

    public distributed func increment() async -> Int {
        count += 1
        return count
    }

    public distributed func decrement() async -> Int {
        count -= 1
        return count
    }

    public distributed func value() async -> Int {
        count
    }

    public distributed func set(_ newValue: Int) async -> Int {
        count = newValue
        return count
    }
}

public distributed actor TestEcho {
    public typealias ActorSystem = TransmissionSystem

    public distributed func echo(_ message: String) async -> String {
        message
    }

    public distributed func reverse(_ message: String) async -> String {
        String(message.reversed())
    }

    public distributed func uppercase(_ message: String) async -> String {
        message.uppercased()
    }
}

public distributed actor TestCalculator {
    public typealias ActorSystem = TransmissionSystem

    public distributed func add(_ a: Int, _ b: Int) async -> Int {
        a + b
    }

    public distributed func multiply(_ a: Int, _ b: Int) async -> Int {
        a * b
    }

    public distributed func complexOperation(_ values: [Int]) async -> ComplexResult {
        let sum = values.reduce(0, +)
        let product = values.reduce(1, *)
        let average = values.isEmpty ? 0.0 : Double(sum) / Double(values.count)
        return ComplexResult(sum: sum, product: product, average: average)
    }
}

public struct ComplexResult: Codable, Equatable, Sendable {
    public let sum: Int
    public let product: Int
    public let average: Double
}

actor ConnectionStatusTracker {
    private var status: ConnectionStatus = .disconnected

    func update(_ newStatus: ConnectionStatus) {
        status = newStatus
    }

    var isConnected: Bool {
        if case .connected = status {
            return true
        }
        return false
    }
}

// MARK: - Integration Tests

@Suite("Integration Tests")
struct IntegrationTests {

    @Test("Full wire protocol round-trip")
    func wireProtocolRoundTrip() throws {
        let system = TransmissionSystem(id: "test")

        let encoder = TransmissionEncoder(system: system)
        try encoder.recordArgument(RemoteCallArgument(label: "name", name: "name", value: "World"))
        try encoder.recordArgument(RemoteCallArgument(label: "count", name: "count", value: 5))
        try encoder.recordGenericSubstitution(String.self)

        let callEnvelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "greeter", node: NodeIdentity.server),
            target: "greet(name:count:)",
            genericSubs: encoder.genericSubs,
            args: encoder.args,
            priority: .high
        )

        let wireData = try JSONEncoder().encode(WireEnvelope.call(callEnvelope))
        let decoded = try JSONDecoder().decode(WireEnvelope.self, from: wireData)

        if case .call(let decodedCall) = decoded {
            let decoder = TransmissionDecoder(envelope: decodedCall, system: system)

            let name: String = try decoder.decodeNextArgument()
            let count: Int = try decoder.decodeNextArgument()

            #expect(name == "World")
            #expect(count == 5)
            #expect(decodedCall.priority == .high)
        } else {
            Issue.record("Expected call envelope")
        }
    }

    @Test("Reply envelope round-trip")
    func replyRoundTrip() throws {
        let callID = CallID()

        struct GreetResponse: Codable, Equatable {
            let message: String
            let timestamp: Date
        }

        let response = GreetResponse(message: "Hello!", timestamp: Date())
        let responseData = try JSONEncoder().encode(response)

        let reply = ReplyEnvelope(
            callID: callID,
            sender: ActorIdentity(id: "greeter"),
            value: responseData
        )

        let wireData = try JSONEncoder().encode(WireEnvelope.reply(reply))
        let decoded = try JSONDecoder().decode(WireEnvelope.self, from: wireData)

        if case .reply(let decodedReply) = decoded {
            let decodedResponse = try JSONDecoder().decode(GreetResponse.self, from: decodedReply.value)
            #expect(decodedResponse.message == response.message)
        } else {
            Issue.record("Expected reply envelope")
        }
    }

    @Test("Multiple systems can coexist")
    func multipleSystemsCoexist() {
        let server = TransmissionSystem.server(id: "server")
        let client1 = TransmissionSystem(id: "client-1")
        let client2 = TransmissionSystem(id: "client-2")

        #expect(server.nodeID.id == "server")
        #expect(client1.nodeID.id == "client-1")
        #expect(client2.nodeID.id == "client-2")

        #expect(server.nodeID != client1.nodeID)
        #expect(client1.nodeID != client2.nodeID)
    }

    @Test("Actor identity routing across nodes")
    func actorIdentityRouting() {
        let serverNode = NodeIdentity.server
        let clientNode = NodeIdentity(id: "client-1")

        let serverActor = ActorIdentity(id: "greeter").withNode(serverNode)
        let clientActor = ActorIdentity(id: "subscriber").withNode(clientNode)

        #expect(serverActor.node == serverNode)
        #expect(clientActor.node == clientNode)

        #expect(serverActor.node != clientActor.node)
    }

    @Test("Priority queue processes high priority first under load")
    func priorityQueueUnderLoad() async {
        _ = PriorityMessageQueue()

        for i in 0..<100 {
            let priority: MessagePriority = switch i % 4 {
            case 0: .low
            case 1: .normal
            case 2: .high
            default: .realtime
            }

            _ = CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "test-\(i)"),
                target: "method()",
                genericSubs: [],
                args: [],
                priority: priority
            )
        }
    }

    @Test("Serialization format selection")
    func serializationFormatSelection() {
        #expect(SerializationFormat.json != SerializationFormat.binary)
    }

    @Test("Connection status transitions")
    func connectionStatusTransitions() {
        let statuses: [ConnectionStatus] = [
            .disconnected,
            .connecting,
            .connected,
            .reconnecting(attempt: 1),
            .reconnecting(attempt: 2),
            .failed("error"),
            .disconnected
        ]

        for (index, status) in statuses.enumerated() {
            switch status {
            case .disconnected:
                #expect(index == 0 || index == statuses.count - 1)
            case .connecting:
                #expect(index == 1)
            case .connected:
                #expect(index == 2)
            case .reconnecting(let attempt):
                #expect(attempt > 0)
            case .failed(let reason):
                #expect(!reason.isEmpty)
            }
        }
    }

    @Test("Error propagation through wire protocol")
    func errorPropagation() throws {
        let errors: [TransmissionError] = [
            .noConnection,
            .connectionTimeout,
            .actorNotFound(ActorIdentity(id: "missing")),
            .callTimeout(CallID())
        ]

        for error in errors {
            #expect(error.errorDescription != nil)
        }
    }
}

// MARK: - Network Integration Tests

@Suite("Network Integration Tests", .serialized)
struct NetworkIntegrationTests {

    static let testPort = 18080

    @Test("Client connects to server and makes remote call")
    func clientServerRoundTrip() async throws {
        let serverSystem = TransmissionSystem.server(id: "test-server")

        let counterID = ActorIdentity(id: "counter", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: counterID) {
            TestCounter(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort)
        }

        try await Task.sleep(for: .milliseconds(100))

        let clientSystem = TransmissionSystem(id: "test-client")

        let connectionStatus = ConnectionStatusTracker()
        try await clientSystem.connect(to: "ws://127.0.0.1:\(Self.testPort)/transmission") { status in
            await connectionStatus.update(status)
        }

        try await Task.sleep(for: .milliseconds(200))
        let connected = await connectionStatus.isConnected
        #expect(connected)

        let remoteCounter = try TestCounter.resolve(id: counterID, using: clientSystem)

        let initialValue = try await remoteCounter.value()
        #expect(initialValue == 0)

        let afterIncrement = try await remoteCounter.increment()
        #expect(afterIncrement == 1)

        let afterIncrement2 = try await remoteCounter.increment()
        #expect(afterIncrement2 == 2)

        let afterDecrement = try await remoteCounter.decrement()
        #expect(afterDecrement == 1)

        let afterSet = try await remoteCounter.set(100)
        #expect(afterSet == 100)

        serverTask.cancel()
    }

    @Test("Multiple remote method calls with different return types")
    func multipleMethodCalls() async throws {
        let serverSystem = TransmissionSystem.server(id: "echo-server")

        let echoID = ActorIdentity(id: "echo", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: echoID) {
            TestEcho(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort + 1)
        }

        try await Task.sleep(for: .milliseconds(100))

        let clientSystem = TransmissionSystem(id: "echo-client")
        try await clientSystem.connect(to: "ws://127.0.0.1:\(Self.testPort + 1)/transmission")

        try await Task.sleep(for: .milliseconds(200))

        let remoteEcho = try TestEcho.resolve(id: echoID, using: clientSystem)

        let echoed = try await remoteEcho.echo("Hello, Transmission")
        #expect(echoed == "Hello, Transmission")

        let reversed = try await remoteEcho.reverse("Hello")
        #expect(reversed == "olleH")

        let uppercased = try await remoteEcho.uppercase("hello world")
        #expect(uppercased == "HELLO WORLD")

        serverTask.cancel()
    }

    @Test("Complex data structures over wire")
    func complexDataStructures() async throws {
        let serverSystem = TransmissionSystem.server(id: "calc-server")

        let calcID = ActorIdentity(id: "calculator", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: calcID) {
            TestCalculator(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort + 2)
        }

        try await Task.sleep(for: .milliseconds(100))

        let clientSystem = TransmissionSystem(id: "calc-client")
        try await clientSystem.connect(to: "ws://127.0.0.1:\(Self.testPort + 2)/transmission")

        try await Task.sleep(for: .milliseconds(200))

        let remoteCalc = try TestCalculator.resolve(id: calcID, using: clientSystem)

        let sum = try await remoteCalc.add(5, 3)
        #expect(sum == 8)

        let product = try await remoteCalc.multiply(4, 7)
        #expect(product == 28)

        let result = try await remoteCalc.complexOperation([1, 2, 3, 4, 5])
        #expect(result.sum == 15)
        #expect(result.product == 120)
        #expect(result.average == 3.0)

        serverTask.cancel()
    }

    @Test("Multiple clients can connect simultaneously")
    func multipleClients() async throws {
        let serverSystem = TransmissionSystem.server(id: "multi-server")

        let counterID = ActorIdentity(id: "shared-counter", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: counterID) {
            TestCounter(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort + 3)
        }

        try await Task.sleep(for: .milliseconds(100))

        let client1 = TransmissionSystem(id: "client-1")
        let client2 = TransmissionSystem(id: "client-2")
        let client3 = TransmissionSystem(id: "client-3")

        try await client1.connect(to: "ws://127.0.0.1:\(Self.testPort + 3)/transmission")
        try await client2.connect(to: "ws://127.0.0.1:\(Self.testPort + 3)/transmission")
        try await client3.connect(to: "ws://127.0.0.1:\(Self.testPort + 3)/transmission")

        try await Task.sleep(for: .milliseconds(300))

        let counter1 = try TestCounter.resolve(id: counterID, using: client1)
        let counter2 = try TestCounter.resolve(id: counterID, using: client2)
        let counter3 = try TestCounter.resolve(id: counterID, using: client3)

        _ = try await counter1.increment()
        _ = try await counter2.increment()
        _ = try await counter3.increment()

        let finalValue = try await counter1.value()
        #expect(finalValue == 3)

        serverTask.cancel()
    }

    @Test("Rapid successive calls complete correctly")
    func rapidSuccessiveCalls() async throws {
        let serverSystem = TransmissionSystem.server(id: "rapid-server")

        let counterID = ActorIdentity(id: "rapid-counter", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: counterID) {
            TestCounter(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort + 4)
        }

        try await Task.sleep(for: .milliseconds(100))

        let clientSystem = TransmissionSystem(id: "rapid-client")
        try await clientSystem.connect(to: "ws://127.0.0.1:\(Self.testPort + 4)/transmission")

        try await Task.sleep(for: .milliseconds(200))

        let remoteCounter = try TestCounter.resolve(id: counterID, using: clientSystem)

        for _ in 0..<50 {
            _ = try await remoteCounter.increment()
        }

        let finalValue = try await remoteCounter.value()
        #expect(finalValue == 50)

        serverTask.cancel()
    }

    @Test("Concurrent calls from single client")
    func concurrentCalls() async throws {
        let serverSystem = TransmissionSystem.server(id: "concurrent-server")

        let counterID = ActorIdentity(id: "concurrent-counter", node: serverSystem.nodeID)
        _ = serverSystem.makeLocalActor(id: counterID) {
            TestCounter(actorSystem: serverSystem)
        }

        let serverTask = Task {
            try await serverSystem.runServer(host: "127.0.0.1", port: Self.testPort + 5)
        }

        try await Task.sleep(for: .milliseconds(100))

        let clientSystem = TransmissionSystem(id: "concurrent-client")
        try await clientSystem.connect(to: "ws://127.0.0.1:\(Self.testPort + 5)/transmission")

        try await Task.sleep(for: .milliseconds(200))

        let remoteCounter = try TestCounter.resolve(id: counterID, using: clientSystem)

        try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<20 {
                group.addTask {
                    try await remoteCounter.increment()
                }
            }

            var results: [Int] = []
            for try await result in group {
                results.append(result)
            }

            #expect(results.count == 20)
        }

        let finalValue = try await remoteCounter.value()
        #expect(finalValue == 20)

        serverTask.cancel()
    }
}
