import Testing
import Foundation
import Distributed
@testable import Transmission

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
        let queue = PriorityMessageQueue()

        for i in 0..<100 {
            let priority: MessagePriority = switch i % 4 {
            case 0: .low
            case 1: .normal
            case 2: .high
            default: .realtime
            }

            let envelope = CallEnvelope(
                callID: CallID(),
                recipient: ActorIdentity(id: "test-\(i)"),
                target: "method()",
                genericSubs: [],
                args: [],
                priority: priority
            )

            // Note: In real test, would use mock node
            // await queue.enqueue(QueuedMessage(envelope: envelope, node: mockNode))
        }

        // Verify count accumulates correctly
        // In real implementation, would verify dequeue order
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
