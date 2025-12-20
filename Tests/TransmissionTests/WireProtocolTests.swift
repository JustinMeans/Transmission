import Testing
import Foundation
@testable import Transmission

@Suite("Wire Protocol Tests")
struct WireProtocolTests {

    @Test("CallID uniqueness")
    func callIDUniqueness() {
        let ids = (0..<1000).map { _ in CallID() }
        let unique = Set(ids.map(\.value))
        #expect(unique.count == 1000)
    }

    @Test("CallID Codable round-trip")
    func callIDCodable() throws {
        let original = CallID()

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(CallID.self, from: data)

        #expect(decoded == original)
    }

    @Test("MessagePriority ordering")
    func priorityOrdering() {
        #expect(MessagePriority.low < MessagePriority.normal)
        #expect(MessagePriority.normal < MessagePriority.high)
        #expect(MessagePriority.high < MessagePriority.realtime)
    }

    @Test("CallEnvelope Codable round-trip")
    func callEnvelopeCodable() throws {
        let envelope = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "test-actor"),
            target: "greet(name:)",
            genericSubs: ["Swift.String"],
            args: ["\"Hello\"".data(using: .utf8)!],
            priority: .high
        )

        let data = try JSONEncoder().encode(envelope)
        let decoded = try JSONDecoder().decode(CallEnvelope.self, from: data)

        #expect(decoded.callID == envelope.callID)
        #expect(decoded.recipient == envelope.recipient)
        #expect(decoded.target == envelope.target)
        #expect(decoded.genericSubs == envelope.genericSubs)
        #expect(decoded.args == envelope.args)
        #expect(decoded.priority == envelope.priority)
    }

    @Test("ReplyEnvelope Codable round-trip")
    func replyEnvelopeCodable() throws {
        let envelope = ReplyEnvelope(
            callID: CallID(),
            sender: ActorIdentity(id: "test-actor"),
            value: "result".data(using: .utf8)!
        )

        let data = try JSONEncoder().encode(envelope)
        let decoded = try JSONDecoder().decode(ReplyEnvelope.self, from: data)

        #expect(decoded.callID == envelope.callID)
        #expect(decoded.sender == envelope.sender)
        #expect(decoded.value == envelope.value)
    }

    @Test("WireEnvelope call variant")
    func wireEnvelopeCall() throws {
        let call = CallEnvelope(
            callID: CallID(),
            recipient: ActorIdentity(id: "actor"),
            target: "method()",
            genericSubs: [],
            args: [],
            priority: .normal
        )
        let envelope = WireEnvelope.call(call)

        let data = try JSONEncoder().encode(envelope)
        let decoded = try JSONDecoder().decode(WireEnvelope.self, from: data)

        if case .call(let decodedCall) = decoded {
            #expect(decodedCall.callID == call.callID)
        } else {
            Issue.record("Expected call envelope")
        }
    }

    @Test("WireEnvelope reply variant")
    func wireEnvelopeReply() throws {
        let reply = ReplyEnvelope(
            callID: CallID(),
            sender: nil,
            value: Data()
        )
        let envelope = WireEnvelope.reply(reply)

        let data = try JSONEncoder().encode(envelope)
        let decoded = try JSONDecoder().decode(WireEnvelope.self, from: data)

        if case .reply(let decodedReply) = decoded {
            #expect(decodedReply.callID == reply.callID)
        } else {
            Issue.record("Expected reply envelope")
        }
    }

    @Test("WireEnvelope close variant")
    func wireEnvelopeClose() throws {
        let envelope = WireEnvelope.close

        let data = try JSONEncoder().encode(envelope)
        let decoded = try JSONDecoder().decode(WireEnvelope.self, from: data)

        if case .close = decoded {
            // Success
        } else {
            Issue.record("Expected close envelope")
        }
    }
}
