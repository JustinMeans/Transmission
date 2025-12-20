import Testing
import Foundation
@testable import Transmission

@Suite("TransmissionError Tests")
struct TransmissionErrorTests {

    @Test("Error descriptions are meaningful")
    func errorDescriptions() {
        let errors: [TransmissionError] = [
            .noConnection,
            .connectionFailed("timeout"),
            .connectionTimeout,
            .actorNotFound(ActorIdentity(id: "test")),
            .noNodeForActor(ActorIdentity(id: "test")),
            .typeMismatch(expected: "String", got: "Int"),
            .encodingFailed("invalid data"),
            .decodingFailed("malformed json"),
            .callTimeout(CallID()),
            .callFailed(CallID(), "network error"),
            .invalidURL("not-a-url"),
            .authenticationRequired,
            .authenticationFailed("bad token")
        ]

        for error in errors {
            let description = error.errorDescription
            #expect(description != nil)
            #expect(!description!.isEmpty)
        }
    }

    @Test("noConnection error")
    func noConnectionError() {
        let error = TransmissionError.noConnection
        #expect(error.errorDescription?.contains("connection") == true)
    }

    @Test("connectionFailed includes reason")
    func connectionFailedError() {
        let error = TransmissionError.connectionFailed("network unreachable")
        #expect(error.errorDescription?.contains("network unreachable") == true)
    }

    @Test("actorNotFound includes actor ID")
    func actorNotFoundError() {
        let error = TransmissionError.actorNotFound(ActorIdentity(id: "my-actor"))
        #expect(error.errorDescription?.contains("my-actor") == true)
    }

    @Test("typeMismatch includes both types")
    func typeMismatchError() {
        let error = TransmissionError.typeMismatch(expected: "Greeter", got: "Calculator")
        let desc = error.errorDescription!
        #expect(desc.contains("Greeter"))
        #expect(desc.contains("Calculator"))
    }

    @Test("callTimeout includes call ID")
    func callTimeoutError() {
        let callID = CallID()
        let error = TransmissionError.callTimeout(callID)
        #expect(error.errorDescription?.contains(callID.description) == true)
    }

    @Test("Error is Sendable")
    func errorIsSendable() {
        let error: any Sendable = TransmissionError.noConnection
        #expect(error is TransmissionError)
    }
}
