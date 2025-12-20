import Testing
import Foundation
@testable import Transmission

@Suite("ActorIdentity Tests")
struct ActorIdentityTests {

    @Test("Identity equality based on ID only")
    func identityEquality() {
        let a = ActorIdentity(id: "test-actor")
        let b = ActorIdentity(id: "test-actor")
        let c = ActorIdentity(id: "test-actor", node: NodeIdentity(id: "node-1"))

        #expect(a == b)
        #expect(a == c)
        #expect(b == c)
    }

    @Test("Random identities are unique")
    func randomIdentitiesUnique() {
        let identities = (0..<100).map { _ in ActorIdentity.random() }
        let uniqueIDs = Set(identities.map(\.id))
        #expect(uniqueIDs.count == 100)
    }

    @Test("Identity with type information")
    func identityWithType() {
        struct TestActor {}
        let identity = ActorIdentity.random(for: TestActor.self)

        #expect(identity.typeName != nil)
        #expect(identity.typeName?.contains("TestActor") == true)
    }

    @Test("Identity string literal initialization")
    func stringLiteralInit() {
        let identity: ActorIdentity = "my-actor"
        #expect(identity.id == "my-actor")
        #expect(identity.node == nil)
    }

    @Test("Identity with node")
    func identityWithNode() {
        let identity = ActorIdentity(id: "actor-1")
        let node = NodeIdentity(id: "server")
        let withNode = identity.withNode(node)

        #expect(withNode.id == "actor-1")
        #expect(withNode.node == node)
    }

    @Test("Identity description format")
    func identityDescription() {
        let simple = ActorIdentity(id: "actor-1")
        #expect(simple.description == "actor-1")

        let withNode = ActorIdentity(id: "actor-1", node: NodeIdentity(id: "server"))
        #expect(withNode.description == "server/actor-1")
    }

    @Test("Identity Codable round-trip")
    func codableRoundTrip() throws {
        let original = ActorIdentity(
            id: "test-actor",
            node: NodeIdentity(id: "node-1"),
            typeName: "TestType"
        )

        let encoder = JSONEncoder()
        let data = try encoder.encode(original)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(ActorIdentity.self, from: data)

        #expect(decoded == original)
        #expect(decoded.node == original.node)
        #expect(decoded.typeName == original.typeName)
    }

    @Test("Identity hashable behavior")
    func hashableBehavior() {
        let a = ActorIdentity(id: "test")
        let b = ActorIdentity(id: "test", node: NodeIdentity(id: "different"))

        var set: Set<ActorIdentity> = []
        set.insert(a)
        set.insert(b)

        #expect(set.count == 1)
    }
}
