import Testing
import Foundation
@testable import Transmission

@Suite("NodeIdentity Tests")
struct NodeIdentityTests {

    @Test("Node identity equality")
    func nodeEquality() {
        let a = NodeIdentity(id: "server")
        let b = NodeIdentity(id: "server")
        let c = NodeIdentity(id: "client")

        #expect(a == b)
        #expect(a != c)
    }

    @Test("Server constant")
    func serverConstant() {
        #expect(NodeIdentity.server.id == "server")
    }

    @Test("Random node identities unique")
    func randomNodesUnique() {
        let nodes = (0..<100).map { _ in NodeIdentity.random() }
        let uniqueIDs = Set(nodes.map(\.id))
        #expect(uniqueIDs.count == 100)
    }

    @Test("String literal initialization")
    func stringLiteralInit() {
        let node: NodeIdentity = "my-node"
        #expect(node.id == "my-node")
    }

    @Test("Node Codable round-trip")
    func codableRoundTrip() throws {
        let original = NodeIdentity(id: "test-node")

        let encoder = JSONEncoder()
        let data = try encoder.encode(original)

        let decoder = JSONDecoder()
        let decoded = try decoder.decode(NodeIdentity.self, from: data)

        #expect(decoded == original)
    }

    @Test("Node description")
    func nodeDescription() {
        let node = NodeIdentity(id: "test-node")
        #expect(node.description == "test-node")
    }
}
