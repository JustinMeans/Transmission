import Distributed
import Foundation
import Testing
import TransmissionTestSupport
@testable import Transmission

@Suite("Error replies, dispatch and actor lifetime", .serialized)
struct DispatchTests {

    @Test("a Codable thrown error round-trips its type and payload")
    func typedError() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            do {
                try await remote.voidThrow()
                Issue.record("expected a throw")
            } catch let error as RemoteCallError {
                #expect(error.code == .thrown)
                #expect(error.errorType?.contains("Boom") == true)
                let decoded = try JSONDecoder().decode(Boom.self, from: try #require(error.payload))
                #expect(decoded == Boom())
            }
        }
    }

    @Test("a non-Codable error sends its type name only")
    func opaqueError() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            do {
                try await remote.opaqueThrow()
                Issue.record("expected a throw")
            } catch let error as RemoteCallError {
                #expect(error.payload == nil)
                #expect(!error.message.contains("do-not-leak"))
                #expect(!String(describing: error).contains("do-not-leak"))
            }
        }
    }

    @Test("skew: missing target reports targetNotFound, unknown actor reports actorNotFound")
    func errorCodes() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let node = try await client.nodes.node(for: "srv", timeout: .seconds(2))
            func code(actor: String, target: String) async throws -> RemoteCallError.Code? {
                do {
                    _ = try await client.pendingCalls.send(
                        envelope: CallEnvelope(
                            callID: CallID(),
                            recipient: ActorIdentity(id: actor, node: "srv"),
                            target: target, genericSubs: [], args: [], priority: .normal),
                        via: node, timeout: .seconds(2))
                    return nil
                } catch let error as RemoteCallError {
                    return error.code
                }
            }
            #expect(try await code(actor: "svc", target: "$sNope") == .targetNotFound)
            #expect(try await code(actor: "nope", target: "$sNope") == .actorNotFound)
        }
    }

    @Test("a single configured frame limit: oversize fails the call, not the connection")
    func oversize() async throws {
        try await withNIOServer(configure: { $0.maxFrameSize = 1 << 20 }) { _, _, port in
            let client = TransmissionSystem(id: "limit")
            client.maxFrameSize = 1 << 20
            try await client.connect(to: address(port))
            defer { Task { await client.disconnect() } }
            _ = try await client.nodes.node(for: "srv", timeout: .seconds(2))
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            #expect(try await remote.echoSize(String(repeating: "y", count: 800_000)) == 800_000)
            await #expect(throws: TransmissionError.self) {
                _ = try await remote.echoSize(String(repeating: "y", count: 2 << 20))
            }
            do {
                _ = try await remote.big(2 << 20)
                Issue.record("expected a throw")
            } catch let error as RemoteCallError {
                #expect(error.code == .frameTooLarge)
            }
            #expect(try await remote.ok() == 42)
        }
    }

    @Test("T4 RemoteNode.current is the caller during dispatch")
    func currentNode() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(id: "caller-1", port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            #expect(try await remote.whoCalled() == "caller-1")
        }
    }

    @Test("T5 a directly constructed actor is not retained and resigns on release")
    func weakActors() async throws {
        let system = TransmissionSystem(id: "weak")
        var actor: RegPeer? = RegPeer(actorSystem: system)
        let id = actor!.id
        #expect(system.lookupActor(id: id) != nil)
        actor = nil
        _ = actor
        #expect(system.lookupActor(id: id) == nil)
        #expect(system.liveActorCount == 0)
    }

    @Test("T5 makeLocalActor keeps a discarded result alive until released")
    func pinnedActors() async throws {
        let system = TransmissionSystem(id: "pin")
        system.makeLocalActor(id: "pinned") { RegPeer(actorSystem: system) }
        let identity = ActorIdentity(id: "pinned", node: system.nodeID)
        #expect(system.lookupActor(id: identity) != nil)
        system.releaseLocalActor(id: identity)
        #expect(system.lookupActor(id: identity) == nil)
    }

    @Test("T5 a dying actor does not evict its replacement with the same id")
    func resignDoesNotEvictReplacement() async throws {
        let system = TransmissionSystem(id: "replace")
        var old: RegPeer? = system.makeLocalActor(id: "same") { RegPeer(actorSystem: system) }
        system.releaseLocalActor(id: ActorIdentity(id: "same", node: system.nodeID))
        let replacement = system.makeLocalActor(id: "same") { RegPeer(actorSystem: system) }
        old = nil
        _ = old
        #expect(system.lookupActor(id: replacement.id) != nil)
    }

    @Test("T6 an actor reference passes as an argument")
    func actorReferenceArgument() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(id: "ref-client", port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let peer = client.makeLocalActor(id: "peer") { RegPeer(actorSystem: client) }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            #expect(try await withDeadline(.seconds(5), "T6") { try await remote.call(peer: peer) } == 7)
        }
    }

    @Test("T7 an unresolvable generic substitution throws rather than being dropped")
    func genericSubstitution() throws {
        let system = TransmissionSystem(id: "generic")
        let decoder = TransmissionDecoder(
            envelope: CallEnvelope(
                callID: CallID(), recipient: "a", target: "t",
                genericSubs: ["$sNotARealType"], args: [], priority: .normal),
            system: system)
        #expect(throws: TransmissionError.self) { _ = try decoder.decodeGenericSubstitutions() }
    }

    @Test("T8 the on-demand resolver runs without the lock and on every dispatch path")
    func resolver() async throws {
        try await withNIOServer(configure: { system in
            system.registerOnDemandResolver { id in
                guard id.id == "lazy" else { return nil }
                _ = system.lookupActor(id: "svc")
                return system.makeLocalActor(id: id) { RegPeer(actorSystem: system) }
            }
        }) { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegPeer.resolve(id: ActorIdentity(id: "lazy", node: "srv"), using: client)
            #expect(try await withDeadline(.seconds(5), "T8") { try await remote.pong() } == 7)
        }
    }

    @Test("T10 cancelling the caller ends a pending call promptly")
    func cancellation() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            let task = Task { try await remote.slow(10_000) }
            try await Task.sleep(for: .milliseconds(100))
            task.cancel()
            let outcome = try await withDeadline(.seconds(2), "T10") { await task.result }
            #expect(throws: (any Error).self) { try outcome.get() }
        }
    }

    @Test("T10 callTimeout is honoured")
    func callTimeout() async throws {
        try await withNIOServer { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            client.callTimeout = .milliseconds(100)
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            await #expect(throws: TransmissionError.self) { _ = try await remote.slow(5_000) }
        }
    }

    @Test("concurrency per connection is bounded")
    func concurrencyBound() async throws {
        let context = ServiceContext()
        let server = TransmissionSystem.server(id: "srv")
        server.dispatchLimits = DispatchLimits(maxConcurrentCalls: 4, ordering: .concurrent)
        let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server, context: context) }
        _ = service
        let port = freePort()
        let task = Task { try await server.runServer(at: address(port)) }
        defer { task.cancel() }
        let client = try await connectedClient(port: port, serverID: "srv")
        defer { Task { await client.disconnect() } }
        let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<20 { group.addTask { _ = try await remote.slow(30) } }
            try await group.waitForAll()
        }
        #expect(context.peakRunning <= 4)
        #expect(context.peakRunning >= 2)
    }

    @Test("a full call queue answers overloaded and never stalls the read loop")
    func overload() async throws {
        try await withNIOServer(configure: {
            $0.dispatchLimits = DispatchLimits(maxConcurrentCalls: 1, maxQueuedCalls: 2, ordering: .perActor, orderingStallLimit: .seconds(30))
        }) { _, _, port in
            let client = try await connectedClient(port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            let blocker = Task { try await remote.slow(800) }
            try await Task.sleep(for: .milliseconds(100))
            let results = await withTaskGroup(of: Bool.self, returning: [Bool].self) { group in
                for _ in 0..<8 {
                    group.addTask {
                        do {
                            _ = try await remote.slow(1)
                            return false
                        } catch let error as RemoteCallError {
                            return error.code == .overloaded
                        } catch {
                            return false
                        }
                    }
                }
                return await group.reduce(into: []) { $0.append($1) }
            }
            #expect(results.contains(true))
            _ = try await blocker.value
        }
    }

    @Test("per-actor order does not deadlock a nested call back into the same actor")
    func nestedCallIntoSameActor() async throws {
        try await withNIOServer(configure: {
            $0.dispatchLimits = DispatchLimits(orderingStallLimit: .milliseconds(100))
        }) { _, _, port in
            let client = try await connectedClient(id: "nested", port: port, serverID: "srv")
            defer { Task { await client.disconnect() } }
            let peer = client.makeLocalActor(id: "peer") { RegPeer(actorSystem: client) }
            let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
            #expect(try await withDeadline(.seconds(5), "nested") { try await remote.reenter(from: peer) } == 42)
        }
    }

    @Test("disconnect cancels handlers still running for that connection")
    func handlersCancelled() async throws {
        let context = ServiceContext()
        let server = TransmissionSystem.server(id: "srv")
        let service = server.makeLocalActor(id: "svc") { RegSvc(actorSystem: server, context: context) }
        _ = service
        let port = freePort()
        let task = Task { try await server.runServer(at: address(port)) }
        defer { task.cancel() }
        let client = try await connectedClient(port: port, serverID: "srv")
        let remote = try RegSvc.resolve(id: ActorIdentity(id: "svc", node: "srv"), using: client)
        let call = Task { try await remote.waitForCancel() }
        try await Task.sleep(for: .milliseconds(100))
        await client.disconnect()
        let finished = await eventually(within: .seconds(2)) { await context.recorder.seen == [1] }
        #expect(finished)
        call.cancel()
    }

    @Test("error envelope: compact wire bytes are stable and tolerate trailing fields")
    func errorEnvelopeWire() throws {
        let id = CallID(UUID(uuidString: "00000000-0000-0000-0000-000000000001")!)
        let envelope = ErrorEnvelope(
            callID: id, sender: nil, code: .thrown, errorType: "E", message: "m", payload: Data([9]))
        var bytes = WireEnvelope.error(envelope).encodeCompact()
        #expect(bytes[0] == 3)
        bytes.append(contentsOf: [0xDE, 0xAD])
        guard case .error(let decoded) = try WireEnvelope.decodeCompact(from: bytes) else {
            Issue.record("wrong case")
            return
        }
        #expect(decoded.code == .thrown)
        #expect(decoded.errorType == "E")
        #expect(decoded.message == "m")
        #expect(decoded.payload == Data([9]))
    }

    @Test("error envelope: an unknown code decodes to .unknown")
    func errorEnvelopeUnknownCode() throws {
        let id = CallID()
        var bytes = WireEnvelope.error(ErrorEnvelope(callID: id, sender: nil, code: .thrown, errorType: nil, message: "m", payload: nil)).encodeCompact()
        // type(1) + uuid(16) + sender flag(1), then the code byte.
        bytes[18] = 250
        guard case .error(let decoded) = try WireEnvelope.decodeCompact(from: bytes) else {
            Issue.record("wrong case")
            return
        }
        #expect(decoded.code == .unknown)
    }

    @Test("metrics gauge counts rather than sets")
    func gaugeCounts() {
        let metrics = TransmissionMetrics(nodeID: "gauge")
        metrics.connectionOpened()
        metrics.connectionOpened()
        metrics.connectionClosed()
        #expect(metrics.activeConnectionCount == 1)
    }
}
