# Transmission

Real-time distributed actor communication for Swift, plus `TransmissionLive`, a versioned stream protocol for live data.

## Overview

Transmission provides transparent RPC over WebSockets using Swift's distributed actor system. Call methods on remote actors as if they were local. The framework handles serialization, connection management, and automatic reconnection.

## Features

- Swift 6 language mode
- Bidirectional communication (server push supported)
- Vapor integration for server-side use
- Remote errors reach the caller: thrown errors, version-skewed targets and unknown actors all fail the call
- Compact binary call envelope; arguments and results are JSON
- Per-connection dispatch with bounded concurrency and per-actor ordering
- Automatic reconnection with exponential backoff, `reconnectNow()` and `disconnect()`
- WebSocket ping/pong dead-peer detection
- Metrics via swift-metrics

**Authentication is not built in.** A Vapor route is open unless you pass auth middleware to `configure`. See [Authentication](#authentication).

**Wire stability.** Distributed method targets are mangled names, so a source-compatible change such as adding a defaulted parameter changes the wire identity. A call to a target the peer lacks now fails with `RemoteCallError.Code.targetNotFound`, but it still fails. Use `TransmissionLive` for anything that must survive independently shipped clients.

**Linux clients.** `wss://` is refused on the NIOPosix client path until it has TLS. Plain `ws://` works.

## Requirements

- Swift 6.0+
- macOS 14+, iOS 17+, watchOS 10+, tvOS 17+, visionOS 1+

## Installation

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/JustinMeans/Transmission.git", from: "1.0.0"),
]
```

For client applications:
```swift
.target(name: "MyApp", dependencies: ["Transmission"])
```

For Vapor servers:
```swift
.target(name: "MyServer", dependencies: ["TransmissionVapor"])
```

For live data, depend on `TransmissionLive` (client and hub) or `TransmissionLiveVapor` (server route). Neither needs `Transmission`.

## Quick Start

### Define a Distributed Actor

```swift
import Transmission

public distributed actor Calculator {
    public typealias ActorSystem = TransmissionSystem

    public distributed func add(_ a: Int, _ b: Int) async -> Int {
        a + b
    }

    public distributed func fibonacci(_ n: Int) async -> Int {
        if n <= 1 { return n }
        return await fibonacci(n - 1) + await fibonacci(n - 2)
    }
}
```

### Server Setup (Vapor)

```swift
import TransmissionVapor

func configure(_ app: Application) async throws {
    let transmission = TransmissionSystem.server(id: "main")
    try app.transmission.register(transmission)
    try app.transmission.configure(path: "transmission", middleware: [MyAuthMiddleware()])

    // The system keeps actors made with makeLocalActor alive until
    // releaseLocalActor(id:). Actors made with Actor(actorSystem:) are held
    // weakly and resign when released.
    transmission.makeLocalActor(id: "calculator") {
        Calculator(actorSystem: transmission)
    }
}
```

### Client Setup

```swift
import Transmission

let transmission = TransmissionSystem()
try await transmission.connect(
    to: "wss://api.example.com/transmission",
    options: .init(headerProvider: { ["Authorization": "Bearer \(await token())"] })
)

let id = ActorIdentity(id: "calculator", node: .server)
let calculator = try Calculator.resolve(id: id, using: transmission)
let result = try await calculator.add(40, 2)  // Returns 42

await transmission.reconnectNow()   // network changed or app foregrounded
await transmission.disconnect()     // stays down until the next connect
```

## Authentication

Run auth as Vapor middleware, which rejects before the upgrade. `onConnect` runs before the connection's first call, so an authenticated identity can be copied onto the node:

```swift
try app.transmission.configure(
    path: "transmission",
    middleware: [MyAuthMiddleware()],
    onConnect: { request, node in
        await node.setUserInfo("user", value: try request.auth.require(User.self).id)
    }
)
```

Handlers read it through `RemoteNode.current`:

```swift
distributed func secrets() async throws -> [Secret] {
    guard let user: UUID = await RemoteNode.current?.getUserInfo("user") else {
        throw Denied()
    }
    return try await store.secrets(for: user)
}
```

## Errors

A failed call throws `RemoteCallError`. Only a `Codable` error's contents cross the wire, in `payload`, and a `throws(MyError)` method gets its own type back. Any other error sends its type name alone, so internals do not leak to callers.

## Live data

Do not stream metrics or logs through distributed calls. Use `TransmissionLive` (client and hub, no Vapor dependency) and `TransmissionLiveVapor` (the `/v3/live` route). It has sequence numbers, snapshots, resume, coalescing and per-client backpressure. A slow client costs nobody else anything, and clients on different builds negotiate a protocol version rather than going blank. The protocol is specified in [Docs/LiveProtocol.md](Docs/LiveProtocol.md).

```swift
// Shared by client and server.
let statistics = Topic<[String: Double], [String: Double]>(
    name: "statistics", kind: .state,
    merge: { earlier, later in earlier.merge(later) { _, new in new } },
    apply: { state, delta in state.merge(delta) { _, new in new } }
)

// Server.
let hub = LiveHub(authorizer: MyRules())
try await hub.host(statistics, initial: [:])
app.live.mount(hub: hub) { _, token in try await identity(for: token) }
try await hub.publish(statistics, ["requests": 1042])

// Client.
let live = LiveClient(configuration: .init(
    url: url, clientID: installID, build: build, tokenProvider: { await token() }
))
await live.start()
let subscription = await live.subscribe(statistics, maxHz: 1)
for await update in subscription.updates {
    store.statistics = update.state
    store.freshness = update.freshness   // .live, .stale(since:) or .resyncing
}

await live.pause()    // app background
await live.resume()   // app foreground
```

## Use Cases

Handlers run concurrently, except that calls from one connection to one actor start in order (`DispatchLimits`). These are sketches.

### Real-time Chat

```swift
distributed actor ChatRoom {
    public typealias ActorSystem = TransmissionSystem

    private var messages: [Message] = []

    distributed func send(message: String, from user: String) async -> Message {
        let msg = Message(id: UUID(), text: message, author: user, timestamp: Date())
        messages.append(msg)
        return msg
    }

    distributed func history(limit: Int) async -> [Message] {
        Array(messages.suffix(limit))
    }
}
```

### Multiplayer Game State

```swift
distributed actor GameWorld {
    public typealias ActorSystem = TransmissionSystem

    private var players: [PlayerID: PlayerState] = [:]

    distributed func join(player: PlayerID) async -> WorldSnapshot {
        players[player] = PlayerState.initial
        return WorldSnapshot(players: players)
    }

    distributed func move(player: PlayerID, to position: Vector3) async {
        players[player]?.position = position
        // Broadcast to other players
    }
}
```

## License

MIT License
