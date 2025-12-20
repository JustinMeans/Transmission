# Transmission

Real-time distributed actor communication for Swift. Built for production use in financial applications and immersive experiences.

## Overview

Transmission provides transparent RPC over WebSockets using Swift's distributed actor system. Call methods on remote actors as if they were local. The framework handles serialization, connection management, and automatic reconnection.

## Features

- Swift 6 with full Sendable compliance
- Bidirectional communication (server push supported)
- Vapor integration for server-side use
- JWT authentication built-in
- Binary serialization for low-latency paths
- Priority message queues (realtime, high, normal, low)
- Automatic reconnection with exponential backoff
- Comprehensive metrics via swift-metrics

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
    let transmission = try TransmissionSystem.server(id: "main")
    try await app.transmission.register(transmission)

    // Register actors
    let calc = Calculator(actorSystem: transmission)
    await transmission.registerLocalActor(calc, id: "calculator")
}
```

### Client Setup

```swift
import Transmission

let transmission = TransmissionSystem()
try await transmission.connect(to: "wss://api.example.com/transmission")

let calculator = try Calculator.resolve(id: "calculator", using: transmission)
let result = try await calculator.add(40, 2)  // Returns 42
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Transmission Layer                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌───────────┐ │
│  │  Vapor   │◄──►│  Vapor   │◄──►│  Vapor   │◄──►│  Clients  │ │
│  │ Server A │    │ Server B │    │ Server C │    │ iOS/macOS │ │
│  └──────────┘    └──────────┘    └──────────┘    └───────────┘ │
│       ▲              ▲              ▲                  ▲        │
│       └──────────────┴──────────────┴──────────────────┘        │
│                 WebSocket Distributed Actors                     │
└─────────────────────────────────────────────────────────────────┘
```

## Use Cases

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

### Live Metrics Dashboard

```swift
distributed actor MetricsCollector {
    public typealias ActorSystem = TransmissionSystem

    distributed func currentStats() async -> SystemStats {
        SystemStats(
            cpuUsage: getCPUUsage(),
            memoryUsage: getMemoryUsage(),
            activeConnections: connectionCount,
            requestsPerSecond: rps
        )
    }

    distributed func subscribe(to metrics: [MetricType]) async -> AsyncStream<MetricUpdate> {
        // Push real-time metric updates to client
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

## Priority Queues

Messages can be assigned priority levels for proper ordering under load:

```swift
// Set priority on the encoder
encoder.priority = .realtime  // For time-critical updates
encoder.priority = .high      // For important operations
encoder.priority = .normal    // Default
encoder.priority = .low       // Background tasks
```

## License

MIT License
