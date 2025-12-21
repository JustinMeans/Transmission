import Foundation
import NIO
import NIOWebSocket
import Logging

/// Represents a connection to a remote node in the Transmission network.
public actor RemoteNode {
    public let nodeID: NodeIdentity
    let channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>
    let inbound: NIOAsyncChannelInboundStream<WebSocketFrame>
    let outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>

    private var userInfo: [String: any Sendable] = [:]
    private var isClosed = false

    @TaskLocal public static var current: RemoteNode?

    init(
        nodeID: NodeIdentity,
        channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        inbound: NIOAsyncChannelInboundStream<WebSocketFrame>,
        outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>
    ) {
        self.nodeID = nodeID
        self.channel = channel
        self.inbound = inbound
        self.outbound = outbound
    }

    /// Serialization format for this connection. Defaults to binary for optimal performance.
    public var serializationFormat: SerializationFormat = .binary

    /// Sends a wire envelope to this remote node.
    public func send(_ envelope: WireEnvelope) async throws {
        guard !isClosed else {
            throw TransmissionError.noConnection
        }

        let data: Data
        let opcode: WebSocketOpcode

        switch serializationFormat {
        case .binary:
            data = envelope.encodeCompact()
            opcode = .binary
        case .json:
            let encoder = JSONEncoder()
            data = try encoder.encode(envelope)
            opcode = .text
        }

        var buffer = channel.channel.allocator.buffer(capacity: data.count)
        buffer.writeBytes(data)

        let frame = WebSocketFrame(fin: true, opcode: opcode, data: buffer)
        try await outbound.write(frame)
    }

    /// Sends raw data as a binary frame.
    public func sendBinary(_ data: Data) async throws {
        guard !isClosed else {
            throw TransmissionError.noConnection
        }

        var buffer = channel.channel.allocator.buffer(capacity: data.count)
        buffer.writeBytes(data)

        let frame = WebSocketFrame(fin: true, opcode: .binary, data: buffer)
        try await outbound.write(frame)
    }

    /// Sends a ping frame.
    public func ping() async throws {
        guard !isClosed else { return }

        var buffer = channel.channel.allocator.buffer(capacity: 0)
        let frame = WebSocketFrame(fin: true, opcode: .ping, data: buffer)
        try await outbound.write(frame)
    }

    /// Closes this connection.
    public func close() async {
        guard !isClosed else { return }
        isClosed = true

        do {
            var buffer = channel.channel.allocator.buffer(capacity: 2)
            buffer.writeInteger(UInt16(1000)) // Normal closure
            let frame = WebSocketFrame(fin: true, opcode: .connectionClose, data: buffer)
            try await outbound.write(frame)
            outbound.finish()
        } catch {
            // Ignore close errors
        }
    }

    /// Gets user info for this connection.
    public func getUserInfo<T: Sendable>(_ key: String) -> T? {
        userInfo[key] as? T
    }

    /// Sets user info for this connection.
    public func setUserInfo<T: Sendable>(_ key: String, value: T?) {
        userInfo[key] = value
    }

    /// Executes a closure with this node as the current TaskLocal node.
    public static func withCurrent<T>(_ node: RemoteNode, operation: () async throws -> T) async rethrows -> T {
        try await $current.withValue(node, operation: operation)
    }
}
