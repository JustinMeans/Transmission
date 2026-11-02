import Foundation
import NIO
import NIOWebSocket
import Logging

/// Represents a connection to a remote node in the Transmission network.
public actor RemoteNode {
    public nonisolated let nodeID: NodeIdentity

    // NIO-based connection (used by clients)
    private let channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>?
    private let outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>?

    // Closure-based connection (used by Vapor servers)
    private let sendClosure: (@Sendable (Data) async throws -> Void)?
    private let closeClosure: (@Sendable () async -> Void)?

    /// RFC 6455 section 5.3: everything a client writes must be masked.
    private let masksFrames: Bool
    private let maxFrameSize: Int

    private var userInfo: [String: any Sendable] = [:]
    private var isClosed = false

    @TaskLocal public static var current: RemoteNode?

    /// Creates a RemoteNode with NIO async channel (for clients)
    init(
        nodeID: NodeIdentity,
        channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>,
        inbound: NIOAsyncChannelInboundStream<WebSocketFrame>,
        outbound: NIOAsyncChannelOutboundWriter<WebSocketFrame>,
        masksFrames: Bool = false,
        maxFrameSize: Int = TransmissionSystem.defaultMaxFrameSize
    ) {
        self.nodeID = nodeID
        self.channel = channel
        self.outbound = outbound
        self.sendClosure = nil
        self.closeClosure = nil
        self.masksFrames = masksFrames
        self.maxFrameSize = maxFrameSize
    }

    /// Creates a RemoteNode with send/close closures (for Vapor servers)
    public init(
        nodeID: NodeIdentity,
        send: @escaping @Sendable (Data) async throws -> Void,
        close: @escaping @Sendable () async -> Void,
        maxFrameSize: Int = TransmissionSystem.defaultMaxFrameSize
    ) {
        self.nodeID = nodeID
        self.channel = nil
        self.outbound = nil
        self.sendClosure = send
        self.closeClosure = close
        self.masksFrames = false
        self.maxFrameSize = maxFrameSize
    }

    /// Serialization format for this connection. Defaults to binary for optimal performance.
    public var serializationFormat: SerializationFormat = .binary

    /// Sends a wire envelope to this remote node.
    public func send(_ envelope: WireEnvelope) async throws {
        guard !isClosed else {
            throw TransmissionError.noConnection
        }

        let data: Data
        switch serializationFormat {
        case .binary:
            data = envelope.encodeCompact()
        case .json:
            let encoder = JSONEncoder()
            data = try encoder.encode(envelope)
        }

        try await write(data, opcode: serializationFormat == .binary ? .binary : .text)
    }

    /// Sends raw data as a binary frame.
    public func sendBinary(_ data: Data) async throws {
        guard !isClosed else {
            throw TransmissionError.noConnection
        }
        try await write(data, opcode: .binary)
    }

    /// Refuses an oversize message before it reaches the socket: the receiver
    /// would tear the connection down and fail every other call in flight.
    private func write(_ data: Data, opcode: WebSocketOpcode) async throws {
        guard data.count <= maxFrameSize else {
            throw TransmissionError.encodingFailed("Message of \(data.count) bytes exceeds the \(maxFrameSize) byte frame limit")
        }
        if let sendClosure {
            try await sendClosure(data)
        } else if let channel, let outbound {
            var buffer = channel.channel.allocator.buffer(capacity: data.count)
            buffer.writeBytes(data)
            try await outbound.write(makeFrame(opcode, buffer))
        } else {
            throw TransmissionError.noConnection
        }
    }

    private func makeFrame(_ opcode: WebSocketOpcode, _ data: ByteBuffer) -> WebSocketFrame {
        WebSocketFrame(fin: true, opcode: opcode, maskKey: masksFrames ? .random() : nil, data: data)
    }

    /// Writes a control frame on the NIO path, masked when this side is a client.
    func writeControl(_ opcode: WebSocketOpcode, payload: ByteBuffer) async throws {
        guard let outbound else { return }
        try await outbound.write(makeFrame(opcode, payload))
    }

    /// Sends a ping frame.
    public func ping() async throws {
        guard !isClosed else { return }
        guard let channel, let outbound else { return }

        let buffer = channel.channel.allocator.buffer(capacity: 0)
        try await outbound.write(makeFrame(.ping, buffer))
    }

    /// Closes this connection.
    public func close() async {
        guard !isClosed else { return }
        isClosed = true

        if let closeClosure {
            await closeClosure()
        } else if let channel, let outbound {
            do {
                var buffer = channel.channel.allocator.buffer(capacity: 2)
                buffer.writeInteger(UInt16(1000)) // Normal closure
                try await outbound.write(makeFrame(.connectionClose, buffer))
                outbound.finish()
            } catch {
                // Ignore close errors
            }
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
