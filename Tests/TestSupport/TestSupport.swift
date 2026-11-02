import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public struct DeadlineExceeded: Error, CustomStringConvertible {
    public let label: String
    public init(label: String) { self.label = label }
    public var description: String { "deadline exceeded: \(label)" }
}

/// Runs `body`, failing with `DeadlineExceeded` if it takes longer than `limit`.
public func withDeadline<T: Sendable>(
    _ limit: Duration,
    _ label: String = "operation",
    _ body: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await body() }
        group.addTask {
            try await Task.sleep(for: limit)
            throw DeadlineExceeded(label: label)
        }
        let value = try await group.next()!
        group.cancelAll()
        return value
    }
}

/// Polls `condition` until it holds or `limit` passes. Returns whether it held.
@discardableResult
public func eventually(
    within limit: Duration = .seconds(5),
    every step: Duration = .milliseconds(10),
    _ condition: @Sendable () async -> Bool
) async -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: limit)
    while clock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: step)
    }
    return await condition()
}

/// An unused loopback TCP port.
public func freePort() -> Int {
    let fd = socket(AF_INET, SOCK_STREAM, 0)
    precondition(fd >= 0)
    defer { close(fd) }
    var address = sockaddr_in()
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = 0
    address.sin_addr.s_addr = inet_addr("127.0.0.1")
    var length = socklen_t(MemoryLayout<sockaddr_in>.size)
    let bound = withUnsafePointer(to: &address) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, length) }
    }
    precondition(bound == 0)
    _ = withUnsafeMutablePointer(to: &address) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(fd, $0, &length) }
    }
    return Int(UInt16(bigEndian: address.sin_port))
}

/// True if something accepts TCP on the port. Sends no bytes, so it cannot be
/// mistaken for a WebSocket client by the server under test.
public func tcpPortAccepts(_ port: Int) throws -> Bool {
    let fd = socket(AF_INET, SOCK_STREAM, 0)
    guard fd >= 0 else { throw POSIXError(.EIO) }
    defer { close(fd) }
    var address = sockaddr_in()
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = UInt16(port).bigEndian
    address.sin_addr.s_addr = inet_addr("127.0.0.1")
    return withUnsafePointer(to: &address) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) == 0
        }
    }
}

/// Nearest-rank percentile; `p` in 0...100.
public func percentile(_ values: [Double], _ p: Double) -> Double {
    guard !values.isEmpty else { return 0 }
    let sorted = values.sorted()
    let rank = Int((p / 100 * Double(sorted.count)).rounded(.up))
    return sorted[min(max(rank, 1), sorted.count) - 1]
}

/// A hand-rolled WebSocket client on a blocking socket. It upgrades and then
/// does nothing unless told to, which is how tests get a peer that never pongs
/// or a reader that never drains its socket. No client library will do either.
public final class RawSocketClient: @unchecked Sendable {
    private let fd: Int32
    public private(set) var responseHead = ""

    /// Connects and performs the upgrade. `receiveBuffer` shrinks SO_RCVBUF so a
    /// peer that stops reading applies backpressure quickly.
    public init(
        port: Int,
        path: String,
        headers: [String: String] = [:],
        receiveBuffer: Int32? = nil
    ) throws {
        fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else { throw POSIXError(.EIO) }
        if var size = receiveBuffer {
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, socklen_t(MemoryLayout<Int32>.size))
        }
        var address = sockaddr_in()
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = UInt16(port).bigEndian
        address.sin_addr.s_addr = inet_addr("127.0.0.1")
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard result == 0 else {
            close(fd)
            throw POSIXError(.ECONNREFUSED)
        }
        var request = "GET \(path) HTTP/1.1\r\nHost: 127.0.0.1:\(port)\r\n"
        request += "Upgrade: websocket\r\nConnection: Upgrade\r\n"
        request += "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
        for (name, value) in headers { request += "\(name): \(value)\r\n" }
        request += "\r\n"
        _ = request.withCString { write(fd, $0, strlen($0)) }
        var head = [UInt8]()
        var byte: UInt8 = 0
        while read(fd, &byte, 1) == 1 {
            head.append(byte)
            if head.suffix(4) == [13, 10, 13, 10] { break }
        }
        responseHead = String(decoding: head, as: UTF8.self)
    }

    /// True once the server has closed the socket. Non-blocking.
    public var peerClosed: Bool {
        var buffer = [UInt8](repeating: 0, count: 65536)
        while true {
            let count = recv(fd, &buffer, buffer.count, Int32(MSG_DONTWAIT))
            if count == 0 { return true }
            if count < 0 { return errno != EAGAIN && errno != EWOULDBLOCK }
        }
    }

    /// True once the server has sent a Close frame or dropped the socket.
    public var sawClose: Bool {
        var byte: UInt8 = 0
        let count = recv(fd, &byte, 1, Int32(MSG_DONTWAIT))
        if count == 0 { return true }
        return count == 1 && byte == 0x88
    }

    /// Writes one masked binary frame.
    public func sendBinary(_ payload: [UInt8]) {
        var frame: [UInt8] = [0x82]
        let key: [UInt8] = [1, 2, 3, 4]
        if payload.count < 126 {
            frame.append(0x80 | UInt8(payload.count))
        } else {
            frame.append(0x80 | 126)
            frame.append(UInt8(payload.count >> 8))
            frame.append(UInt8(payload.count & 0xFF))
        }
        frame += key
        frame += payload.enumerated().map { $0.element ^ key[$0.offset % 4] }
        _ = frame.withUnsafeBytes { write(fd, $0.baseAddress, $0.count) }
    }

    public func shutdown() {
        close(fd)
    }
}
