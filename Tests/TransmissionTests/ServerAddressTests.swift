import Testing
import Foundation
@testable import Transmission

@Suite("ServerAddress Tests")
struct ServerAddressTests {

    @Test("Basic initialization")
    func basicInit() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443)

        #expect(address.scheme == .secure)
        #expect(address.host == "api.example.com")
        #expect(address.port == 443)
        #expect(address.path == "/transmission")
    }

    @Test("Custom path")
    func customPath() {
        let address = ServerAddress(scheme: .insecure, host: "localhost", port: 8080, path: "/ws")

        #expect(address.path == "/ws")
    }

    @Test("URL parsing - secure")
    func urlParsingSecure() {
        let address = ServerAddress(url: "wss://api.example.com:8443/custom")

        #expect(address != nil)
        #expect(address?.scheme == .secure)
        #expect(address?.host == "api.example.com")
        #expect(address?.port == 8443)
        #expect(address?.path == "/custom")
    }

    @Test("URL parsing - insecure")
    func urlParsingInsecure() {
        let address = ServerAddress(url: "ws://localhost:8080/transmission")

        #expect(address != nil)
        #expect(address?.scheme == .insecure)
        #expect(address?.host == "localhost")
        #expect(address?.port == 8080)
    }

    @Test("URL parsing - default ports")
    func urlParsingDefaultPorts() {
        let secure = ServerAddress(url: "wss://api.example.com/path")
        #expect(secure?.port == 443)

        let insecure = ServerAddress(url: "ws://api.example.com/path")
        #expect(insecure?.port == 80)
    }

    @Test("URL parsing - invalid scheme")
    func urlParsingInvalidScheme() {
        let address = ServerAddress(url: "https://api.example.com")
        #expect(address == nil)
    }

    @Test("URL parsing - missing host")
    func urlParsingMissingHost() {
        let address = ServerAddress(url: "wss:///path")
        #expect(address == nil)
    }

    @Test("URL generation")
    func urlGeneration() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443, path: "/ws")
        let url = address.url

        #expect(url.scheme == "wss")
        #expect(url.host == "api.example.com")
        #expect(url.port == 443)
        #expect(url.path == "/ws")
    }

    @Test("Description format")
    func descriptionFormat() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443, path: "/transmission")
        #expect(address.description == "wss://api.example.com:443/transmission")
    }

    @Test("Hashable and Equatable")
    func hashableEquatable() {
        let a = ServerAddress(scheme: .secure, host: "api.example.com", port: 443)
        let b = ServerAddress(scheme: .secure, host: "api.example.com", port: 443)
        let c = ServerAddress(scheme: .insecure, host: "api.example.com", port: 443)

        #expect(a == b)
        #expect(a != c)

        var set: Set<ServerAddress> = []
        set.insert(a)
        set.insert(b)
        #expect(set.count == 1)
    }
}
