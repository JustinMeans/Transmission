import Testing
import Foundation
@testable import Transmission

@Suite("TLS Selection Tests")
struct TLSSelectionTests {

    @Test("Secure scheme requires TLS")
    func secureRequiresTLS() {
        #expect(ClientConnectionHandler.requiresTLS(.secure) == true)
    }

    @Test("Insecure scheme does not require TLS")
    func insecureDoesNotRequireTLS() {
        #expect(ClientConnectionHandler.requiresTLS(.insecure) == false)
    }

    @Test("wss URL parses to secure scheme with port 443")
    func wssURLDefaults() throws {
        let address = try #require(ServerAddress(url: "wss://x.trycloudflare.com"))
        #expect(address.scheme == .secure)
        #expect(address.port == 443)
        #expect(address.path == "/transmission")
        #expect(ClientConnectionHandler.requiresTLS(address.scheme) == true)
    }

    @Test("ws URL parses to insecure scheme with port 80")
    func wsURLDefaults() throws {
        let address = try #require(ServerAddress(url: "ws://localhost"))
        #expect(address.scheme == .insecure)
        #expect(address.port == 80)
        #expect(ClientConnectionHandler.requiresTLS(address.scheme) == false)
    }

    @Test("wss URL with explicit port keeps that port and stays secure")
    func wssExplicitPort() throws {
        let address = try #require(ServerAddress(url: "wss://host.example.com:9443/transmission"))
        #expect(address.scheme == .secure)
        #expect(address.port == 9443)
        #expect(ClientConnectionHandler.requiresTLS(address.scheme) == true)
    }
}
