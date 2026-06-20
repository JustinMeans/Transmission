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

/// Regression tests for path normalization.
///
/// `ServerAddress.url` builds a `URL` via `URLComponents`. When an authority
/// (host) is present, `URLComponents.url` returns `nil` for any non-empty path
/// that does not begin with `/`. The `url` property previously force-unwrapped
/// that result, so a `ServerAddress` constructed with a relative path such as
/// `"ws"` would TRAP — crashing the whole process — the moment `url` was read
/// (e.g. while establishing a connection).
///
/// The fix normalizes every path to leading-slash origin form in `init` and
/// makes `url` non-trapping as defense in depth.
@Suite("ServerAddress Path Normalization Tests")
struct ServerAddressPathNormalizationTests {

    @Test("Relative path is normalized to a leading-slash path")
    func relativePathGetsLeadingSlash() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443, path: "ws")
        #expect(address.path == "/ws")
    }

    @Test("Path with leading slash is preserved unchanged")
    func absolutePathPreserved() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443, path: "/custom/ws")
        #expect(address.path == "/custom/ws")
    }

    @Test("Empty path falls back to the default")
    func emptyPathDefaults() {
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 443, path: "")
        #expect(address.path == "/transmission")
    }

    @Test("url does not trap for a relative path (the regression)")
    func urlDoesNotTrapForRelativePath() {
        // Before the fix this construction stored "ws" verbatim and `url`
        // force-unwrapped a nil URLComponents.url, crashing the process.
        let address = ServerAddress(scheme: .secure, host: "api.example.com", port: 8443, path: "ws")
        let url = address.url
        #expect(url.scheme == "wss")
        #expect(url.host == "api.example.com")
        #expect(url.port == 8443)
        #expect(url.path == "/ws")
    }

    @Test("Description is well-formed for a relative path input")
    func descriptionWellFormedForRelativePath() {
        let address = ServerAddress(scheme: .insecure, host: "localhost", port: 8080, path: "ws")
        #expect(address.description == "ws://localhost:8080/ws")
    }

    @Test("Round-trip: url string from a relative-path address re-parses identically")
    func relativePathRoundTrip() throws {
        let original = ServerAddress(scheme: .secure, host: "api.example.com", port: 8443, path: "ws")
        let reparsed = try #require(ServerAddress(url: original.description))
        #expect(reparsed.scheme == original.scheme)
        #expect(reparsed.host == original.host)
        #expect(reparsed.port == original.port)
        #expect(reparsed.path == original.path)
        #expect(reparsed == original)
    }

    @Test("Equality is unaffected by relative vs absolute input for the same path")
    func relativeAndAbsoluteInputsAreEqual() {
        let relative = ServerAddress(scheme: .secure, host: "h", port: 1, path: "p")
        let absolute = ServerAddress(scheme: .secure, host: "h", port: 1, path: "/p")
        #expect(relative == absolute)
    }
}
