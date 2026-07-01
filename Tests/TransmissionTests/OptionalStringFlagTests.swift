/// OptionalStringFlagTests.swift
///
/// Regression tests for the strict validation of the optional-presence flag byte
/// written by CompactEncoder.writeOptionalString and consumed by
/// CompactDecoder.readOptionalString.
///
/// BUG: Before the fix, readOptionalString treated any byte != 1 as "absent" (nil)
/// rather than validating that the flag byte is exactly 0 (absent) or 1 (present).
/// A malformed frame containing byte value 2 (or any value 2-255) for the optional
/// flag would silently decode to nil instead of throwing a typed error, violating
/// the strict-decode contract that governs all other length-prefixed fields in the
/// compact binary protocol.
///
/// FIX: readOptionalString now validates the flag byte with a switch and throws
/// TransmissionError.decodingFailed for any value outside {0, 1}.

import Testing
import Foundation
@testable import Transmission

@Suite("OptionalStringFlag")
struct OptionalStringFlagTests {

    // MARK: - Round-trip: present value

    @Test("writeOptionalString(present) round-trips through readOptionalString")
    func roundTripPresent() throws {
        var encoder = CompactEncoder()
        encoder.writeOptionalString("hello")
        var decoder = CompactDecoder(encoder.data)
        let result = try decoder.readOptionalString()
        #expect(result == "hello")
    }

    // MARK: - Round-trip: nil value

    @Test("writeOptionalString(nil) round-trips through readOptionalString")
    func roundTripNil() throws {
        var encoder = CompactEncoder()
        encoder.writeOptionalString(nil)
        var decoder = CompactDecoder(encoder.data)
        let result = try decoder.readOptionalString()
        #expect(result == nil)
    }

    // MARK: - Invalid flag byte 2 throws (regression: was silently decoded as nil)

    /// Before the fix, a flag byte of 2 was treated as nil (the else-branch of
    /// `if hasValue == 1`). This test verifies that it now throws, enforcing the
    /// strict binary protocol contract that only 0 and 1 are valid flag values.
    @Test("flag byte 2 throws decodingFailed instead of returning nil")
    func invalidFlagByte2Throws() {
        var encoder = CompactEncoder()
        encoder.writeUInt8(2)          // invalid: not 0 (nil) or 1 (present)
        encoder.writeString("hidden")  // would be read only if flag were 1
        var decoder = CompactDecoder(encoder.data)
        #expect(throws: (any Error).self, "flag byte 2 must throw, not silently return nil") {
            _ = try decoder.readOptionalString()
        }
    }

    // MARK: - Invalid flag byte 255 also throws

    @Test("flag byte 255 throws decodingFailed instead of returning nil")
    func invalidFlagByte255Throws() {
        var encoder = CompactEncoder()
        encoder.writeUInt8(255)
        var decoder = CompactDecoder(encoder.data)
        #expect(throws: (any Error).self, "flag byte 255 must throw, not silently return nil") {
            _ = try decoder.readOptionalString()
        }
    }

    // MARK: - Flag byte 0 is unambiguously nil

    @Test("flag byte 0 is unambiguously nil (not an error)")
    func flagByte0IsNil() throws {
        var encoder = CompactEncoder()
        encoder.writeUInt8(0)
        var decoder = CompactDecoder(encoder.data)
        let result = try decoder.readOptionalString()
        #expect(result == nil)
    }

    // MARK: - Flag byte 1 is unambiguously present

    @Test("flag byte 1 followed by string is present and correct")
    func flagByte1IsPresent() throws {
        var encoder = CompactEncoder()
        encoder.writeUInt8(1)
        encoder.writeString("world")
        var decoder = CompactDecoder(encoder.data)
        let result = try decoder.readOptionalString()
        #expect(result == "world")
    }
}
