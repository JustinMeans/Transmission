extension TransmissionSystem { public var serverHeartbeat: HeartbeatPolicy? { get { nil } set {} } }
extension ClientConnectionHandler {
    static func sanitizedHeaders(_ h: [String: String]) throws -> [String: String] { h }
    static func hostHeader(for a: ServerAddress) -> String { a.host }
}
