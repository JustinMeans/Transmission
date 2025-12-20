import Vapor
import Transmission

public protocol TransmissionModule: Sendable {
    var name: String { get }
    func register(with system: TransmissionSystem, app: Application) async throws
}

extension Application.TransmissionProvider {
    public func registerModule(_ module: some TransmissionModule) async throws {
        guard let system else {
            throw Abort(.internalServerError, reason: "TransmissionSystem not registered")
        }
        try await module.register(with: system, app: application)
        application.logger.info("Registered Transmission module: \(module.name)")
    }
}
