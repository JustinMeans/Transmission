import Vapor
import JWT
import Transmission

public struct TransmissionJWTPayload: JWTPayload, Sendable {
    public let subject: SubjectClaim
    public let expiration: ExpirationClaim
    public let nodeID: String?
    public let permissions: [String]

    public init(
        subject: String,
        expiration: Date,
        nodeID: String? = nil,
        permissions: [String] = []
    ) {
        self.subject = SubjectClaim(value: subject)
        self.expiration = ExpirationClaim(value: expiration)
        self.nodeID = nodeID
        self.permissions = permissions
    }

    public func verify(using key: some JWTAlgorithm) throws {
        try expiration.verifyNotExpired()
    }
}

public struct TransmissionAuthMiddleware: AsyncMiddleware {
    private let jwtKeyCollection: JWTKeyCollection

    public init(keyCollection: JWTKeyCollection) {
        self.jwtKeyCollection = keyCollection
    }

    public func respond(to request: Request, chainingTo next: any AsyncResponder) async throws -> Response {
        guard let token = request.headers.bearerAuthorization?.token else {
            throw Abort(.unauthorized, reason: "Missing authorization token")
        }

        do {
            let payload = try await jwtKeyCollection.verify(token, as: TransmissionJWTPayload.self)
            request.storage[TransmissionAuthKey.self] = payload
        } catch {
            throw Abort(.unauthorized, reason: "Invalid token")
        }

        return try await next.respond(to: request)
    }
}

private struct TransmissionAuthKey: StorageKey {
    typealias Value = TransmissionJWTPayload
}

extension Request {
    public var transmissionAuth: TransmissionJWTPayload? {
        storage[TransmissionAuthKey.self]
    }
}

extension Application.TransmissionProvider {
    public func configureWithAuth(
        path: String = "transmission",
        keyCollection: JWTKeyCollection
    ) throws {
        let middleware = TransmissionAuthMiddleware(keyCollection: keyCollection)
        try configure(path: path, middleware: [middleware])
    }
}
