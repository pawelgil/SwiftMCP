#if Server
    import Foundation
    @testable import SwiftMCP
    import Testing

    @Suite("Logical session removal", .serialized)
    struct SessionRemovalTests {
        @Test
        func `DELETE cancels an admitted tool, suppresses its response and emits one end event`() async throws {
            try await withHTTPFixture { fixture in
                let id = try await fixture.initialize()
                let post = try await fixture.startHold(id)
                #expect(await fixture.server.observation.waitForStart())
                let session = try #require(await fixture.server.observation.session)
                let end = await fixture.observeEnd(session)

                var request = URLRequest(url: fixture.url)
                request.httpMethod = "DELETE"
                request.setValue(id, forHTTPHeaderField: "Mcp-Session-Id")
                let (_, response) = try await fixture.client.data(for: request)
                #expect((response as? HTTPURLResponse)?.statusCode == 204)
                #expect(await fixture.server.observation.waitForFinish())
                #expect(await fixture.server.observation.cancelled)
                #expect(await fixture.server.observation.naturalCompletion == false)
                #expect(await end.value == 1)
                let reply = await post.value
                #expect(reply.responseIDs.isEmpty)

                let lateEnd = await fixture.observeEnd(session)
                #expect(await lateEnd.value == 1)
                let (_, removed) = try await fixture.client.data(for: fixture.request(
                    .request(id: 3, method: "ping"), id: id
                ))
                #expect((removed as? HTTPURLResponse)?.statusCode == 404)
            }
        }

        @Test
        func `transport stop cancels all admitted sessions and emits each end once`() async throws {
            try await withHTTPFixture { fixture in
                let firstID = try await fixture.initialize()
                let firstSession = try #require(await fixture.transport.sessionManager.existingSession(
                    id: UUID(uuidString: firstID)!
                ))
                let firstEnd = await fixture.observeEnd(firstSession)
                let id = try await fixture.initialize()
                let post = try await fixture.startHold(id)
                #expect(await fixture.server.observation.waitForStart())
                let session = try #require(await fixture.server.observation.session)
                let end = await fixture.observeEnd(session)

                try await fixture.transport.stop()
                #expect(await fixture.server.observation.waitForFinish())
                #expect(await fixture.server.observation.cancelled)
                #expect(await fixture.server.observation.naturalCompletion == false)
                #expect(await firstEnd.value == 1)
                #expect(await end.value == 1)
                #expect(await post.value.responseIDs.isEmpty)
                #expect(await fixture.transport.sessionManager.sessionIDs.isEmpty)
            }
        }

        @Test
        func `request cancellation leaves the session live and normal requests still respond`() async throws {
            try await withHTTPFixture { fixture in
                let id = try await fixture.initialize()
                let post = try await fixture.startHold(id)
                #expect(await fixture.server.observation.waitForStart())
                let session = try #require(await fixture.server.observation.session)
                let end = await fixture.observeEnd(session)
                let notification = JSONRPCMessage.notification(
                    method: "notifications/cancelled", params: ["requestId": .integer(2)]
                )
                let (_, accepted) = try await fixture.client.data(for: fixture.request(notification, id: id))
                #expect((accepted as? HTTPURLResponse)?.statusCode == 202)
                #expect(await fixture.server.observation.waitForFinish())
                #expect(await fixture.server.observation.cancelled)
                #expect(await post.value.responseIDs.isEmpty)
                let (data, response) = try await fixture.client.data(for: fixture.request(
                    .request(id: 3, method: "ping"), id: id
                ))
                #expect((response as? HTTPURLResponse)?.statusCode == 200)
                #expect(SessionRemovalHTTPReply(data: data, response: response).responseIDs == [3])
                #expect(await fixture.transport.sessionManager.existingSession(id: session.id) === session)
                end.cancel()
                #expect(await end.value == 0)
            }
        }

        @Test
        func `closing a resumable POST socket does not end or cancel its session`() async throws {
            try await withHTTPFixture { fixture in
                let id = try await fixture.initialize()
                let post = try await fixture.startHold(id)
                #expect(await fixture.server.observation.waitForStart())
                let session = try #require(await fixture.server.observation.session)
                let end = await fixture.observeEnd(session)
                post.cancel()
                _ = await post.value
                #expect(await fixture.server.observation.waitForFinish())
                #expect(await fixture.server.observation.naturalCompletion)
                #expect(await fixture.server.observation.cancelled == false)
                #expect(await fixture.transport.sessionManager.existingSession(id: session.id) === session)
                end.cancel()
                #expect(await end.value == 0)
            }
        }

        @Test
        func `common destruction cancels late registration and refuses admitted work`() async {
            let manager = SessionManager()
            let session = await manager.session(id: UUID())
            let end = await session.ended()
            await manager.removeSession(id: session.id)
            let cancellation = SessionRemovalFlag()
            await session.registerInFlightRequest(id: .integer(7)) { cancellation.mark() }
            #expect(cancellation.value)
            #expect(await session.unregisterInFlightRequest(id: .integer(7)))
            let work = await session.workUnlessDisconnected { _ in true }
            #expect(work == nil)
            #expect(await manager.createStream(for: session, kind: .general) == nil)
            await manager.removeSession(id: session.id)
            #expect(await countSessionEndEvents(end) == 1)
        }

        @Test
        func `expiry uses the same cancellation and replayable end event`() async {
            let manager = SessionManager()
            let session = await manager.session(id: UUID())
            let cancellation = SessionRemovalFlag()
            await session.registerInFlightRequest(id: .integer(8)) { cancellation.mark() }
            await session.setExpiresAt(Date.distantPast)
            #expect(await manager.existingSession(id: session.id) == nil)
            #expect(cancellation.value)
            #expect(await session.unregisterInFlightRequest(id: .integer(8)))
            #expect(await countSessionEndEvents(session.ended()) == 1)
        }

        @Test
        func `cancelling one end observer does not end the session or affect another`() async {
            let manager = SessionManager()
            let session = await manager.session(id: UUID())
            let first = await session.ended()
            let second = await session.ended()
            let cancelled = Task { await countSessionEndEvents(first) }
            cancelled.cancel()
            #expect(await cancelled.value == 0)
            #expect(await manager.existingSession(id: session.id) === session)
            await manager.disconnectSession(session)
            #expect(await countSessionEndEvents(second) == 1)
        }

        private func withHTTPFixture(_ body: (SessionRemovalHTTPFixture) async throws -> Void) async throws {
            let fixture = try await SessionRemovalHTTPFixture()
            do {
                try await body(fixture)
                try await fixture.close()
            } catch {
                try? await fixture.close()
                throw error
            }
        }
    }

    private func countSessionEndEvents(_ events: AsyncStream<Void>) async -> Int {
        await withTaskGroup(of: Int.self) { group in
            group.addTask { var count = 0; for await _ in events {
                count += 1
            }; return count }
            group.addTask {
                do { try await Task.sleep(for: .seconds(8)); return -1 } catch { return 0 }
            }
            defer { group.cancelAll() }
            return await group.next() ?? -1
        }
    }

    private final class SessionRemovalFlag: @unchecked Sendable {
        private let lock = NSLock()
        private var storage = false
        var value: Bool {
            lock.withLock { storage }
        }

        func mark() {
            lock.withLock { storage = true }
        }
    }

    private actor SessionRemovalObservation {
        private(set) var session: Session?
        private(set) var cancelled = false
        private(set) var naturalCompletion = false
        func start(_ session: Session) {
            self.session = session
        }

        func finish(cancelled: Bool) {
            self.cancelled = cancelled; naturalCompletion = !cancelled
        }

        func waitForStart() async -> Bool {
            await wait { self.session != nil }
        }

        func waitForFinish() async -> Bool {
            await wait { self.cancelled || self.naturalCompletion }
        }

        private func wait(_ predicate: () -> Bool) async -> Bool {
            let deadline = ContinuousClock.now + .seconds(8)
            while !predicate(), ContinuousClock.now < deadline {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return predicate()
        }
    }

    @MCPServer(name: "SessionRemovalRegression")
    private final class SessionRemovalServer: @unchecked Sendable {
        let observation = SessionRemovalObservation()
        @MCPTool(description: "Finite cooperative hold")
        func hold() async throws -> String {
            await observation.start(Session.current)
            do {
                try await Task.sleep(for: .seconds(3))
                await observation.finish(cancelled: false)
                return "natural-completion"
            } catch {
                await observation.finish(cancelled: error is CancellationError && Task.isCancelled)
                throw error
            }
        }
    }

    private struct SessionRemovalHTTPReply: Sendable {
        let responseIDs: [Int]
        init(data: Data, response _: URLResponse) {
            guard let text = String(bytes: data, encoding: .utf8) else {
                Issue.record("Session-removal HTTP response was not valid UTF-8")
                responseIDs = []
                return
            }
            responseIDs = text.split(separator: "\n").compactMap { line in
                guard line.hasPrefix("data: "), let json = String(line.dropFirst(6)).data(using: .utf8),
                      let value = try? JSONSerialization.jsonObject(with: json) as? [String: Any],
                      value["result"] != nil || value["error"] != nil else { return nil }
                return value["id"] as? Int
            }
        }

        init() {
            responseIDs = []
        }
    }

    private actor SessionRemovalHTTPFixture {
        nonisolated let server: SessionRemovalServer
        nonisolated let transport: HTTPSSETransport
        nonisolated let client: URLSession
        nonisolated let url: URL
        private var posts: [Task<SessionRemovalHTTPReply, Never>] = []
        private var endObservers: [Task<Int, Never>] = []
        init() async throws {
            let server = SessionRemovalServer()
            let transport = HTTPSSETransport(server: server, host: "127.0.0.1", port: 0)
            let configuration = URLSessionConfiguration.ephemeral
            configuration.timeoutIntervalForRequest = 10
            configuration.timeoutIntervalForResource = 15
            let client = URLSession(configuration: configuration)
            try await transport.start()
            self.server = server
            self.transport = transport
            self.client = client
            url = URL(string: "http://127.0.0.1:\(transport.port)/mcp")!
        }

        nonisolated func request(_ message: JSONRPCMessage, id: String? = nil) throws -> URLRequest {
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.setValue("application/json, text/event-stream", forHTTPHeaderField: "Accept")
            request.setValue("2025-06-18", forHTTPHeaderField: "MCP-Protocol-Version")
            if let id {
                request.setValue(id, forHTTPHeaderField: "Mcp-Session-Id")
            }
            request.httpBody = try JSONEncoder().encode(message)
            return request
        }

        func initialize() async throws -> String {
            let message = JSONRPCMessage.request(id: 1, method: "initialize", params: [
                "protocolVersion": .string("2025-06-18"), "capabilities": .object([:]),
                "clientInfo": .object(["name": .string("removal-regression"), "version": .string("1")])
            ])
            let (_, response) = try await client.data(for: request(message))
            #expect((response as? HTTPURLResponse)?.statusCode == 200)
            let id = try #require((response as? HTTPURLResponse)?.value(forHTTPHeaderField: "Mcp-Session-Id"))
            _ = try await client.data(for: request(.notification(method: "notifications/initialized"), id: id))
            return id
        }

        func startHold(_ id: String) throws -> Task<SessionRemovalHTTPReply, Never> {
            let request = try request(.request(id: 2, method: "tools/call", params: [
                "name": .string("hold"), "arguments": .object([:])
            ]), id: id)
            let client = client
            let task = Task.detached {
                do {
                    let (data, response) = try await client.data(for: request)
                    return SessionRemovalHTTPReply(data: data, response: response)
                } catch { return SessionRemovalHTTPReply() }
            }
            posts.append(task)
            return task
        }

        func observeEnd(_ session: Session) async -> Task<Int, Never> {
            let events = await session.ended()
            let task = Task.detached { await countSessionEndEvents(events) }
            endObservers.append(task)
            return task
        }

        func close() async throws {
            client.invalidateAndCancel()
            for post in posts {
                post.cancel()
            }
            for observer in endObservers {
                observer.cancel()
            }
            for post in posts {
                _ = await post.value
            }
            for observer in endObservers {
                _ = await observer.value
            }
            try await transport.stop()
        }
    }
#endif
