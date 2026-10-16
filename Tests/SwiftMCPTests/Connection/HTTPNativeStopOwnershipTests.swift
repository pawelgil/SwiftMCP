#if Server && canImport(Network)
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

/// The gates wrap only the engine and the lifecycle's adapter entry. The actual
/// listener, accepted children, HTTP parser, response writes and shutdown are NIO.
private final class NativeStopEngine: MCPHTTPEngine, @unchecked Sendable {
    let transport: HTTPSSETransport
    let before: AdapterOwnershipGate?
    let response: AdapterOwnershipGate?
    let cancelled = AdapterOwnershipWaiter()
    let returned = HTTPTransportBox(false)

    init(_ transport: HTTPSSETransport, before: AdapterOwnershipGate? = nil,
         response: AdapterOwnershipGate? = nil) {
        self.transport = transport
        self.before = before
        self.response = response
    }

    var configuredHost: String { transport.configuredHost }
    var configuredPort: Int { transport.configuredPort }
    func maxBodySize(for head: HTTPRequest) -> Int { transport.maxBodySize(for: head) }

    func handle(head: HTTPRequest, bodyStream: AsyncStream<Data>) async -> EngineResponse {
        await withTaskCancellationHandler {
            await before?.enter()
            let result = await transport.handle(head: head, bodyStream: bodyStream)
            await response?.enter()
            returned.value = true
            return result
        } onCancel: {
            self.cancelled.complete(.success(()))
        }
    }

    func registerConnection(_ connection: any SSEConnection,
                            for registration: SSERegistration) async -> SSEConnectionToken? {
        await transport.registerConnection(connection, for: registration)
    }

    func connectionDisconnected(_ token: SSEConnectionToken) async {
        await transport.connectionDisconnected(token)
    }
}

private final class NativeStopAdapter: HTTPTransportAdapter, @unchecked Sendable {
    let native: NIOHTTPServerAdapter
    let afterFirstSweep: AdapterOwnershipGate?
    let disposed = HTTPTransportBox(false)

    init(engine: NativeStopEngine, afterFirstSweep: AdapterOwnershipGate? = nil) {
        native = NIOHTTPServerAdapter(engine: engine, logger: engine.transport.logger)
        self.afterFirstSweep = afterFirstSweep
    }

    var currentTaskIsOwned: Bool { native.currentTaskIsOwned }
    func sealRequests() { native.sealRequests() }
    func start() async throws -> Int { try await native.start() }
    func waitUntilClosed() async throws { try await native.waitUntilClosed() }
    func shutdown(afterDrain: @escaping @Sendable () async -> Void) async throws {
        // Lifecycle calls this only after its first real session sweep.
        await afterFirstSweep?.enter()
        try await native.shutdown(afterDrain: afterDrain)
        disposed.value = true
    }
}

@Suite("Native HTTP stop ownership", .serialized, .timeLimit(.minutes(1)))
struct HTTPNativeStopOwnershipTests {
    @Test("Real NIO stop joins the held original MCP route and removes its late session before restart")
    func heldMCPRouteAcrossStop() async throws {
        let transport = HTTPSSETransport(server: Calculator(), host: "127.0.0.1", port: 0)
        transport.keepAliveMode = .none
        let before = AdapterOwnershipGate()
        let response = AdapterOwnershipGate()
        let sweep = AdapterOwnershipGate()
        let engine = NativeStopEngine(transport, before: before, response: response)
        let adapter = NativeStopAdapter(engine: engine, afterFirstSweep: sweep)
        let stopped = HTTPTransportBox(false)
        let client = URLSession(configuration: .ephemeral)

        try await withOwnershipCleanup(gates: [before, response, sweep]) { cleanup in
            cleanup.onRelease { client.invalidateAndCancel() }
            cleanup.onFinish { try? await transport.stop() }
            try await transport.startHTTP(using: { adapter })
            let url = URL(string: "http://127.0.0.1:\(transport.port)/mcp")!
            let request = try HTTPTransportTestHelpers.streamablePOSTRequest(
                url: url, message: HTTPTransportTestHelpers.initializeRequest()
            )
            let post = cleanup.retain(Task { _ = try? await client.data(for: request) })
            try await before.waitForArrival() // The real accepted channel dispatched /mcp.
            let stop = cleanup.retain(Task {
                try await transport.stop()
                stopped.value = true
            })
            try await sweep.waitForArrival()
            #expect(await transport.sessionManager.sessions.isEmpty)
            #expect(adapter.native.dispatch.requestsAreSealed)

            // Materialize a genuine initialize session AFTER the first sweep,
            // before the original adapter begins cancellation/channel closure.
            await before.open()
            try await response.waitForArrival()
            let lateID = try #require(await transport.sessionManager.sessionIDs.first)
            #expect(await transport.sessionManager.sessions.count == 1)
            await sweep.open()
            try await engine.cancelled.wait() // Actual NIO shutdown cancelled this original route.
            #expect(!engine.returned.value)
            #expect(!adapter.disposed.value)
            #expect(!stopped.value)

            await response.open()
            try await stop.value
            await post.value
            #expect(engine.returned.value)
            #expect(adapter.disposed.value)
            #expect(await transport.sessionManager.sessions.isEmpty)
            #expect(await transport.sessionManager.streamMeta.isEmpty)
            try await assertFreshGeneration(transport, previousSession: lateID, client: client)
        }
    }

    @Test("A real chunked 413 closes once and stop joins the already-admitted route and accepted child")
    func chunkedRejectionAcrossStop() async throws {
        let transport = HTTPSSETransport(server: Calculator(), host: "127.0.0.1", port: 0)
        transport.keepAliveMode = .none
        transport.maxMessageSize = 16
        let before = AdapterOwnershipGate()
        let engine = NativeStopEngine(transport, before: before)
        let adapter = NativeStopAdapter(engine: engine)
        let stopped = HTTPTransportBox(false)
        let client = URLSession(configuration: .ephemeral)

        try await withOwnershipCleanup(gates: [before]) { cleanup in
            cleanup.onRelease { client.invalidateAndCancel() }
            cleanup.onFinish { try? await transport.stop() }
            try await transport.startHTTP(using: { adapter })
            let socket = try TestTCPClient(port: UInt16(transport.port))
            cleanup.onRelease { socket.closeSocket() }
            await offPool {
                socket.send("POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
                    + "Accept: application/json, text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
            }
            try await before.waitForArrival()
            await offPool { socket.send("11\r\n" + String(repeating: " ", count: 17) + "\r\n0\r\n\r\n") }
            let lines = await offPool {
                var lines: [String] = []
                while let line = socket.readLine() { lines.append(line) }
                return lines
            }
            #expect(lines.first?.hasPrefix("HTTP/1.1 413") == true)
            #expect(lines.filter { $0.hasPrefix("HTTP/1.1 ") }.count == 1)
            #expect(await offPool { socket.readsToEndOfFile() })
            #expect(!engine.returned.value)

            let stop = cleanup.retain(Task {
                try await transport.stop()
                stopped.value = true
            })
            try await engine.cancelled.wait()
            #expect(!adapter.disposed.value)
            #expect(!stopped.value)
            await before.open()
            try await stop.value
            #expect(engine.returned.value)
            #expect(adapter.disposed.value)
            #expect(await transport.sessionManager.sessions.isEmpty)
            transport.maxMessageSize = 4 * 1024 * 1024
            try await assertFreshGeneration(transport, previousSession: nil, client: client)
        }
    }

    private func assertFreshGeneration(_ transport: HTTPSSETransport, previousSession: UUID?,
                                       client: URLSession) async throws {
        // Production start: a new real NIO adapter, with no test wrappers/gates.
        try await transport.start()
        let url = URL(string: "http://127.0.0.1:\(transport.port)/mcp")!
        let (id, events) = try await HTTPTransportTestHelpers.initializeSession(url: url, urlSession: client)
        #expect(HTTPTransportTestHelpers.responseEvent(events, id: 1) != nil)
        #expect(UUID(uuidString: id) != previousSession)
        #expect(await transport.sessionManager.sessions.count == 1)
        let request = try HTTPTransportTestHelpers.streamablePOSTRequest(
            url: url, message: .request(id: 2, method: "ping"), sessionID: id
        )
        let (reply, ping) = try await HTTPTransportTestHelpers.readFiniteSSEResponse(request, urlSession: client)
        #expect(reply.statusCode == 200)
        #expect(HTTPTransportTestHelpers.responseEvent(ping, id: 2) != nil)
        try await transport.stop()
        #expect(await transport.sessionManager.sessions.isEmpty)
        #expect(await transport.sessionManager.streamMeta.isEmpty)
    }
}
#endif
