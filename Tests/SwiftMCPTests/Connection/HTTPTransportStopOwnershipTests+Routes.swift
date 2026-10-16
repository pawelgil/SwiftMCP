#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

extension HTTPTransportStopOwnershipTests {
    @Test("Real initialize admitted before stop is swept after late body/session creation")
    func lateRealSessionMaterialization() async throws {
        let transport = HTTPSSETransport(server: Calculator(), port: 0)
        transport.keepAliveMode = .none
        let listener = AdapterOwnershipGate()
        let response = AdapterOwnershipGate()
        let adapter = OwnershipLifecycleAdapter(listener: listener)
        let body = AsyncStream<Data>.makeStream()
        let payload = try HTTPTransportTestHelpers.encode(HTTPTransportTestHelpers.initializeRequest())
        try await withOwnershipCleanup(owner: adapter.owner, gates: [listener, response]) { cleanup in
            cleanup.onRelease { body.continuation.finish() }
            cleanup.onFinish { try? await transport.stop() }
            try await transport.startHTTP(using: { adapter })
            let original = cleanup.retain(adapter.owner.startRequest { parent in
                await HTTPAdapterRouteOperation.run(
                    owner: adapter.owner, parent: parent, engine: transport,
                    request: (head: HTTPRequest(
                        method: .post, scheme: nil, authority: nil, path: "/mcp",
                        headerFields: [.accept: "application/json, text/event-stream", .contentType: "application/json"]
                    ), body: body.stream),
                    writer: ownershipWriter(head: { _ in await response.enter(); return .discarded })
                )
            })
            let stop = cleanup.retain(Task { try await transport.stop() })
            try await listener.waitForArrival() // First production session sweep has returned.
            #expect(await transport.sessionManager.sessions.count == 0)
            // Keep listener closure held: explicit request cancellation has not
            // happened yet, so the real body consumer can materialize a session.
            body.continuation.yield(payload)
            body.continuation.finish()
            try await response.waitForArrival()
            #expect(await transport.sessionManager.sessions.count == 1)
            #expect(!adapter.disposed.read())
            await listener.open()
            await response.open()
            await original?.task.value
            try await stop.value
            #expect(await transport.sessionManager.sessions.count == 0)
            #expect(adapter.disposed.read())
        }
    }

    @Test("Response-consumer completion does not join the separate inner MCP batch producer")
    func excludedBatchProducer() async throws {
        let transport = HTTPSSETransport(port: 0)
        let adapter = InMemoryHTTPServerAdapter(engine: transport)
        let called = AdapterOwnershipGate()
        let held = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let payload = try HTTPTransportTestHelpers.encode(HTTPTransportTestHelpers.initializeRequest())
        await called.open()
        let dispatcher = OwnershipHeldDispatcher(called: called, held: held, events: events)
        transport.connect(to: dispatcher)
        try await withOwnershipCleanup(gates: [called, held]) { cleanup in
            cleanup.retain(dispatcher.work)
            cleanup.onFinish { try? await transport.stop() }
            let exchange = await adapter.send(
                method: .post, path: "/mcp",
                headerFields: [.accept: "application/json, text/event-stream", .contentType: "application/json"],
                body: payload
            )
            guard case .sse(let stream) = exchange.body else {
                Issue.record("expected real MCP request SSE response")
                return
            }
            try await called.waitForArrival() // Actual inner batch reached the dispatcher.
            let consumer = cleanup.retain(Task {
                for await _ in stream {}
            })
            consumer.cancel()
            await consumer.value
            #expect(await events.count("dispatchWorkReturned") == 0)
            if let raw = exchange.headerFields[.mcpSessionID], let id = UUID(uuidString: raw) {
                let session = await transport.sessionManager.existingSession(id: id)
                #expect(session != nil)
                #expect(session?.disconnection.isMarked == false)
            } else {
                Issue.record("expected retained legacy initialize session")
            }
            await held.open()
            await dispatcher.work.value
            #expect(await events.count("dispatchWorkReturned") == 1)
            // We joined the original test-owned dispatcher work and consumer.
            // MCPRoutes' outer producer handle is unexposed and remains explicitly
            // UNJOINED; observed dispatch completion is not that Task.value join.
            // Timer outer/nested ping tasks are also outside this patch's claim.
        }
    }
}
#endif
