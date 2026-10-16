#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

private enum OwnershipFixtureError: Error { case bind, disposal }

private final class OwnershipBox<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value
    init(_ value: Value) { self.value = value }
    func read() -> Value { lock.withLock { value } }
    func update(_ body: (inout Value) -> Void) { lock.withLock { body(&value) } }
}

/// Uses the actual production shutdown operation, without allocating NIO loops.
private final class OwnershipLifecycleAdapter: HTTPTransportAdapter, @unchecked Sendable {
    let owner = HTTPAdapterDispatch()
    let events = AdapterOwnershipEvents()
    let bindGate: AdapterOwnershipGate?
    let listenerGate: AdapterOwnershipGate?
    let disposalGate: AdapterOwnershipGate?
    let failBind: Bool
    let failDisposal: Bool
    let retained: AnyObject?
    let disposed = OwnershipBox(false)

    init(
        bind: AdapterOwnershipGate? = nil,
        listener: AdapterOwnershipGate? = nil,
        disposal: AdapterOwnershipGate? = nil,
        failBind: Bool = false,
        failDisposal: Bool = false,
        retained: AnyObject? = nil
    ) {
        bindGate = bind
        listenerGate = listener
        disposalGate = disposal
        self.failBind = failBind
        self.failDisposal = failDisposal
        self.retained = retained
    }

    var currentTaskIsOwned: Bool { owner.currentTaskIsOwned }
    func sealRequests() { owner.sealRequests() }
    func start() async throws -> Int {
        await bindGate?.enter()
        await events.mark("bind")
        if failBind { throw OwnershipFixtureError.bind }
        return 7_777
    }
    func waitUntilClosed() async throws {}
    func shutdown(afterDrain: @escaping @Sendable () async -> Void) async throws {
        try await HTTPAdapterShutdownOperation.run(
            owner: owner,
            closeListener: { await self.listenerGate?.enter() },
            closeChildren: {}, afterDrain: afterDrain,
            shutdown: {
                await self.events.mark("disposal")
                await self.disposalGate?.enter()
                if self.failDisposal { throw OwnershipFixtureError.disposal }
                self.disposed.update { $0 = true }
            }
        )
    }
}

private func startLifecycle(
    _ lifecycle: HTTPTransportLifecycle,
    adapter: OwnershipLifecycleAdapter,
    before: @escaping @Sendable () async -> Void = {},
    after: @escaping @Sendable () async -> Void = {}
) async throws {
    try await lifecycle.start(
        prepare: {}, makeAdapter: { adapter }, didStart: { _ in },
        beforeDrain: before, afterDrain: after
    )
}

private final class OwnershipHeldDispatcher: MCPDispatcher, Sendable {
    let called: AdapterOwnershipGate
    let work: Task<Void, Never>

    init(called: AdapterOwnershipGate, held: AdapterOwnershipGate, events: AdapterOwnershipEvents) {
        self.called = called
        self.work = Task.detached {
            await held.enter()
            await events.mark("dispatchWorkReturned")
        }
    }

    func handle(_ message: JSONRPCMessage) async -> JSONRPCMessage? {
        await called.enter()
        await work.value
        return nil
    }

    func handle(_ batch: [JSONRPCMessage]) async -> [JSONRPCMessage] {
        await called.enter()
        await work.value
        return []
    }
}

@Suite("HTTP transport stop generations", .timeLimit(.minutes(1)))
struct HTTPTransportStopOwnershipTests {
    @Test("Concurrent stop callers await one original disposal operation")
    func concurrentStop() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let disposal = AdapterOwnershipGate()
        let secondReserved = AdapterOwnershipWaiter()
        let adapter = OwnershipLifecycleAdapter(disposal: disposal)
        try await withOwnershipCleanup(owner: adapter.owner, gates: [disposal]) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            try await startLifecycle(lifecycle, adapter: adapter)
            let first = cleanup.retain(Task { try await lifecycle.stop(beforeDrain: {}, afterDrain: {}) })
            try await disposal.waitForArrival()
            let second = cleanup.retain(Task {
                try await lifecycle.stop(beforeDrain: {}, afterDrain: {}, didReserve: {
                    secondReserved.complete(.success(()))
                })
            })
            try await secondReserved.wait()
            #expect(!adapter.disposed.read())
            await disposal.open()
            try await first.value
            try await second.value
            #expect(await adapter.events.count("disposal") == 1)
        }
    }

    @Test("Stop seals a suspended bind and restart waits for its original disposal")
    func bindStopRestart() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let bind = AdapterOwnershipGate()
        let firstSweep = AdapterOwnershipGate()
        let disposal = AdapterOwnershipGate()
        let first = OwnershipLifecycleAdapter(bind: bind, disposal: disposal)
        let second = OwnershipLifecycleAdapter()
        let factoryCount = OwnershipBox(0)
        let overlapped = OwnershipBox(false)
        let factory: @Sendable () -> any HTTPTransportAdapter = {
            var next = 0
            factoryCount.update { $0 += 1; next = $0 }
            if next == 1 { return first }
            if !first.disposed.read() { overlapped.update { $0 = true } }
            return second
        }
        try await withOwnershipCleanup(owner: first.owner, gates: [bind, firstSweep, disposal]) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            let initial = cleanup.retain(Task {
                try await lifecycle.start(
                    prepare: {}, makeAdapter: factory, didStart: { _ in },
                    beforeDrain: { await firstSweep.enter() }, afterDrain: {}
                )
            })
            try await bind.waitForArrival()
            let stop = cleanup.retain(Task { try await lifecycle.stop(beforeDrain: {}, afterDrain: {}) })
            try await firstSweep.waitForArrival()
            #expect(first.owner.requestsAreSealed)
            let restart = cleanup.retain(Task {
                try await lifecycle.start(
                    prepare: {}, makeAdapter: factory, didStart: { _ in }, beforeDrain: {}, afterDrain: {}
                )
            })
            await firstSweep.open()
            await bind.open()
            try await disposal.waitForArrival()
            #expect(factoryCount.read() == 1)
            await disposal.open()
            try await initial.value
            try await stop.value
            try await restart.value
            #expect(factoryCount.read() == 2)
            #expect(!overlapped.read())
            try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
            await second.owner.join()
        }
    }

    @Test("A failed bind joins its allocated adapter cleanup before returning or retrying")
    func failedBindCleanup() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let disposal = AdapterOwnershipGate()
        let adapter = OwnershipLifecycleAdapter(disposal: disposal, failBind: true)
        try await withOwnershipCleanup(owner: adapter.owner, gates: [disposal]) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            let start = cleanup.retain(Task { try await startLifecycle(lifecycle, adapter: adapter) })
            try await disposal.waitForArrival()
            #expect(!adapter.disposed.read())
            await disposal.open()
            let result = await start.result
            if case .success = result { Issue.record("expected bind failure") }
            #expect(adapter.disposed.read())
            let retry = OwnershipLifecycleAdapter()
            try await startLifecycle(lifecycle, adapter: retry)
            try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
            await retry.owner.join()
        }
    }

    @Test("Disposal failure remains shared and prevents a fresh generation")
    func disposalFailureRetained() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let adapter = OwnershipLifecycleAdapter(failDisposal: true)
        try await withOwnershipCleanup(owner: adapter.owner, gates: []) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            try await startLifecycle(lifecycle, adapter: adapter)
            let first = cleanup.retain(Task { try await lifecycle.stop(beforeDrain: {}, afterDrain: {}) })
            let firstResult = await first.result
            if case .success = firstResult { Issue.record("expected disposal failure") }
            let second = cleanup.retain(Task { try await lifecycle.stop(beforeDrain: {}, afterDrain: {}) })
            let secondResult = await second.result
            if case .success = secondResult { Issue.record("expected retained disposal failure") }
            let factoryUsed = OwnershipBox(false)
            do {
                try await lifecycle.start(
                    prepare: {}, makeAdapter: { factoryUsed.update { $0 = true }; return adapter },
                    didStart: { _ in }, beforeDrain: {}, afterDrain: {}
                )
                Issue.record("restart unexpectedly ignored failed disposal")
            } catch {}
            #expect(!factoryUsed.read())
            #expect(await adapter.events.count("disposal") == 1)
        }
    }

    @Test("An owned caller cannot start a self-join or mutate admission")
    func ownedCallerRefused() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let adapter = OwnershipLifecycleAdapter()
        try await withOwnershipCleanup(owner: adapter.owner, gates: []) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            try await startLifecycle(lifecycle, adapter: adapter)
            let original = cleanup.retain(adapter.owner.startRequest { _ in
                do {
                    try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
                    Issue.record("owned stop unexpectedly succeeded")
                } catch {
                    #expect(error as? HTTPAdapterLifecycleError == .ownedOperationCannotStop)
                }
                do {
                    try await startLifecycle(lifecycle, adapter: adapter)
                    Issue.record("owned start unexpectedly succeeded")
                } catch {
                    #expect(error as? HTTPAdapterLifecycleError == .ownedOperationCannotStop)
                }
                #expect(!adapter.owner.requestsAreSealed)
            })
            await original?.task.value
            try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
        }
    }

    @Test("Inherited stale task-local identity does not permanently block stop")
    func staleIdentity() async throws {
        let lifecycle = HTTPTransportLifecycle()
        let adapter = OwnershipLifecycleAdapter()
        let stale = AdapterOwnershipGate()
        let child = OwnershipBox<Task<Void, Error>?>(nil)
        try await withOwnershipCleanup(owner: adapter.owner, gates: [stale]) { cleanup in
            cleanup.onFinish { try? await lifecycle.stop(beforeDrain: {}, afterDrain: {}) }
            try await startLifecycle(lifecycle, adapter: adapter)
            let parent = cleanup.retain(adapter.owner.startRequest { _ in
                let inherited = Task {
                    await stale.enter()
                    try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
                }
                child.update { $0 = inherited }
                cleanup.retain(inherited)
            })
            await parent?.task.value
            await stale.open()
            if let inherited = child.read() { try await inherited.value }
            #expect(adapter.disposed.read())
        }
    }

    @Test("Successful stop releases the generation's adapter and callback payloads")
    func successfulStopReleasesPayload() async throws {
        final class Sentinel: @unchecked Sendable {}
        final class WeakSentinel: @unchecked Sendable { weak var value: Sentinel? }
        let lifecycle = HTTPTransportLifecycle()
        let weakSentinel = WeakSentinel()
        func startRetainingPayload() async throws {
            let sentinel = Sentinel()
            weakSentinel.value = sentinel
            let adapter = OwnershipLifecycleAdapter(retained: sentinel)
            try await startLifecycle(lifecycle, adapter: adapter,
                before: { _ = sentinel }, after: { _ = sentinel })
        }
        try await startRetainingPayload()
        #expect(weakSentinel.value != nil)
        try await lifecycle.stop(beforeDrain: {}, afterDrain: {})
        #expect(weakSentinel.value == nil)
    }

    @Test("Repeated idle transport stops sweep sessions created since the previous stop")
    func repeatedIdleStop() async throws {
        let transport = HTTPSSETransport(server: Calculator())
        _ = await transport.sessionManager.session(id: UUID())
        try await transport.stop()
        #expect(await transport.sessionManager.sessions.count == 0)
        _ = await transport.sessionManager.session(id: UUID())
        #expect(await transport.sessionManager.sessions.count == 1)
        try await transport.stop()
        #expect(await transport.sessionManager.sessions.count == 0)
    }

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
                    head: HTTPRequest(method: .post, scheme: nil, authority: nil, path: "/mcp", headerFields: [
                        .accept: "application/json, text/event-stream",
                        .contentType: "application/json"
                    ]), body: body.stream,
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
