#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

/// Replayable gates ignore cancellation deliberately: a cancelled original is
/// still an obligation until its body actually returns. No sleeps or listeners.
actor AdapterOwnershipGate {
    private var opened = false
    private var arrivals = 0
    private var held: [CheckedContinuation<Void, Never>] = []
    private var watchers: [(Int, AdapterOwnershipWaiter)] = []

    func enter() async {
        arrivals += 1
        let ready = watchers.filter { $0.0 <= arrivals }
        watchers.removeAll { $0.0 <= arrivals }
        ready.forEach { $0.1.complete(.success(())) }
        if !opened { await withCheckedContinuation { held.append($0) } }
    }

    func waitForArrival(_ count: Int = 1) async throws {
        if arrivals < count {
            let waiter = AdapterOwnershipWaiter()
            watchers.append((count, waiter))
            try await waiter.wait()
        }
    }

    func open() {
        opened = true
        let pending = held
        held.removeAll()
        pending.forEach { $0.resume() }
    }
}

/// Cancellation can settle a test watcher synchronously, without launching an
/// unowned actor-hop task from the cancellation handler.
final class AdapterOwnershipWaiter: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<Void, Error>?
    private var continuation: CheckedContinuation<Void, Error>?

    func wait() async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let completed: Result<Void, Error>? = lock.withLock {
                    if let result { return result }
                    self.continuation = continuation
                    return nil
                }
                if let completed { continuation.resume(with: completed) }
            }
        } onCancel: {
            self.complete(.failure(CancellationError()))
        }
    }

    func complete(_ result: Result<Void, Error>) {
        let pending = lock.withLock { () -> CheckedContinuation<Void, Error>? in
            guard self.result == nil else { return nil }
            self.result = result
            let pending = continuation
            continuation = nil
            return pending
        }
        pending?.resume(with: result)
    }
}

final class AdapterOwnershipCleanup: @unchecked Sendable {
    private let lock = NSLock()
    private var originals: [Task<Void, Never>] = []
    private var stops: [Task<Void, Error>] = []
    private var finalizers: [@Sendable () async -> Void] = []
    private var releases: [@Sendable () -> Void] = []

    func onRelease(_ operation: @escaping @Sendable () -> Void) {
        lock.withLock { releases.append(operation) }
    }

    func onFinish(_ operation: @escaping @Sendable () async -> Void) {
        lock.withLock { finalizers.append(operation) }
    }

    @discardableResult
    func retain(_ original: HTTPAdapterDispatch.Operation?) -> HTTPAdapterDispatch.Operation? {
        if let original { lock.withLock { originals.append(original.task) } }
        return original
    }

    @discardableResult
    func retain(_ original: Task<Void, Never>) -> Task<Void, Never> {
        lock.withLock { originals.append(original) }
        return original
    }

    @discardableResult
    func retain(_ stop: Task<Void, Error>) -> Task<Void, Error> {
        lock.withLock { stops.append(stop) }
        return stop
    }

    func finish(owner: HTTPAdapterDispatch?, gates: [AdapterOwnershipGate]) async {
        for gate in gates { await gate.open() }
        let releases = lock.withLock { self.releases }
        releases.forEach { $0() }
        owner?.sealRequests()
        owner?.closeAcceptedEnrollment()
        owner?.cancelRequests()
        var originalIndex = 0
        var stopIndex = 0
        while true {
            let (originals, stops) = lock.withLock {
                (Array(self.originals.dropFirst(originalIndex)), Array(self.stops.dropFirst(stopIndex)))
            }
            if originals.isEmpty, stops.isEmpty { break }
            for task in originals { await task.value }
            for task in stops { _ = await task.result }
            originalIndex += originals.count
            stopIndex += stops.count
        }
        let finalizers = lock.withLock { self.finalizers }
        for finalize in finalizers { await finalize() }
        await owner?.join()
    }
}

func withOwnershipCleanup(
    owner: HTTPAdapterDispatch? = nil,
    gates: [AdapterOwnershipGate],
    _ body: (AdapterOwnershipCleanup) async throws -> Void
) async throws {
    let cleanup = AdapterOwnershipCleanup()
    do {
        try await body(cleanup)
    } catch {
        await cleanup.finish(owner: owner, gates: gates)
        throw error
    }
    await cleanup.finish(owner: owner, gates: gates)
}

actor AdapterOwnershipEvents {
    private var counts: [String: Int] = [:]
    func mark(_ event: String) { counts[event, default: 0] += 1 }
    func count(_ event: String) -> Int { counts[event, default: 0] }
}

private actor AdapterOwnershipFencePair {
    let first: AdapterOwnershipGate
    let final: AdapterOwnershipGate
    private var calls = 0
    init(first: AdapterOwnershipGate, final: AdapterOwnershipGate) {
        self.first = first
        self.final = final
    }
    func enter() async {
        calls += 1
        if calls == 1 { await first.enter() } else { await final.enter() }
    }
}

final class AdapterOwnershipConnection: SSEConnection, @unchecked Sendable {
    private let lock = NSLock()
    private var connected = true
    var isConnected: Bool { lock.withLock { connected } }
    func terminate() { lock.withLock { connected = false } }
}

final class AdapterOwnershipEngine: MCPHTTPEngine, @unchecked Sendable {
    let configuredHost = "127.0.0.1"
    let configuredPort = 0
    let response: EngineResponse
    let events: AdapterOwnershipEvents
    let handleGate: AdapterOwnershipGate?
    let registerGate: AdapterOwnershipGate?
    let disconnectGate: AdapterOwnershipGate?

    init(
        response: EngineResponse = EngineResponse(status: .ok, headerFields: [:], body: .buffered(nil)),
        events: AdapterOwnershipEvents = AdapterOwnershipEvents(),
        handle: AdapterOwnershipGate? = nil,
        register: AdapterOwnershipGate? = nil,
        disconnect: AdapterOwnershipGate? = nil
    ) {
        self.response = response
        self.events = events
        self.handleGate = handle
        self.registerGate = register
        self.disconnectGate = disconnect
    }

    func maxBodySize(for head: HTTPRequest) -> Int { 1_024 }
    func handle(head: HTTPRequest, bodyStream: AsyncStream<Data>) async -> EngineResponse {
        await events.mark("handle")
        await handleGate?.enter()
        if Task.isCancelled { await events.mark("cancelledHandle") }
        return response
    }

    func registerConnection(
        _ connection: any SSEConnection, for registration: SSERegistration
    ) async -> SSEConnectionToken? {
        await events.mark(connection.isConnected ? "liveRegister" : "deadRegister")
        await registerGate?.enter()
        return SSEConnectionToken(streamID: registration.streamID, connectionToken: UUID())
    }

    func connectionDisconnected(_ token: SSEConnectionToken) async {
        await events.mark("disconnectEntered")
        await disconnectGate?.enter()
        await events.mark("disconnected")
    }
}

func ownershipWriter(
    buffered: @escaping @Sendable (EngineResponse) async -> HTTPAdapterWriteResult = { _ in .written },
    head: @escaping @Sendable (EngineResponse) async -> HTTPAdapterWriteResult = { _ in .written },
    chunk: @escaping @Sendable (Data) async -> Bool = { _ in true },
    closed: AdapterOwnershipGate = AdapterOwnershipGate(),
    finish: @escaping @Sendable () async -> Void = {}
) -> HTTPAdapterResponseWriter {
    let connection = AdapterOwnershipConnection()
    return HTTPAdapterResponseWriter(
        buffered: buffered, sseHead: head, sseChunk: chunk, sseEnd: { true },
        connection: connection,
        closed: { await closed.enter() },
        close: { connection.terminate(); await closed.open() },
        finish: finish
    )
}

func startOwnershipRoute(
    _ owner: HTTPAdapterDispatch, engine: any MCPHTTPEngine, writer: HTTPAdapterResponseWriter
) -> HTTPAdapterDispatch.Operation? {
    owner.startRequest { parent in
        await HTTPAdapterRouteOperation.run(
            owner: owner, parent: parent, engine: engine,
            head: HTTPRequest(method: .get, scheme: nil, authority: nil, path: "/"),
            body: AsyncStream { $0.finish() }, writer: writer
        )
    }
}

func ownershipStop(
    _ owner: HTTPAdapterDispatch,
    ready: AdapterOwnershipGate,
    events: AdapterOwnershipEvents,
    close: @escaping @Sendable () async -> Void = {}
) -> Task<Void, Error> {
    owner.sealRequests()
    return Task {
        try await HTTPAdapterShutdownOperation.run(
            owner: owner, closeListener: {},
            closeChildren: { await close(); await ready.enter() }, afterDrain: {},
            shutdown: { await events.mark("shutdown") }
        )
    }
}

@Suite("HTTP adapter original-operation ownership", .timeLimit(.minutes(1)))
struct HTTPAdapterDispatchOwnershipTests {
    @Test("A failed buffered write still joins every later flushed write completion")
    func failedWriteStillJoinsRemainder() async throws {
        let owner = HTTPAdapterDispatch()
        let bodyWrite = AdapterOwnershipGate()
        let endWrite = AdapterOwnershipGate()
        let ready = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let engine = AdapterOwnershipEngine()
        try await withOwnershipCleanup(owner: owner, gates: [bodyWrite, endWrite, ready]) { cleanup in
            let original = cleanup.retain(startOwnershipRoute(owner, engine: engine, writer: ownershipWriter(
                buffered: { _ in
                    let succeeded = await HTTPAdapterWriteOperation.join([
                        { await events.mark("headFailed"); return false },
                        { await bodyWrite.enter(); await events.mark("bodyJoined"); return true },
                        { await endWrite.enter(); await events.mark("endJoined"); return true }
                    ])
                    return succeeded ? .written : .failed
                }
            )))
            try await bodyWrite.waitForArrival()
            #expect(await events.count("headFailed") == 1)
            let stop = cleanup.retain(ownershipStop(owner, ready: ready, events: events))
            try await ready.waitForArrival()
            await ready.open()
            #expect(await events.count("shutdown") == 0)
            await bodyWrite.open()
            try await endWrite.waitForArrival()
            #expect(await events.count("shutdown") == 0)
            await endWrite.open()
            await original?.task.value
            try await stop.value
            #expect(await events.count("bodyJoined") == 1)
            #expect(await events.count("endJoined") == 1)
            #expect(await events.count("shutdown") == 1)
        }
    }

    @Test("Stop joins engine return, response writer and final loop tail independently")
    func completeRouteJoin() async throws {
        let owner = HTTPAdapterDispatch()
        let engineGate = AdapterOwnershipGate()
        let writeGate = AdapterOwnershipGate()
        let tailGate = AdapterOwnershipGate()
        let stopGate = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let engine = AdapterOwnershipEngine(handle: engineGate)
        try await withOwnershipCleanup(owner: owner, gates: [engineGate, writeGate, tailGate, stopGate]) { cleanup in
        let original = cleanup.retain(startOwnershipRoute(owner, engine: engine, writer: ownershipWriter(
            buffered: { _ in await writeGate.enter(); return .written },
            finish: { await tailGate.enter() }
        )))
        try await engineGate.waitForArrival()
        let stop = cleanup.retain(ownershipStop(owner, ready: stopGate, events: events))
        try await stopGate.waitForArrival()
        #expect(owner.startRequest { _ in await events.mark("lateRoute") } == nil)
        await stopGate.open()
        #expect(await events.count("shutdown") == 0)
        await engineGate.open()
        try await writeGate.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await writeGate.open()
        try await tailGate.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await tailGate.open()
        await original?.task.value
        try await stop.value
        #expect(await events.count("shutdown") == 1)
        #expect(await events.count("lateRoute") == 0)
        }
    }

    @Test("A bound SSE close reconciles its observer without cancelling the route or sealing admission")
    func resumableSocketClose() async throws {
        let owner = HTTPAdapterDispatch()
        let chunk = AdapterOwnershipGate()
        let disconnect = AdapterOwnershipGate()
        let closed = AdapterOwnershipGate()
        let ready = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let engine = AdapterOwnershipEngine(response: EngineResponse(
            status: .ok, headerFields: [:], body: .sse(
                stream: AsyncStream { $0.yield(Data([1])); $0.finish() },
                registration: SSERegistration(sessionID: UUID(), streamID: UUID())
            )
        ), events: events, disconnect: disconnect)
        let writer = ownershipWriter(
            chunk: { _ in await chunk.enter(); return true }, closed: closed,
            finish: { if Task.isCancelled { await events.mark("cancelledRoute") } }
        )
        try await withOwnershipCleanup(owner: owner, gates: [chunk, disconnect, closed, ready]) { cleanup in
        let original = cleanup.retain(startOwnershipRoute(owner, engine: engine, writer: writer))
        try await chunk.waitForArrival()
        writer.connection.terminate()
        await closed.open()
        try await disconnect.waitForArrival() // The actual owned observer ran.
        #expect(!owner.requestsAreSealed)
        let second = cleanup.retain(owner.startRequest { _ in })
        #expect(second != nil)
        await disconnect.open()
        await chunk.open()
        await original?.task.value
        await second?.task.value
        #expect(await events.count("cancelledRoute") == 0)
        #expect(await events.count("liveRegister") == 1)
        let stop = cleanup.retain(ownershipStop(owner, ready: ready, events: AdapterOwnershipEvents()))
        try await ready.waitForArrival()
        await ready.open()
        try await stop.value
        }
    }

    @Test("SSE registration racing stop retains the original disconnect descendant")
    func lateDisconnectObserver() async throws {
        let owner = HTTPAdapterDispatch()
        let registration = AdapterOwnershipGate()
        let disconnect = AdapterOwnershipGate()
        let closed = AdapterOwnershipGate()
        let ready = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let stream = AsyncStream<Data> { $0.finish() }
        let engine = AdapterOwnershipEngine(
            response: EngineResponse(status: .ok, headerFields: [:], body: .sse(
                stream: stream, registration: SSERegistration(sessionID: UUID(), streamID: UUID())
            )), events: events, register: registration, disconnect: disconnect
        )
        try await withOwnershipCleanup(owner: owner, gates: [registration, disconnect, closed, ready]) { cleanup in
        let original = cleanup.retain(startOwnershipRoute(owner, engine: engine, writer: ownershipWriter(closed: closed)))
        try await registration.waitForArrival()
        let stop = cleanup.retain(ownershipStop(owner, ready: ready, events: events, close: { await closed.open() }))
        try await ready.waitForArrival()
        await ready.open()
        await registration.open()
        try await disconnect.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await disconnect.open()
        await original?.task.value
        try await stop.value
        #expect(await events.count("liveRegister") == 1)
        #expect(await events.count("deadRegister") == 0)
        #expect(await events.count("disconnected") == 1)
        #expect(await events.count("shutdown") == 1)
        }
    }

    @Test("Failed SSE head reconciles once and still awaits the finalizer")
    func failedHeadDisposition() async throws {
        let owner = HTTPAdapterDispatch()
        let tail = AdapterOwnershipGate()
        let ready = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        let engine = AdapterOwnershipEngine(response: EngineResponse(
            status: .ok, headerFields: [:], body: .sse(
                stream: AsyncStream { $0.finish() },
                registration: SSERegistration(sessionID: UUID(), streamID: UUID())
            )
        ), events: events)
        try await withOwnershipCleanup(owner: owner, gates: [tail, ready]) { cleanup in
        let original = cleanup.retain(startOwnershipRoute(owner, engine: engine, writer: ownershipWriter(
            head: { _ in .failed }, finish: { await tail.enter() }
        )))
        try await tail.waitForArrival()
        #expect(await events.count("deadRegister") == 1)
        #expect(await events.count("disconnected") == 1)
        let stop = cleanup.retain(ownershipStop(owner, ready: ready, events: events))
        try await ready.waitForArrival()
        await ready.open()
        #expect(await events.count("shutdown") == 0)
        await tail.open()
        await original?.task.value
        try await stop.value
        }
    }

    @Test("A body-ended parent's escaped capability cannot append cleanup")
    func descendantAdmissionEndsWithBody() async {
        let owner = HTTPAdapterDispatch()
        let original = owner.startRequest { _ in }
        await original?.task.value
        if let original {
            #expect(owner.startCleanup(from: original.capability) { _ in } == nil)
        }
        owner.sealRequests()
        owner.closeAcceptedEnrollment()
        await owner.join()
        #expect(owner.startRequest { _ in } == nil)
    }

    @Test("Accepted setup held before initializer remains owned through every fixed tail")
    func acceptedBeforeInitializer() async throws {
        let owner = HTTPAdapterDispatch()
        let setup = AdapterOwnershipGate()
        let child = AdapterOwnershipGate()
        let parent = AdapterOwnershipGate()
        let finalChild = AdapterOwnershipGate()
        let finalParent = AdapterOwnershipGate()
        let childFences = AdapterOwnershipFencePair(first: child, final: finalChild)
        let parentFences = AdapterOwnershipFencePair(first: parent, final: finalParent)
        let closed = AdapterOwnershipGate()
        let ready = AdapterOwnershipGate()
        let events = AdapterOwnershipEvents()
        try await withOwnershipCleanup(owner: owner, gates: [setup, child, parent, closed, finalChild, finalParent, ready]) { cleanup in
        let original = cleanup.retain(owner.startAccepted(onJoined: {}) { _ in
            await HTTPAdapterAcceptedOperation.run(
                setup: { await setup.enter() }, childFence: { await childFences.enter() },
                parentFence: { await parentFences.enter() }, closed: { await closed.enter() }
            )
        })
        try await setup.waitForArrival()
        let stop = cleanup.retain(ownershipStop(owner, ready: ready, events: events))
        try await ready.waitForArrival()
        await ready.open()
        #expect(await events.count("shutdown") == 0)
        await setup.open()
        try await child.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await child.open()
        try await parent.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await parent.open()
        try await closed.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await closed.open()
        try await finalChild.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await finalChild.open()
        try await finalParent.waitForArrival()
        #expect(await events.count("shutdown") == 0)
        await finalParent.open()
        await original?.task.value
        try await stop.value
        #expect(await events.count("shutdown") == 1)
        }
    }
}
#endif
