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
            request: (head: HTTPRequest(method: .get, scheme: nil, authority: nil, path: "/"),
                      body: AsyncStream { $0.finish() }), writer: writer
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

#endif
