#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

private enum OwnershipFixtureError: Error { case bind, disposal }

final class OwnershipBox<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value
    init(_ value: Value) { self.value = value }
    func read() -> Value { lock.withLock { value } }
    func update(_ body: (inout Value) -> Void) { lock.withLock { body(&value) } }
}

/// Uses the actual production shutdown operation, without allocating NIO loops.
final class OwnershipLifecycleAdapter: HTTPTransportAdapter, @unchecked Sendable {
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

func startLifecycle(
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

final class OwnershipHeldDispatcher: MCPDispatcher, Sendable {
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

#endif
