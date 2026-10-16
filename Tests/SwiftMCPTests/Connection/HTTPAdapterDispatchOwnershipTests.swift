#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

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
        let original = cleanup.retain(startOwnershipRoute(
            owner, engine: engine, writer: ownershipWriter(closed: closed)
        ))
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
        try await withOwnershipCleanup(
            owner: owner, gates: [setup, child, parent, closed, finalChild, finalParent, ready]
        ) { cleanup in
        let original = cleanup.retain(owner.startAccepted(onJoined: {}, { _ in
            await HTTPAdapterAcceptedOperation.run(
                setup: { await setup.enter() }, childFence: { await childFences.enter() },
                parentFence: { await parentFences.enter() }, closed: { await closed.enter() }
            )
        }))
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
