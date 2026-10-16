#if Server
import Foundation
import HTTPTypes
import Testing
@testable import SwiftMCP

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

}
#endif
