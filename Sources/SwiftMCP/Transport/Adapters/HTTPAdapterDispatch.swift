#if Server
import Foundation
import HTTPTypes

enum HTTPAdapterLifecycleError: Error, Equatable {
    /// An operation cannot join the original task which is currently calling it.
    case ownedOperationCannotStop
}

/// Owns original tasks, rather than interpreting cancellation or body completion
/// as a join. A separate retained reaper joins each original before removal.
/// Socket closure does not seal/cancel requests; explicit transport stop does.
final class HTTPAdapterDispatch: @unchecked Sendable {
    struct Capability: Sendable {
        fileprivate let owner: UUID
        fileprivate let record: UUID
    }

    struct Operation: Sendable {
        let capability: Capability
        let task: Task<Void, Never>
    }

    private enum Context {
        @TaskLocal static var capability: Capability?
    }

    private final class State: @unchecked Sendable {
        struct Record {
            let task: Task<Void, Never>
            let isRequest: Bool
            let onJoined: @Sendable () -> Void
            var bodyOpen = true
        }

        let id = UUID()
        let lock = NSLock()
        let completions: AsyncStream<UUID>.Continuation
        var records: [UUID: Record] = [:]
        var requestsSealed = false
        var acceptedEnrollmentOpen = true
        var joinedWaiters: [CheckedContinuation<Void, Never>] = []

        init(completions: AsyncStream<UUID>.Continuation) {
            self.completions = completions
        }

        func finishBody(_ id: UUID) {
            lock.withLock {
                records[id]?.bodyOpen = false
                completions.yield(id)
            }
        }

        /// Called only after the reaper awaited the exact original handle.
        func removeJoined(_ id: UUID) {
            let callback = lock.withLock { records[id]?.onJoined }
            callback?()
            let waiters: [CheckedContinuation<Void, Never>] = lock.withLock {
                records.removeValue(forKey: id)
                return finishQueueIfJoined()
            }
            waiters.forEach { $0.resume() }
        }

        // Requires lock. External setup admission must close before emptiness
        // can be terminal; active parents are themselves counted records.
        func finishQueueIfJoined() -> [CheckedContinuation<Void, Never>] {
            guard requestsSealed, !acceptedEnrollmentOpen, records.isEmpty else { return [] }
            completions.finish()
            let waiters = joinedWaiters
            joinedWaiters.removeAll()
            return waiters
        }
    }

    private enum Admission {
        case request
        case accepted
        case child(Capability)
    }

    private let state: State
    private let reaper: Task<Void, Never>

    init() {
        let queue = AsyncStream<UUID>.makeStream()
        let state = State(completions: queue.continuation)
        self.state = state
        // State owns no reaper handle, so there is no idle owner/reaper cycle.
        self.reaper = Task.detached {
            for await id in queue.stream {
                let original = state.lock.withLock { state.records[id]?.task }
                await original?.value
                state.removeJoined(id)
            }
        }
    }

    var requestsAreSealed: Bool { state.lock.withLock { state.requestsSealed } }

    /// Task-local values may escape into inherited descendants. Only an active
    /// body in this exact owner is a self-join hazard; stale markers are ignored.
    var currentTaskIsOwned: Bool {
        guard let marker = Context.capability, marker.owner == state.id else { return false }
        return state.lock.withLock { state.records[marker.record]?.bodyOpen == true }
    }

    func requireExternalCaller() throws {
        if currentTaskIsOwned { throw HTTPAdapterLifecycleError.ownedOperationCannotStop }
    }

    func sealRequests() {
        state.lock.withLock { state.requestsSealed = true }
    }

    @discardableResult
    func startRequest(
        _ operation: @escaping @Sendable (Capability) async -> Void
    ) -> Operation? {
        start(.request, operation: operation)
    }

    @discardableResult
    func startCleanup(
        from parent: Capability,
        _ operation: @escaping @Sendable (Capability) async -> Void
    ) -> Operation? {
        start(.child(parent), operation: operation)
    }

    @discardableResult
    func startAccepted(
        onJoined: @escaping @Sendable () -> Void,
        _ operation: @escaping @Sendable (Capability) async -> Void
    ) -> Operation? {
        start(.accepted, onJoined: onJoined, operation: operation)
    }

    private func start(
        _ admission: Admission,
        onJoined: @escaping @Sendable () -> Void = {},
        operation: @escaping @Sendable (Capability) async -> Void
    ) -> Operation? {
        state.lock.withLock {
            switch admission {
            case .request:
                guard !state.requestsSealed else { return nil }
            case .accepted:
                guard state.acceptedEnrollmentOpen else { return nil }
            case .child(let parent):
                guard parent.owner == state.id,
                      state.records[parent.record]?.bodyOpen == true else { return nil }
            }
            let capability = Capability(owner: state.id, record: UUID())
            let state = self.state
            let task = Task.detached {
                // The creating call still holds this lock until the exact
                // original handle is published. The body cannot outrun it.
                state.lock.withLock {}
                await Context.$capability.withValue(capability) {
                    defer { state.finishBody(capability.record) }
                    await operation(capability)
                }
            }
            let isRequest: Bool
            if case .request = admission { isRequest = true } else { isRequest = false }
            state.records[capability.record] = State.Record(
                task: task, isRequest: isRequest, onJoined: onJoined
            )
            return Operation(capability: capability, task: task)
        }
    }

    /// Call only after original listener close + its acceptance-loop fence.
    func closeAcceptedEnrollment() {
        let waiters = state.lock.withLock {
            state.requestsSealed = true
            state.acceptedEnrollmentOpen = false
            return state.finishQueueIfJoined()
        }
        waiters.forEach { $0.resume() }
    }

    func cancelRequests() {
        let originals = state.lock.withLock {
            state.records.values.filter(\.isRequest).map(\.task)
        }
        originals.forEach { $0.cancel() }
    }

    func join() async {
        await withCheckedContinuation { continuation in
            let alreadyJoined = state.lock.withLock {
                if state.requestsSealed, !state.acceptedEnrollmentOpen, state.records.isEmpty { return true }
                state.joinedWaiters.append(continuation)
                return false
            }
            if alreadyJoined { continuation.resume() }
        }
        await reaper.value
    }
}

enum HTTPAdapterWriteResult: Sendable {
    case written
    case discarded
    case failed
}

/// All write attempts must already be enqueued/flushed. Join every original
/// completion even if an earlier write failed; delivery is never inferred.
enum HTTPAdapterWriteOperation {
    static func join(_ completions: [@Sendable () async -> Bool]) async -> Bool {
        var succeeded = true
        for completion in completions {
            let result = await completion()
            if !result { succeeded = false }
        }
        return succeeded
    }
}

/// Concrete NIO writes and fences plug into this actual production operation.
/// A discarded response has not claimed the generation's response transaction.
struct HTTPAdapterResponseWriter: Sendable {
    let buffered: @Sendable (EngineResponse) async -> HTTPAdapterWriteResult
    let sseHead: @Sendable (EngineResponse) async -> HTTPAdapterWriteResult
    let sseChunk: @Sendable (Data) async -> Bool
    let sseEnd: @Sendable () async -> Bool
    let connection: any SSEConnection
    let closed: @Sendable () async -> Void
    let close: @Sendable () async -> Void
    let finish: @Sendable () async -> Void
}

enum HTTPAdapterRouteOperation {
    static func run(
        owner: HTTPAdapterDispatch,
        parent: HTTPAdapterDispatch.Capability,
        engine: any MCPHTTPEngine,
        head: HTTPRequest,
        body: AsyncStream<Data>,
        writer: HTTPAdapterResponseWriter
    ) async {
        let response = await engine.handle(head: head, bodyStream: body)
        switch response.body {
        case .buffered:
            if case .failed = await writer.buffered(response) { await writer.close() }

        case .sse(let stream, let registration):
            let disposition = await writer.sseHead(response)
            if case .written = disposition {
                // Once live registration begins, await that same original call.
                // Cancellation does not authorize a concurrent dead rebind.
                if let registration,
                   let token = await engine.registerConnection(writer.connection, for: registration) {
                    let observer = owner.startCleanup(from: parent) { _ in
                        await writer.closed()
                        await engine.connectionDisconnected(token)
                    }
                    precondition(observer != nil, "live route lost descendant admission")
                }
                var writable = !Task.isCancelled
                if writable {
                    for await chunk in stream {
                        if !(await writer.sseChunk(chunk)) { writable = false; break }
                    }
                }
                if writable, !Task.isCancelled {
                    if !(await writer.sseEnd()) { await writer.close() }
                } else {
                    await writer.close()
                }
            } else {
                await reconcileDiscarded(response, engine: engine)
                if case .failed = disposition { await writer.close() }
            }
        }
        // This is awaited on every response disposition, including cancellation.
        await writer.finish()
    }

    static func reconcileDiscarded(_ response: EngineResponse, engine: any MCPHTTPEngine) async {
        guard case .sse(_, let registration) = response.body, let registration else { return }
        if let token = await engine.registerConnection(DeadSSEConnection(), for: registration) {
            await engine.connectionDisconnected(token)
        }
    }
}

/// Shared by NIO and deterministic tests. Close operations settle their own
/// errors without skipping original lifetime joins. Group-disposal failure is
/// returned only after every adapter task and the reaper have joined.
enum HTTPAdapterShutdownOperation {
    static func run(
        owner: HTTPAdapterDispatch,
        closeListener: @Sendable () async -> Void,
        closeChildren: @Sendable () async -> Void,
        afterDrain: @Sendable () async -> Void,
        shutdown: @Sendable () async throws -> Void
    ) async throws {
        owner.sealRequests()
        await closeListener()
        owner.closeAcceptedEnrollment()
        await closeChildren()
        owner.cancelRequests()
        await owner.join()
        await afterDrain()
        try await shutdown()
    }
}

/// The actual fixed setup/close tail used by each NIO accepted ticket. Each
/// closure settles a real original future even on failure; none throws early.
enum HTTPAdapterAcceptedOperation {
    static func run(
        setup: @Sendable () async -> Void,
        childFence: @Sendable () async -> Void,
        parentFence: @Sendable () async -> Void,
        closed: @Sendable () async -> Void
    ) async {
        await setup()
        await childFence()
        await parentFence()
        await closed()
        await childFence()
        await parentFence()
    }
}
#endif
