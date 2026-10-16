#if Server
import Foundation

protocol HTTPTransportAdapter: AnyObject, Sendable {
    var currentTaskIsOwned: Bool { get }
    func sealRequests()
    func start() async throws -> Int
    func waitUntilClosed() async throws
    func shutdown(afterDrain: @escaping @Sendable () async -> Void) async throws
}

/// Serializes real transport generations, including a suspended bind. Completed
/// stop handles are retained until a caller has awaited their original result.
/// A failed disposal is retained and prevents an overlapping restart.
final class HTTPTransportLifecycle: @unchecked Sendable {
    private final class Generation: @unchecked Sendable {
        let adapter: (any HTTPTransportAdapter)?
        let beforeDrain: @Sendable () async -> Void
        let afterDrain: @Sendable () async -> Void
        var startTask: Task<Int, Error>!
        var stopTask: Task<Void, Error>?

        init(
            adapter: (any HTTPTransportAdapter)?,
            beforeDrain: @escaping @Sendable () async -> Void,
            afterDrain: @escaping @Sendable () async -> Void
        ) {
            self.adapter = adapter
            self.beforeDrain = beforeDrain
            self.afterDrain = afterDrain
        }
    }

    private enum StartAction {
        case start(Generation)
        case joinStop(Generation, Task<Void, Error>)
    }

    private let lock = NSLock()
    private var generation: Generation?

    func start(
        prepare: @escaping @Sendable () -> Void,
        makeAdapter: @escaping @Sendable () -> any HTTPTransportAdapter,
        didStart: @escaping @Sendable (Int) -> Void,
        beforeDrain: @escaping @Sendable () async -> Void,
        afterDrain: @escaping @Sendable () async -> Void
    ) async throws {
        while true {
            let action: StartAction = try lock.withLock {
                if let current = generation {
                    try requireExternalCaller(current)
                    if let stop = current.stopTask { return .joinStop(current, stop) }
                    return .start(current)
                }
                // Configuration/lazy engine publication occurs once, before
                // any adapter task can accept a request.
                prepare()
                let current = Generation(
                    adapter: makeAdapter(), beforeDrain: beforeDrain, afterDrain: afterDrain
                )
                generation = current
                current.startTask = Task.detached {
                    self.lock.withLock {}
                    let port = try await current.adapter!.start()
                    self.lock.withLock {
                        // Serialize timer/port publication with the stop seal.
                        if self.generation === current, current.stopTask == nil {
                            didStart(port)
                        }
                    }
                    return port
                }
                return .start(current)
            }
            switch action {
            case .start(let current):
                do {
                    _ = try await current.startTask.value
                    return
                } catch {
                    // The original bind has returned. Cleanup is a different
                    // retained operation, also shared with racing stop callers.
                    let stop = try lock.withLock { try stopOperation(current) }
                    try await stop.value
                    retireJoined(current)
                    throw error
                }
            case .joinStop(let current, let stop):
                try await stop.value
                retireJoined(current)
                // Only a joined successful stop permits another generation.
            }
        }
    }

    func stop(
        beforeDrain: @escaping @Sendable () async -> Void,
        afterDrain: @escaping @Sendable () async -> Void,
        didReserve: @Sendable () -> Void = {}
    ) async throws {
        // This lock/seal runs before this call's first suspension.
        let (current, stop): (Generation, Task<Void, Error>) = try lock.withLock {
            let current: Generation
            if let existing = generation {
                current = existing
            } else {
                // In-memory use can create sessions without a bound adapter.
                current = Generation(adapter: nil, beforeDrain: beforeDrain, afterDrain: afterDrain)
                current.startTask = Task.detached { 0 }
                generation = current
            }
            return (current, try stopOperation(current))
        }
        // Observable internal seam for deterministic concurrent-caller tests.
        // This original stop handle is retained before the first suspension.
        didReserve()
        try await stop.value
        // Every racing caller already retains this same original stop handle.
        // Retire only after its exact successful join, releasing the engine and
        // callback payloads and allowing later idle stops to sweep new sessions.
        retireJoined(current)
    }

    private func retireJoined(_ current: Generation) {
        lock.withLock {
            if generation === current { generation = nil }
        }
    }

    // Requires lifecycle lock. No stop-task state is cleared by its own body.
    private func stopOperation(_ current: Generation) throws -> Task<Void, Error> {
        try requireExternalCaller(current)
        if let stop = current.stopTask { return stop }
        current.adapter?.sealRequests()
        let stop = Task.detached {
            self.lock.withLock {}
            await current.beforeDrain()
            // Includes failed binds. The adapter still owns its allocated group.
            _ = await current.startTask.result
            if let adapter = current.adapter {
                try await adapter.shutdown(afterDrain: current.afterDrain)
            } else {
                await current.afterDrain()
            }
        }
        current.stopTask = stop
        return stop
    }

    private func requireExternalCaller(_ current: Generation) throws {
        if current.adapter?.currentTaskIsOwned == true {
            throw HTTPAdapterLifecycleError.ownedOperationCannotStop
        }
    }

    func waitUntilClosed() async throws {
        let current = lock.withLock { generation }
        guard let current else { return }
        _ = try await current.startTask.value
        try await current.adapter?.waitUntilClosed()
    }
}
#endif
