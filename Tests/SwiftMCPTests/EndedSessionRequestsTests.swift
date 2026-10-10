#if Server
import Foundation
import Logging
@testable import SwiftMCP
import Testing

/// Actor-only lifecycle regressions: no macro server, URLSession, socket, listener or device.
@Suite("Ended outbound session requests", .serialized, .timeLimit(.minutes(1)))
struct EndedSessionRequestsTests {
    @Test
    func `Live outbound requests still receive the matching in-memory reply`() async throws {
        let session = Session(id: UUID())
        let transport = RespondingRequestTransport()
        await session.setTransport(transport)
        let response = try await session.send(.request(id: 71, method: "roots/list"))
        #expect(response.id == .integer(71))
        #expect(await transport.sentSessions == [session.id])
        #expect(await session.pendingOutboundCount == 0)
    }

    @Test
    func `Common removal refuses new raw send registration after the end event`() async throws {
        let manager = SessionManager()
        let session = await manager.session(id: UUID())
        let transport = RespondingRequestTransport()
        await session.setTransport(transport)
        let end = await session.ended()
        await manager.removeSession(id: session.id)
        #expect(await endedEventCount(end) == 1)
        #expect(await isCancelled { try await session.send(.request(id: 72, method: "roots/list")) })
        #expect(await transport.sentSessions.isEmpty)
        #expect(await session.pendingOutboundCount == 0)
    }

    @Test
    func `Explicit disconnect rejects request convenience calls and leaves a second session live`() async throws {
        let manager = SessionManager()
        let ended = await manager.session(id: UUID())
        let live = await manager.session(id: UUID())
        let transport = RespondingRequestTransport()
        await ended.setTransport(transport)
        await live.setTransport(transport)
        await manager.disconnectSession(ended)
        #expect(await isCancelled { try await ended.request(method: "sampling/createMessage") })
        let response = try await live.request(method: "roots/list")
        #expect(response.id != nil)
        #expect(await transport.sentSessions == [live.id])
        #expect(await ended.pendingOutboundCount == 0)
        #expect(await live.pendingOutboundCount == 0)
        await manager.removeSession(id: live.id)
    }

    @Test
    func `Expiry refuses a retained session's new outbound request`() async throws {
        let manager = SessionManager()
        let session = await manager.session(id: UUID())
        let transport = RespondingRequestTransport()
        await session.setTransport(transport)
        await session.setExpiresAt(.distantPast)
        #expect(await manager.existingSession(id: session.id) == nil)
        #expect(await isCancelled { try await session.request(method: "elicitation/create") })
        #expect(await transport.sentSessions.isEmpty)
        #expect(await session.pendingOutboundCount == 0)
    }

    @Test
    func `The nonisolated removal mark closes outbound admission before its end-event actor hop`() async throws {
        let session = Session(id: UUID())
        let transport = RespondingRequestTransport()
        await session.setTransport(transport)
        let end = await session.ended()
        session.disconnection.mark()
        #expect(await isCancelled { try await session.send(.request(id: 73, method: "roots/list")) })
        #expect(await transport.sentSessions.isEmpty)
        #expect(await session.pendingOutboundCount == 0)
        await session.disconnectRequests()
        #expect(await endedEventCount(end) == 1)
    }

    @Test
    func `An already parked waiter is drained and a later request cannot escape that final drain`() async throws {
        let manager = SessionManager()
        let session = await manager.session(id: UUID())
        let transport = RespondingRequestTransport(parkReplies: true)
        await session.setTransport(transport)
        let parked = Task { await isCancelled { try await session.send(.request(id: 74, method: "roots/list")) } }
        await transport.waitForFirstSend()
        #expect(await session.pendingOutboundCount == 1)
        await manager.removeSession(id: session.id)
        #expect(await parked.value)
        #expect(await session.pendingOutboundCount == 0)
        // The old implementation now replies incorrectly rather than hanging the regression.
        await transport.respondImmediately()
        #expect(await isCancelled { try await session.send(.request(id: 75, method: "roots/list")) })
        #expect(await transport.sentSessions == [session.id])
        #expect(await session.pendingOutboundCount == 0)
    }

    #if !EndedRequestsBaseline
    @Test
    func `Deferred send released after removal is refused before transport invocation`() async throws {
        let manager = SessionManager()
        let session = await manager.session(id: UUID())
        let transport = RespondingRequestTransport()
        await session.setTransport(transport)
        let gate = DeferredRequestGate()
        // Schedule the actual deferred stage behind a deterministic host-only latch.
        // This neither duplicates that stage nor relies on a task scheduling delay.
        let deferred = Task {
            await gate.pause()
            do {
                try await session.sendRegisteredRequest(.request(id: 76, method: "roots/list"))
                return false
            } catch is CancellationError {
                return true
            } catch {
                return false
            }
        }
        await gate.waitUntilPaused()
        await manager.removeSession(id: session.id)
        await gate.release()
        #expect(await deferred.value)
        #expect(await transport.sentSessions.isEmpty)
        #expect(await session.pendingOutboundCount == 0)
    }
    #endif
}

private func isCancelled(_ action: @Sendable () async throws -> JSONRPCMessage) async -> Bool {
    do { _ = try await action(); return false }
    catch is CancellationError { return true }
    catch { return false }
}

private func endedEventCount(_ events: AsyncStream<Void>) async -> Int {
    var count = 0
    for await _ in events { count += 1 }
    return count
}

extension Session {
    fileprivate var pendingOutboundCount: Int { responses.count }
}

/// It immediately answers a real correlated request in Session.current, so a missing
/// post-end admission guard deterministically succeeds incorrectly instead of waiting forever.
private actor RespondingRequestTransport: Transport {
    nonisolated let logger = Logger(label: "test.ended-outbound-requests")
    private var parkReplies: Bool
    private var sendWaiters: [CheckedContinuation<Void, Never>] = []
    private(set) var sentSessions: [UUID] = []

    init(parkReplies: Bool = false) { self.parkReplies = parkReplies }
    func start() async throws {}
    func run() async throws {}
    func stop() async throws {}

    func send(_ data: Data) async throws {
        let message = try JSONDecoder().decode(JSONRPCMessage.self, from: data)
        let session = try #require(Session.current)
        let id = try #require(message.id)
        let sessionID = await session.id
        sentSessions.append(sessionID)
        let waiters = sendWaiters
        sendWaiters = []
        for waiter in waiters { waiter.resume() }
        if !parkReplies { await session.handleResponse(.response(id: id, result: ["ok": .bool(true)])) }
    }

    func waitForFirstSend() async {
        if !sentSessions.isEmpty { return }
        await withCheckedContinuation { sendWaiters.append($0) }
    }

    func respondImmediately() { parkReplies = false }
}

private actor DeferredRequestGate {
    private var paused = false
    private var released = false
    private var releaseWaiter: CheckedContinuation<Void, Never>?
    private var pauseWaiters: [CheckedContinuation<Void, Never>] = []

    func pause() async {
        paused = true
        let waiters = pauseWaiters
        pauseWaiters = []
        for waiter in waiters { waiter.resume() }
        if !released { await withCheckedContinuation { releaseWaiter = $0 } }
    }

    func waitUntilPaused() async {
        if paused { return }
        await withCheckedContinuation { pauseWaiters.append($0) }
    }

    func release() {
        released = true
        releaseWaiter?.resume()
        releaseWaiter = nil
    }
}
#endif
