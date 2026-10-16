//
//  NIOHTTPServerAdapter.swift
//  SwiftMCP
//
//  The swift-nio adapter behind the ``MCPHTTPEngine`` seam: the listening socket,
//  the HTTP/1 pipeline, the per-connection read/write loops, and the `Channel`
//  liveness/close wiring. This is the *only* file in the HTTP/SSE server that
//  links the swift-nio server stack — the engine (`HTTPSSETransport`, routing,
//  sessions, SSE) is NIO-free. Swapping in Network.framework means writing a
//  sibling of this file, not touching the engine.
//

#if Server
import Foundation
import Logging
@preconcurrency import NIOCore
import NIOFoundationCompat
import NIOHTTP1
import NIOHTTPTypes
import NIOHTTPTypesHTTP1
import NIOPosix
import HTTPTypes

/// Owns the listening socket and event-loop group, and drives inbound requests
/// into the ``MCPHTTPEngine``.
final class NIOHTTPServerAdapter: HTTPTransportAdapter, @unchecked Sendable {
    private let engine: any MCPHTTPEngine
    private let logger: Logger
    private let group: EventLoopGroup
    private let lock = NSLock()
    private var channel: Channel?
    private var children: [ObjectIdentifier: NIOAcceptedChannelTicket] = [:]
    private var stopTask: Task<Void, Error>?
    let dispatch = HTTPAdapterDispatch()

    var currentTaskIsOwned: Bool { dispatch.currentTaskIsOwned }
    func sealRequests() { dispatch.sealRequests() }

    init(engine: any MCPHTTPEngine, logger: Logger) {
        self.engine = engine
        self.logger = logger
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    }

    /// Bind the listener and return the actually-bound port (resolving an
    /// ephemeral `0`). The caller (engine) records it as its public `port`.
    func start() async throws -> Int {
        let engine = self.engine
        let logger = self.logger
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .serverChannelInitializer { listener in
                self.lock.withLock { self.channel = listener }
                return listener.pipeline.addHandler(NIOAcceptedChannelCollector(adapter: self))
            }
            .childChannelInitializer { channel in
                let ticket = self.lock.withLock { self.children[ObjectIdentifier(channel)]! }
                // Leave Bootstrap's internal child options EMPTY: otherwise an
                // option failure can skip this initializer and strand a ticket.
                let setup: EventLoopFuture<Void>
                if self.dispatch.requestsAreSealed {
                    setup = channel.eventLoop.makeSucceededFuture(())
                } else {
                    setup = channel.setOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1).flatMap {
                        channel.setOption(ChannelOptions.maxMessagesPerRead, value: 16)
                    }.flatMap {
                        channel.setOption(ChannelOptions.allowRemoteHalfClosure, value: true)
                    }.flatMap {
                        channel.pipeline.configureHTTPServerPipeline()
                    }.flatMap {
                    channel.pipeline.addHandler(HTTPLogger())
                }.flatMap {
                    // Translate NIO's HTTP/1 request/response parts to and from
                    // swift-http-types so the channel handler works in
                    // HTTPRequest / HTTPResponse / HTTPFields directly.
                    channel.pipeline.addHandler(HTTP1ToHTTPServerCodec(secure: false))
                }.flatMap {
                    channel.pipeline.addHandler(NIOHTTPChannelHandler(
                        engine: engine, logger: logger, owner: self.dispatch
                    ))
                    }
                }
                setup.cascade(to: ticket.setup)
                return setup
            }

        do {
            let channel = try await bootstrap.bind(host: engine.configuredHost, port: engine.configuredPort).get()
            self.lock.withLock { self.channel = channel }
            let boundPort = channel.localAddress?.port ?? engine.configuredPort
            logger.info("Server started and listening on \(engine.configuredHost):\(boundPort)")

            channel.closeFuture.whenComplete { [logger] result in
                switch result {
                case .success:
                    logger.info("Server channel closed normally")
                case .failure(let error):
                    logger.error("Server channel closed with error: \(error)")
                }
            }
            return boundPort
        } catch {
            // The lifecycle owner joins this original start task, then joins
            // the allocated group's shared shutdown even when bind failed.
            switch error {
            case let ioError as IOError:
                throw bindingError(for: ioError, host: engine.configuredHost, port: engine.configuredPort)
            default:
                logger.error("Server error: \(error)")
                throw TransportError.bindingFailed(error.localizedDescription)
            }
        }
    }

    /// Suspends until the listening channel closes.
    func waitUntilClosed() async throws {
        let original = lock.withLock { channel }
        try await original?.closeFuture.get()
    }

    /// All callers join the same original stop operation. The callback is fixed
    /// by the transport generation before it can call this method.
    func shutdown(afterDrain: @escaping @Sendable () async -> Void) async throws {
        try dispatch.requireExternalCaller()
        let original: Task<Void, Error> = lock.withLock {
            if let stopTask { return stopTask }
            dispatch.sealRequests()
            let task = Task.detached {
                self.lock.withLock {}
                try await HTTPAdapterShutdownOperation.run(
                    owner: self.dispatch,
                    closeListener: {
                        let listener = self.lock.withLock { self.channel }
                        if let listener {
                            await nioCloseAndJoin(listener)
                            _ = try? await listener.eventLoop.submit {}.get()
                        }
                    },
                    closeChildren: {
                        let originals = self.lock.withLock { Array(self.children.values) }
                        // Start every close before awaiting routes/writes. Each
                        // ticket independently joins its original closeFuture.
                        for ticket in originals { ticket.channel.close(promise: nil) }
                    },
                    afterDrain: afterDrain,
                    shutdown: { try await self.shutdownGroup() }
                )
            }
            stopTask = task
            return task
        }
        try await original.value
    }

    private func shutdownGroup() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            group.shutdownGracefully { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// Invoked synchronously on the listener loop before NIO queues child setup.
    fileprivate func accepted(_ child: Channel, on listener: Channel) -> Bool {
        let key = ObjectIdentifier(child)
        let ticket = NIOAcceptedChannelTicket(channel: child, listener: listener)
        lock.withLock { children[key] = ticket }
        let original = dispatch.startAccepted(onJoined: {
            self.lock.withLock { _ = self.children.removeValue(forKey: key) }
        }) { _ in await ticket.join() }
        // Listener close + fence bounds collector calls before enrollment ends.
        precondition(original != nil, "accepted channel after listener acceptance fence")
        if dispatch.requestsAreSealed {
            // Never forwarded: explicitly settle the no-initializer branch.
            ticket.setup.succeed(())
            child.close(promise: nil)
            return false
        }
        return true
    }

    private func bindingError(for error: IOError, host: String, port: Int) -> TransportError {
        let errorMessage: String
        switch error.errnoCode {
        case EADDRINUSE:
            errorMessage = "Port \(port) is already in use. "
                + "Please choose a different port or ensure no other service is using this port."
        case EACCES:
            errorMessage = "Permission denied to bind to port \(port). This port may require elevated privileges."
        case EADDRNOTAVAIL:
            errorMessage = "The address \(host) is not available for binding."
        default:
            errorMessage = "Failed to bind to \(host):\(port). Error: \(error.localizedDescription)"
        }
        logger.error("\(errorMessage)")
        return TransportError.bindingFailed(errorMessage)
    }
}

/// Original accepted-channel custody, including setup queued on another loop.
/// These fixed callback fences depend on the reviewed pinned Bootstrap path.
final class NIOAcceptedChannelTicket: @unchecked Sendable {
    let channel: Channel
    let listener: Channel
    let setup: EventLoopPromise<Void>

    init(channel: Channel, listener: Channel) {
        self.channel = channel
        self.listener = listener
        self.setup = channel.eventLoop.makePromise(of: Void.self)
    }

    func join() async {
        // Expected setup/close failures are settled attempts, not missing joins.
        // A failure must not skip any subsequent original lifetime/fence await.
        await HTTPAdapterAcceptedOperation.run(
            setup: { _ = try? await self.setup.futureResult.get() },
            childFence: { _ = try? await self.channel.eventLoop.submit {}.get() },
            parentFence: { _ = try? await self.listener.eventLoop.submit {}.get() },
            closed: { _ = try? await self.channel.closeFuture.get() }
        )
    }
}

private final class NIOAcceptedChannelCollector: ChannelInboundHandler {
    typealias InboundIn = Channel
    private let adapter: NIOHTTPServerAdapter
    init(adapter: NIOHTTPServerAdapter) { self.adapter = adapter }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if adapter.accepted(unwrapInboundIn(data), on: context.channel) {
            context.fireChannelRead(data)
        }
    }
}

private func nioCloseAndJoin(_ channel: Channel) async {
    // alreadyClosed can fail the close attempt before original closeFuture ends.
    _ = try? await channel.close().get()
    _ = try? await channel.closeFuture.get()
}

/// The per-connection NIO channel handler: parses HTTP/1 request parts into an
/// `HTTPRequest` + an `AsyncStream<Data>` body, hands them to ``MCPHTTPEngine``,
/// and writes the ``EngineResponse`` back — buffered, or drained chunk-by-chunk
/// for SSE (one flush per event).
final class NIOHTTPChannelHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = HTTPRequestPart
    typealias OutboundOut = HTTPResponsePart

    private enum State {
        case idle
        case streaming(Request)
        case rejected
    }

    /// Accessed only on the channel's event loop. Response selection and first
    /// head enqueue are one transaction, including the competing 413 path.
    private final class Request: @unchecked Sendable {
        enum Disposition { case unclaimed, route, rejected }
        let head: HTTPRequest
        let body: InboundBodyBuffer
        var bytes = 0
        var disposition = Disposition.unclaimed
        var capability: HTTPAdapterDispatch.Capability?
        init(head: HTTPRequest, body: InboundBodyBuffer) {
            self.head = head
            self.body = body
        }
    }

    private var requestState = State.idle
    private let engine: any MCPHTTPEngine
    private let logger: Logger
    private let owner: HTTPAdapterDispatch

    init(engine: any MCPHTTPEngine, logger: Logger, owner: HTTPAdapterDispatch) {
        self.engine = engine
        self.logger = logger
        self.owner = owner
    }

    func channelInactive(context: ChannelHandlerContext) {
        abortInFlightRequestBody()
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        abortInFlightRequestBody()
        context.close(promise: nil)
    }

    /// Ends a mid-request body stream when the connection dies. Without this,
    /// a handler consuming an upload would wait for the next chunk forever —
    /// leaking the handler task and everything it holds open.
    private func abortInFlightRequestBody() {
        if case .streaming(let request) = requestState {
            request.body.abort()
        }
        requestState = .idle
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if let channelEvent = event as? ChannelEvent, channelEvent == .inputClosed {
            context.close(promise: nil)
            return
        }
        context.fireUserInboundEventTriggered(event)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let requestPart = unwrapInboundIn(data)

        switch (requestPart, requestState) {

        // HEAD — create stream and dispatch handler immediately
        case (.head(let head), _):
            guard !owner.requestsAreSealed else {
                context.close(promise: nil)
                return
            }
            let sizeLimit = engine.maxBodySize(for: head)
            if let contentLength = head.headerFields[.contentLength],
               let length = Int(contentLength), length > sizeLimit {
                logger.warning("Rejecting request with Content-Length \(length) > max \(sizeLimit)")
                rejectOversizedRequest(context: context, limit: sizeLimit, parent: nil)
                requestState = .rejected
                return
            }

            // Watermarked handoff: pausing reads when the consumer lags lets
            // TCP flow control push back on the sender, instead of a fast
            // upload with a slow downstream (e.g. Drive) queueing the entire
            // body in memory.
            let channel = context.channel
            let body = InboundBodyBuffer(
                pauseReads: {
                    channel.setOption(ChannelOptions.autoRead, value: false).whenComplete { _ in }
                },
                resumeReads: {
                    channel.setOption(ChannelOptions.autoRead, value: true).whenComplete { _ in
                        channel.read()
                    }
                }
            )
            let request = Request(head: head, body: body)
            requestState = .streaming(request)
            dispatchRoute(context: context, request: request)

        // BODY — append chunk to the buffer
        case (.body(let buffer), .streaming(let request)):
            let sizeLimit = engine.maxBodySize(for: request.head)
            let newTotal = request.bytes + buffer.readableBytes
            guard newTotal <= sizeLimit else {
                logger.warning("Rejecting request: body size \(newTotal) > max \(sizeLimit)")
                request.body.abort()
                if request.disposition == .unclaimed {
                    request.disposition = .rejected
                    rejectOversizedRequest(context: context, limit: sizeLimit, parent: request.capability)
                } else {
                    // A route head already started: never emit a second head.
                    context.close(promise: nil)
                }
                requestState = .rejected
                return
            }
            if let bytes = buffer.getBytes(at: buffer.readerIndex, length: buffer.readableBytes) {
                request.body.append(Data(bytes))
            }
            request.bytes = newTotal

        // END — finish the stream
        case (.end, .streaming(let request)):
            defer { requestState = .idle }
            request.body.finish()

        // Rejection / unexpected states
        case (.body, .rejected), (.end, .rejected):
            if case .end = requestPart { requestState = .idle }
        case (.body, _):
            logger.warning("Received unexpected body without a valid head")
        case (.end, .idle):
            logger.warning("Received end without prior request state")
        }
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

    // MARK: - Dispatch

    private func dispatchRoute(context: ChannelHandlerContext, request: Request) {
        let channel = context.channel
        let engine = self.engine
        let writer = HTTPAdapterResponseWriter(
            buffered: { response in
                guard case .buffered(let data) = response.body else { return .failed }
                let fields = HTTPResponseDefaults.buffered(response.headerFields, bodyLength: data?.count)
                let writes = try? await channel.eventLoop.submit { () -> [EventLoopFuture<Void>]? in
                    guard self.claimRoute(request) else { return nil }
                    return Self.enqueueBuffered(
                        head: HTTPResponse(status: response.status, headerFields: fields),
                        data: data, channel: channel
                    )
                }.get()
                guard let outer = writes else { return .discarded }
                return await Self.joinWrites(outer, channel: channel) ? .written : .failed
            },
            sseHead: { response in
                let write = try? await channel.eventLoop.submit { () -> EventLoopFuture<Void>? in
                    guard self.claimRoute(request) else { return nil }
                    return channel.writeAndFlush(HTTPResponsePart.head(HTTPResponse(
                        status: response.status,
                        headerFields: HTTPResponseDefaults.streaming(response.headerFields)
                    )))
                }.get()
                guard let original = write else { return .discarded }
                do { try await original.get(); return .written } catch { return .failed }
            },
            sseChunk: { chunk in
                do {
                    try await channel.writeAndFlush(HTTPResponsePart.body(channel.allocator.buffer(data: chunk))).get()
                    return true
                } catch { return false }
            },
            sseEnd: {
                do { try await channel.writeAndFlush(HTTPResponsePart.end(nil)).get(); return true }
                catch { return false }
            },
            connection: NIOSSEConnection(channel: channel),
            closed: { _ = try? await channel.closeFuture.get() },
            close: { await nioCloseAndJoin(channel) },
            finish: {
                // Always execute the generation-specific state tail. The close
                // attempt it starts is not mistaken for original closeFuture.
                let close = try? await channel.eventLoop.submit { () -> EventLoopFuture<Void>? in
                    if case .streaming(let active) = self.requestState, active === request {
                        self.abortInFlightRequestBody()
                        return channel.close()
                    }
                    return nil
                }.get()
                if let close {
                    _ = try? await close.get()
                    _ = try? await channel.closeFuture.get()
                }
            }
        )
        let original = owner.startRequest { parent in
            await HTTPAdapterRouteOperation.run(
                owner: self.owner, parent: parent, engine: engine,
                head: request.head, body: request.body.stream, writer: writer
            )
        }
        request.capability = original?.capability
        if original == nil {
            request.body.abort()
            channel.close(promise: nil)
        }
    }

    private func claimRoute(_ request: Request) -> Bool {
        guard !request.body.wasAborted, request.disposition == .unclaimed else { return false }
        request.disposition = .route
        return true
    }

    private static func enqueueBuffered(
        head: HTTPResponse, data: Data?, channel: Channel
    ) -> [EventLoopFuture<Void>] {
        var writes = [channel.write(HTTPResponsePart.head(head))]
        if let data, !data.isEmpty {
            writes.append(channel.write(HTTPResponsePart.body(channel.allocator.buffer(data: data))))
        }
        // Flush BEFORE awaiting any unflushed write (including head).
        writes.append(channel.writeAndFlush(HTTPResponsePart.end(nil)))
        return writes
    }

    private static func joinWrites(_ writes: [EventLoopFuture<Void>], channel: Channel) async -> Bool {
        await HTTPAdapterWriteOperation.join(writes.map { original in
            { @Sendable in
                do { try await original.get(); return true }
                catch { return false }
            }
        })
    }

    // MARK: - Oversize rejection

    private func rejectOversizedRequest(
        context: ChannelHandlerContext, limit: Int, parent: HTTPAdapterDispatch.Capability?
    ) {
        let reply = HTTPResponseDefaults.oversizedBody(limit: limit)
        let head = HTTPResponse(status: reply.status, headerFields: reply.headerFields)
        let channel = context.channel
        let write: @Sendable (HTTPAdapterDispatch.Capability) async -> Void = { _ in
            if let writes = try? await channel.eventLoop.submit({
                Self.enqueueBuffered(head: head, data: reply.body, channel: channel)
            }).get() {
                _ = await Self.joinWrites(writes, channel: channel)
            }
            await nioCloseAndJoin(channel)
        }
        let original: HTTPAdapterDispatch.Operation?
        if let parent { original = owner.startCleanup(from: parent, write) }
        else { original = owner.startRequest(write) }
        if original == nil { channel.close(promise: nil) }
    }
}

/// An ``SSEConnection`` backed by a live NIO `Channel`.
final class NIOSSEConnection: SSEConnection {
    private let channel: Channel
    init(channel: Channel) { self.channel = channel }
    var isConnected: Bool { channel.isActive }
    func terminate() { channel.close(promise: nil) }
}

/// A never-connected ``SSEConnection``, used to reconcile SSE streams whose
/// response was discarded before any connection could bind.
final class DeadSSEConnection: SSEConnection {
    init() {}
    var isConnected: Bool { false }
    func terminate() {}
}
#endif
