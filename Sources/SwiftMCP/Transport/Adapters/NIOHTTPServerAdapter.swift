//
//  NIOHTTPServerAdapter.swift
//  SwiftMCP
//
//  The swift-nio adapter behind the ``MCPHTTPEngine`` seam: the listening socket,
//  the HTTP/1 pipeline, the per-connection read/write loops, and the `Channel`
//  liveness/close wiring. Only these adapter files in the HTTP/SSE server
//  link the swift-nio server stack — the engine (`HTTPSSETransport`, routing,
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
                self.initializeChild(channel)
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

    private func initializeChild(_ channel: Channel) -> EventLoopFuture<Void> {
        let engine = self.engine
        let logger = self.logger
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
        }, { _ in await ticket.join() })
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

func nioCloseAndJoin(_ channel: Channel) async {
    // alreadyClosed can fail the close attempt before original closeFuture ends.
    _ = try? await channel.close().get()
    _ = try? await channel.closeFuture.get()
}

#endif
