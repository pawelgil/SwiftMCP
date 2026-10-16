#if Server
import Foundation
import Logging
@preconcurrency import NIOCore
import NIOFoundationCompat
import NIOHTTPTypes
import HTTPTypes

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

    private enum RequestDisposition { case unclaimed, route, rejected }

    /// Accessed only on the channel's event loop. Response selection and first
    /// head enqueue are one transaction, including the competing 413 path.
    private final class Request: @unchecked Sendable {
        let head: HTTPRequest
        let body: InboundBodyBuffer
        var bytes = 0
        var disposition = RequestDisposition.unclaimed
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
            beginRequest(head, context: context)

        // BODY — append chunk to the buffer
        case (.body(let buffer), .streaming(let request)):
            appendBody(buffer, to: request, context: context)

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

    private func beginRequest(_ head: HTTPRequest, context: ChannelHandlerContext) {
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
    }

    private func appendBody(_ buffer: ByteBuffer, to request: Request, context: ChannelHandlerContext) {
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
    }

    func channelReadComplete(context: ChannelHandlerContext) {
        context.flush()
    }

}

private extension NIOHTTPChannelHandler {
    // MARK: - Dispatch

    private func dispatchRoute(context: ChannelHandlerContext, request: Request) {
        let channel = context.channel
        let engine = self.engine
        let writer = responseWriter(for: request, channel: channel)
        let original = owner.startRequest { parent in
            await HTTPAdapterRouteOperation.run(
                owner: self.owner, parent: parent, engine: engine,
                request: (head: request.head, body: request.body.stream), writer: writer
            )
        }
        request.capability = original?.capability
        if original == nil {
            request.body.abort()
            channel.close(promise: nil)
        }
    }

    private func responseWriter(for request: Request, channel: Channel) -> HTTPAdapterResponseWriter {
        HTTPAdapterResponseWriter(
            buffered: { response in await self.writeBuffered(response, request: request, channel: channel) },
            sseHead: { response in await self.writeSSEHead(response, request: request, channel: channel) },
            sseChunk: { chunk in
                do {
                    try await channel.writeAndFlush(HTTPResponsePart.body(channel.allocator.buffer(data: chunk))).get()
                    return true
                } catch { return false }
            },
            sseEnd: {
                do {
                    try await channel.writeAndFlush(HTTPResponsePart.end(nil)).get()
                    return true
                } catch { return false }
            },
            connection: NIOSSEConnection(channel: channel),
            closed: { _ = try? await channel.closeFuture.get() },
            close: { await nioCloseAndJoin(channel) },
            finish: { await self.finishRoute(request, channel: channel) }
        )
    }

    private func writeBuffered(
        _ response: EngineResponse, request: Request, channel: Channel
    ) async -> HTTPAdapterWriteResult {
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
    }

    private func writeSSEHead(
        _ response: EngineResponse, request: Request, channel: Channel
    ) async -> HTTPAdapterWriteResult {
        let write = try? await channel.eventLoop.submit { () -> EventLoopFuture<Void>? in
            guard self.claimRoute(request) else { return nil }
            return channel.writeAndFlush(HTTPResponsePart.head(HTTPResponse(
                status: response.status,
                headerFields: HTTPResponseDefaults.streaming(response.headerFields)
            )))
        }.get()
        guard let original = write else { return .discarded }
        do { try await original.get(); return .written } catch { return .failed }
    }

    private func finishRoute(_ request: Request, channel: Channel) async {
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
                do { try await original.get(); return true } catch { return false }
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
        if let parent {
            original = owner.startCleanup(from: parent, write)
        } else {
            original = owner.startRequest(write)
        }
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
