import Foundation
import NIOCore
import NIOEmbedded
import NIOHTTP1
@testable import SwiftMCP
import Testing

@Suite(.timeLimit(.minutes(1)))
struct SSEPublicationTests {
    @Test(arguments: [(false, WriteGate.Part.headers), (true, .headers), (true, .endpoint)])
    private func channelWaitsForInitialResponse(sendEndpoint: Bool, heldPart: WriteGate.Part) async throws {
        try await withFixture(sendEndpoint: sendEndpoint, heldPart: heldPart) { fixture in
            let setup = Task { await fixture.connect() }
            await fixture.gate.waitForWrite()

            #expect(await fixture.publishedChannel() == nil)

            try await fixture.channel.triggerUserOutboundEvent(WriteGate.Resolution.succeed).get()
            await setup.value
            #expect(await fixture.publishedChannel() === fixture.channel)

            var buffer = fixture.channel.allocator.buffer(capacity: 16)
            buffer.writeString("data: response\n\n")
            try await fixture.channel.writeAndFlush(HTTPServerResponsePart.body(.byteBuffer(buffer))).get()

            let head = try await fixture.channel.readOutbound(as: HTTPServerResponsePart.self)
            guard case let .head(response) = head else {
                Issue.record("SSE response must start with HTTP headers")
                _ = try await fixture.channel.finish()
                return
            }
            #expect(response.status == .ok)
            if sendEndpoint {
                let endpoint = try await fixture.channel.readOutbound(as: HTTPServerResponsePart.self)
                #expect(bodyText(endpoint)?.contains("event: endpoint") == true)
            } else {
                #expect(response.headers["Mcp-Session-Id"] == [fixture.sessionID.uuidString])
            }
            let message = try await fixture.channel.readOutbound(as: HTTPServerResponsePart.self)
            #expect(bodyText(message) == "data: response\n\n")
            #expect(try await fixture.channel.finish().isClean)
        }
    }

    @Test(arguments: [(false, WriteGate.Part.headers), (true, .headers), (true, .endpoint)])
    private func failedInitialWriteNeverPublishes(sendEndpoint: Bool, heldPart: WriteGate.Part) async throws {
        try await withFixture(sendEndpoint: sendEndpoint, heldPart: heldPart) { fixture in
            let setup = Task { await fixture.connect() }
            await fixture.gate.waitForWrite()
            try await fixture.channel.triggerUserOutboundEvent(WriteGate.Resolution.fail).get()
            await setup.value

            #expect(await fixture.publishedChannel() == nil)
            #expect(!fixture.channel.isActive)
            let remaining = try await fixture.channel.readOutbound(as: HTTPServerResponsePart.self)
            #expect((remaining != nil) == (heldPart == .endpoint))
            #expect(try await fixture.channel.finish(acceptAlreadyClosed: true).isClean)
        }
    }

    @Test
    private func closedConnectionIsNotPublished() async throws {
        try await withFixture(sendEndpoint: false, heldPart: .headers) { fixture in
            let setup = Task { await fixture.connect() }
            await fixture.gate.waitForWrite()
            try await fixture.channel.triggerUserOutboundEvent(WriteGate.Resolution.close).get()
            await setup.value

            #expect(await fixture.publishedChannel() == nil)
        }
    }

    private func withFixture(sendEndpoint: Bool, heldPart: WriteGate.Part,
                             operation: (Fixture) async throws -> Void) async throws
    {
        let fixture = try await Fixture(sendEndpoint: sendEndpoint, heldPart: heldPart)
        do {
            try await operation(fixture)
        } catch {
            do { try await fixture.stop() }
            catch { Issue.record(error, "Failed to stop SSE test fixture") }
            throw error
        }
        try await fixture.stop()
    }

    private func bodyText(_ part: HTTPServerResponsePart?) -> String? {
        guard case let .body(.byteBuffer(buffer)) = part else { return nil }
        return String(buffer: buffer)
    }

    private struct Fixture {
        let sessionID = UUID()
        let transport = HTTPSSETransport(server: Calculator(), host: "localhost", port: 8080)
        let channel: NIOAsyncTestingChannel
        let gate: WriteGate
        let sendEndpoint: Bool

        init(sendEndpoint: Bool, heldPart: WriteGate.Part) async throws {
            self.sendEndpoint = sendEndpoint
            gate = WriteGate(part: heldPart)
            channel = await NIOAsyncTestingChannel(handler: gate)
            try await channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 8080)).get()
        }

        func connect() async {
            let headers = HTTPHeaders([
                ("Accept", "text/event-stream"),
                ("Host", "localhost:8080"),
                ("Mcp-Session-Id", sessionID.uuidString),
            ])
            let head = HTTPRequestHead(version: .http1_1, method: .GET,
                                       uri: sendEndpoint ? "/sse" : "/mcp", headers: headers)
            await HTTPHandler(transport: transport).handleSSE(
                channel: channel, head: head, body: nil, sendEndpoint: sendEndpoint,
            )
        }

        func publishedChannel() async -> Channel? {
            await transport.sessionManager.getChannel(for: sessionID)
        }

        func stop() async throws {
            if channel.isActive { try await channel.close().get() }
            try await transport.stop()
        }
    }

    /// NIO confines mutable handler state to its event loop; the receipt is Sendable.
    private final class WriteGate: ChannelOutboundHandler, @unchecked Sendable {
        typealias OutboundIn = HTTPServerResponsePart
        typealias OutboundOut = HTTPServerResponsePart

        enum Resolution { case succeed, fail, close }
        enum Part { case headers, endpoint }
        enum Failure: Error { case initialWrite }

        private let part: Part
        private let receipt = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        private var heldWrite: (HTTPServerResponsePart, EventLoopPromise<Void>?)?
        private var hasHeldWrite = false

        init(part: Part) {
            self.part = part
        }

        func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
            let part = unwrapOutboundIn(data)
            guard !hasHeldWrite, shouldHold(part) else {
                context.write(data, promise: promise)
                return
            }
            heldWrite = (part, promise)
            hasHeldWrite = true
            receipt.continuation.yield(())
            receipt.continuation.finish()
        }

        func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any,
                                      promise: EventLoopPromise<Void>?)
        {
            guard let resolution = event as? Resolution, let (part, writePromise) = heldWrite else {
                context.triggerUserOutboundEvent(event, promise: promise)
                return
            }
            heldWrite = nil
            switch resolution {
            case .succeed:
                context.writeAndFlush(wrapOutboundOut(part), promise: writePromise)
            case .fail:
                writePromise?.fail(Failure.initialWrite)
            case .close:
                context.close(promise: nil)
                writePromise?.succeed(())
            }
            promise?.succeed(())
        }

        func waitForWrite() async {
            for await _ in receipt.stream {
                return
            }
        }

        private func shouldHold(_ response: HTTPServerResponsePart) -> Bool {
            switch (part, response) {
            case (.headers, .head), (.endpoint, .body): true
            default: false
            }
        }
    }
}
