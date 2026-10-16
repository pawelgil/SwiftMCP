import Foundation

extension Session {
    /// Sends a JSON-RPC message to the client and waits for the response.
    /// - Parameter message: The JSON-RPC message to send
    /// - Returns: The response message from the client
    /// - Throws: An error if the message fails to send or if no response is received
    @discardableResult public func send(_ message: JSONRPCMessage) async throws -> JSONRPCMessage {
        guard let messageId = message.id else {
            preconditionFailure("Message requires an id")
        }

        // A retained session must not park a new sampling/elicitation/request
        // waiter after logical removal has performed its final response drain.
        // This also closes admission while the manager's disconnection mark is
        // set but its actor-hop cancellation/end-event work is still pending.
        guard !disconnection.isMarked else { throw CancellationError() }

        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<JSONRPCMessage, Error>) in
            responses.register(messageId, continuation)

            // Send the message via the transport, activating the session context.
            Task {
                do {
                    try await self.sendRegisteredRequest(message)
                } catch {
                    // A no-op if a reply already resolved this id.
                    _ = self.responses.fail(messageId, with: error)
                }
            }
        }
    }

    /// The deferred send stage rechecks logical admission: the session may have
    /// ended and drained this waiter after registration but before this task ran.
    /// Cancellation cannot retract a transport send which has already begun.
    internal func sendRegisteredRequest(_ message: JSONRPCMessage) async throws {
        try await self.work { _ in
            guard !self.disconnection.isMarked else { throw CancellationError() }
            try await transport?.send(message)
        }
    }

    /// Handles an incoming JSON-RPC response by matching it with a waiting continuation.
    /// - Parameter response: The response message to handle
    internal func handleResponse(_ response: JSONRPCMessage) {
        guard let messageId = response.id else { return }
        responses.resolve(messageId, with: response)
    }

    /// Sends a JSON-RPC request with an auto-generated ID and waits for the response.
    /// - Parameters:
    ///   - method: The method name to call
    ///   - params: Optional parameters for the request
    /// - Returns: The response message from the client
    /// - Throws: An error if the request fails or if no response is received
    public func request(method: String, params: JSONDictionary? = nil) async throws -> JSONRPCMessage {
        let message = JSONRPCMessage.request(
            id: .string(UUID().uuidString),
            method: method,
            params: params.map(JSONValue.object)
        )
        return try await send(message)
    }

    public func request<T: Encodable & Sendable>(method: String, params value: T) async throws -> JSONRPCMessage {
        try await request(method: method, params: JSONDictionary(encoding: value))
    }

    /// Cancels all waiting continuations when the session is being removed.
    /// This prevents continuation leaks when sessions are disconnected.
    internal func cancelAllWaitingTasks() {
        responses.failAll()
    }
}
