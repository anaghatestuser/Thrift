# per-call-transport Specification

## Purpose

Allows generated asynchronous clients to opt into independent transport state for each call, so overlapping calls do not interfere with request or response data while existing shared-transport clients remain compatible.

## Requirements

### Requirement: Per-call transport is opt-in
The system MUST allow clients to use per-call transport semantics when the supplied transport supports or delegates that capability, while preserving existing shared-transport behavior for clients that do not opt in.

#### Scenario: Existing client retains shared transport behavior
- **GIVEN** a client is created with the existing protocol-based constructors
- **WHEN** the client performs a call over a transport without per-call semantics enabled
- **THEN** the call uses the existing shared transport and protocol behavior

#### Scenario: Client opts into per-call transport behavior
- **GIVEN** a client is constructed with a transport that supports per-call semantics
- **WHEN** the client starts an asynchronous call
- **THEN** that call uses transport and protocol state scoped to the call

#### Scenario: Wrapper delegates per-call capability
- **GIVEN** a transport wrapper delegates per-call operations to an underlying transport that supports them
- **WHEN** a client uses the wrapper for an asynchronous call
- **THEN** the call receives the same isolation guarantees as when using the underlying capability directly

#### Scenario: Framed wrapper delegates per-call capability
- **GIVEN** a `TFramedTransport` wraps a transport that supports per-call operations
- **WHEN** a client uses the framed wrapper for an asynchronous call
- **THEN** the call uses a new framed wrapper with independent frame buffers around its own per-call underlying transport

#### Scenario: Layered wrappers compose per-call capabilities
- **GIVEN** buffered and framed wrappers are layered over a transport that supports per-call operations
- **WHEN** a client performs an asynchronous call through the layered transport
- **THEN** each wrapper layer is recreated around the call's underlying transport and delegates its timeout

### Requirement: Concurrent calls keep request and response state isolated
The system MUST prevent one per-call operation from modifying, replacing, or disposing another operation's request buffers, response buffers, or protocol state, and each reply MUST remain associated with its originating call.

#### Scenario: Interleaved HTTP calls complete independently
- **GIVEN** two asynchronous calls use the same per-call-enabled client through an HTTP transport
- **WHEN** their request sends and response reads overlap
- **THEN** each call sends its complete request and receives the response associated with that request without a closed-stream failure or cross-call data

#### Scenario: Per-call read-ahead does not consume another response
- **GIVEN** concurrent calls use a transport or wrapper with read-ahead buffering
- **WHEN** either call reads ahead while the other call is active
- **THEN** buffered bytes remain associated with their originating call

### Requirement: Per-call resources are cleaned up on every terminal path
The system MUST keep resources needed by a call alive until that call has finished consuming its response, then release resources owned by that call exactly once whether the call succeeds, fails, or is interrupted.

#### Scenario: Successful call releases its owned resources
- **GIVEN** a per-call operation completes its request and response successfully
- **WHEN** the call finishes
- **THEN** its call-owned buffers and resources are released without disposing resources still in use by another call

#### Scenario: Failed call releases resources and leaves other calls usable
- **GIVEN** one per-call operation fails while another call is active
- **WHEN** failure cleanup runs
- **THEN** only resources owned by the failed call are released and the other call can continue

#### Scenario: Interrupted call releases resources
- **GIVEN** a per-call operation is canceled or reaches its configured timeout
- **WHEN** the operation terminates
- **THEN** its call-owned resources are released and no resource remains disposed while another call still uses it

### Requirement: Cancellation and timeouts cover the per-call operation
The system MUST propagate cancellation and enforce the configured timeout across the asynchronous per-call transport operation, and termination of one call MUST NOT cancel unrelated calls.

#### Scenario: Caller cancellation reaches an in-flight request
- **GIVEN** a per-call operation is sending or consuming a response
- **WHEN** its caller cancels the operation
- **THEN** the operation terminates with the transport's established interrupted-operation behavior and releases its resources

#### Scenario: Configured timeout bounds an in-flight call
- **GIVEN** a per-call operation exceeds the configured transport timeout while sending or consuming its response
- **WHEN** the timeout expires
- **THEN** that operation terminates with the transport's established interrupted-operation behavior and does not affect other calls

#### Scenario: Caller-provided HTTP timeout is preserved
- **GIVEN** an `HttpClient` has a caller-configured timeout before it is supplied to `THttpTransport`
- **WHEN** a generated client performs a per-call operation through that transport
- **THEN** the supplied timeout value remains unchanged and bounds the complete operation

#### Scenario: Per-call deadline reaches the generated RPC
- **GIVEN** a generated high-level RPC runs through a per-call-enabled transport
- **WHEN** its linked caller/deadline token is canceled during request serialization, flush, or response receive
- **THEN** the active phase observes that token and the RPC terminates with the interrupted-operation behavior