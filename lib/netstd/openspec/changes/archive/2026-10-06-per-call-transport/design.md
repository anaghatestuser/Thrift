# Design

## Context

`THttpTransport` currently stores its output and input streams on the transport instance. `FlushAsync` posts the shared output stream, replaces it during cleanup, and `Dispose` closes both streams and the client. `TFramedTransport` also owns read/write frame buffers. Generated service methods execute send, flush, and receive in one asynchronous method, while generated clients currently retain fixed input/output protocols. See proposal.md for motivation and spec.md for the required behavior.

## Goals / Non-Goals

**Goals:**
- Add an explicit construction path for generated clients to create a per-call transport and protocols.
- Keep each generated high-level call's transport, buffers, response, and protocol state alive from serialization through response consumption.
- Let transport wrappers explicitly delegate the capability and preserve the current constructors as shared-mode behavior.
- Apply one caller cancellation/timeout scope across the whole call and clean up only that call's resources.

**Non-Goals:**
- Change the wire protocol or make existing protocol-based constructors opt into per-call behavior.
- Make a manually split sequence of generated `send_*` and `recv_*` methods a per-call operation; those low-level methods retain their existing shared-protocol behavior.
- Make wrappers that do not opt into per-call support appear capable automatically.

## Decisions

- **Use explicit transport capability and additive constructors.** Add a per-call provider contract for a transport to create a call-scoped transport. Add a `TBaseClient` constructor path that accepts the provider and protocol factories, and emit corresponding constructors for generated clients. Preserve the existing protocol constructors and document that they remain the shared-mode path. This avoids changing behavior for existing callers; silently changing every client to per-call operation was rejected because it would alter resource and concurrency semantics.
- **Scope the full generated call in `TBaseClient`.** The generated convenience method will execute its existing send, flush, and receive sequence through one base-client per-call helper. The helper passes its linked caller/deadline token into the generated callback so serialization, flush, and response receive share one deadline. Allocate call identifiers atomically so concurrent calls through one client cannot collide. Keep direct low-level `send_*` and `recv_*` APIs on the legacy protocol path, as they do not represent one complete call lifetime.
- **Isolate mutable HTTP state, not the connection pool.** Each HTTP call owns its request body, response stream, and any per-call wrapper buffers. Reuse the thread-safe `HttpClient` owned by the root `THttpTransport`; a call-scoped transport must not dispose that shared client. Preserve the timeout of an `HttpClient` supplied by the caller, while internally created clients retain the transport's default timeout. Coordinate root disposal with active call leases so the client is not closed while a call still uses it. Creating and disposing a separate `HttpClient` per call was rejected because it discards connection pooling and complicates ownership.
- **Delegate wrappers explicitly.** `TBufferedTransport` and `TFramedTransport` each implement the provider and recreate themselves around a per-call underlying transport, preserving their configured buffer/frame behavior and delegating the timeout. `TLayeredTransport` does not implement the provider: it cannot recreate arbitrary derived wrapper types or their configuration, and would falsely advertise support for external subclasses. This follows the existing transport-composition model rather than adding special handling in generated clients for each wrapper type.
- **Bound the whole call with one linked cancellation scope.** Link the caller token with the configured transport timeout before acquiring/using call resources, and pass the linked token through protocol I/O and transport operations. Translate timeout/cancellation using the existing interrupted transport exception behavior; canceling one call must not cancel another.
- **Keep resource cleanup local and idempotent.** Dispose call-owned protocols, streams, content, and wrapper transports on success, fault, or cancellation; do not dispose root-owned shared resources from a call. Ensure each lease is released once, including failures during provider creation or protocol construction.

## Risks / Trade-offs

- [A custom wrapper may contain state that cannot be recreated per call] → Require explicit provider delegation and test built-in wrappers; retain shared mode for unsupported wrappers.
- [Generated high-level methods and direct `send_*`/`recv_*` calls have different concurrency guarantees] → Document the boundary on the new and existing generated APIs and exercise the supported high-level workflow in the tutorial.
- [Root transport disposal can race with active operations] → Track active call leases and defer disposal of root-owned HTTP resources until outstanding calls release them.
- [Timing-based concurrency tests may be flaky] → Use a controllable HTTP handler/server and synchronization gates to force request/response interleavings and timeout paths.
- [A layered wrapper may fail to delegate a newly supported inner wrapper] → Test both direct framed delegation and buffered-over-framed composition through the tutorial smoke path.

## Migration Plan

The change is additive. Existing clients and constructors continue to use shared transport semantics. Users opt into per-call behavior by constructing a generated client with a supporting transport and protocol factories. Rollback removes the opt-in capability and generated constructor path while retaining existing constructors, as described in proposal.md; no wire-format or default-mode migration is needed.