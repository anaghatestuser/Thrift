# Tasks

## 1. Opt-In API and Compatibility

- [x] 1.1 Add failing client tests for the `Existing client retains shared transport behavior`, `Client opts into per-call transport behavior`, and `Wrapper delegates per-call capability` scenarios; verify they fail because no per-call construction path exists.
- [x] 1.2 Add the per-call provider contract and `TBaseClient` call context/protocol-factory construction path, then emit matching constructors from `t_netstd_generator.cc`; verify the focused client tests and generated-client compile tests pass.
- [x] 1.3 Refactor constructor and provider ownership as needed, and add XML documentation to new public/protected APIs and generator output explaining when the existing shared-protocol constructors remain appropriate; verify generated API documentation and compile tests.

## 2. Concurrent Call Isolation

- [x] 2.1 Add deterministic failing tests for `Interleaved HTTP calls complete independently` and `Per-call read-ahead does not consume another response`, using one client and controlled request/response interleaving; verify the tests reproduce shared-buffer or cross-call state interference.
- [x] 2.2 Scope generated high-level send/flush/receive operations to one call context, isolate HTTP request/response streams and wrapper buffers, and allocate call identifiers without races; verify both concurrency scenarios pass, including response-to-request association.
- [x] 2.3 Refactor call-context and wrapper delegation paths without changing shared-mode behavior; rerun the focused concurrency and existing transport tests.

## 3. Resource Lifetime, Cancellation, and Timeouts

- [x] 3.1 Add failing lifecycle tests for `Successful call releases its owned resources`, `Failed call releases resources and leaves other calls usable`, `Interrupted call releases resources`, `Caller cancellation reaches an in-flight request`, and `Configured timeout bounds an in-flight call`; verify each exercises its respective terminal path.
- [x] 3.2 Implement exception-safe per-call cleanup, root HTTP-client lease coordination, and a linked caller-cancellation/transport-timeout scope across send and response consumption; verify the lifecycle and timeout tests pass and cancellation of one call leaves another active.
- [x] 3.3 Refactor ownership and timeout handling to release each call-owned resource once on success, fault, and interruption; rerun all focused lifecycle and cancellation tests.

## 4. Tutorial and Usage Documentation

- [x] 4.1 Add an automated tutorial smoke assertion for the per-call client workflow; verify it fails before the tutorial exposes per-call mode.
- [x] 4.2 Add a per-call usage example to `tutorial/netstd` and document construction, opt-in behavior, and the shared-mode constructor choice in `lib/netstd/README.md`; verify the example builds and the documented invocation runs.
- [x] 4.3 Refactor the example and documentation for consistency with generated API comments, including the boundary for direct `send_*`/`recv_*` use; verify the tutorial smoke test passes.

## 5. Cross-Target Integration

- [x] 5.1 Build and run the focused transport/client test suite for `netstandard2.0`, `netstandard2.1`, `net8.0`, `net9.0`, and `net10.0`; verify the new API and existing constructors compile on every supported target.
- [x] 5.2 Run the netstd tutorial smoke workflow and the generator's relevant compile tests together; verify the tutorial works end-to-end and generated clients preserve shared-mode compatibility while supporting per-call calls.

## 6. Framed Wrapper Delegation

- [x] 6.1 Add failing tests for direct framed-provider delegation and buffered-over-framed composition; verify they fail before framed transports advertise per-call support.
- [x] 6.2 Implement per-call provider and timeout delegation on `TFramedTransport`, recreating its frame buffers around a new inner transport; verify the focused wrapper tests and framed HTTP handler test pass.
- [x] 6.3 Confirm `TLayeredTransport` does not advertise the provider to unsupported subclasses, update wrapper documentation as needed, and rerun the focused transport and tutorial checks.

## 7. Whole-Call Timeout Propagation

- [x] 7.1 Add failing tests for caller-provided `HttpClient.Timeout` preservation and linked-token delivery to a generated RPC; verify both regressions before implementation.
- [x] 7.2 Preserve supplied HTTP-client timeout values and pass the combined caller/deadline token into generated send and receive phases; verify the focused timeout tests pass.
- [x] 7.3 Refactor timeout validation and overload compatibility, update the main capability spec, and verify generated-client builds plus all focused transport tests.