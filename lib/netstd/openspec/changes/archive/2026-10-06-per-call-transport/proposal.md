# Proposal

## Why

Concurrent or interleaved asynchronous calls through a shared `THttpTransport` can reset or dispose the class-level output buffer while another call is still sending it, producing failures such as the closed-stream exception reported by THRIFT-5830. Callers need an opt-in way to isolate transport state per call without changing the established shared-buffer behavior of existing clients.

## What Changes

- Add per-call transport semantics that isolate each call's buffers and protocol state, with `THttpTransport` as the initial built-in implementation and wrapping transports able to support or delegate the capability.
- Keep existing constructors and shared-buffer semantics as the compatibility default; add the generated-client construction support needed to opt into per-call behavior.
- Define safe completion, exception, cancellation, timeout, and disposal behavior for per-call resources.
- Preserve a caller-configured `HttpClient.Timeout` and propagate the linked per-call deadline through generated request send and response receive operations.
- Add focused transport/client tests, a working netstd tutorial example and test coverage, and usage documentation in the netstd README.
- Document new public/protected API and generator-emitted API, including when existing constructors remain appropriate.

## Capabilities

### New Capabilities
- `per-call-transport`: Opt-in per-call isolation for asynchronous client transport operations, including safe lifecycle and compatibility with shared transport behavior.

### Modified Capabilities

## Impact

- Affected implementation: `Thrift/Transport/Client/THttpTransport.cs`, `Thrift/Transport/Layered/TFramedTransport.cs`, `Thrift/TBaseClient.cs`, and generated client constructors in `compiler/cpp/src/thrift/generate/t_netstd_generator.cc`.
- Affected validation and docs: netstd transport/client tests, `tutorial/netstd` client and tutorial checks, and `lib/netstd/README.md`.
- Affected teams: netstd library maintainers, compiler/generator maintainers, and tutorial/test maintainers.
- Rollback: remove the opt-in per-call API and its generated constructor path while retaining the existing shared-mode constructors and behavior; revert the accompanying tutorial and documentation changes. No default behavior or wire format is intended to change.