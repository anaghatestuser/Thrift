# Per-Call Transport

Create an extension to existing transports so that per-call semantics are enabled such that
asynchronous and interleaved calls are safe and can implement a read-ahead buffering protocol.

## TDD Discipline (non-negotiable)
- Follow Red–Green–Refactor.
- For every behavior in the specs, write failing tests FIRST, then implement.
- Task lists must explicitly sequence: tests → implementation → refactor.

## Features

### per-call-transport

- add a new mechanism for per-call transport to support the existing THttpTransport and any wrapping transports that can support or delegate the per-call semantics.
- reference the observed bug from https://issues.apache.org/jira/browse/THRIFT-5830 where interleaved and asynchronous call can cause exceptions due to the shared buffers being created and disposed before the previous call can retrieve its contents.
- When using the THttpTransport, interleaved asynchronous calls can cause the following exception: System.AggregateException : One or more errors occurred. Couldn't connect to server: System.Net.Http.HttpRequestException: Error while copying content to a stream.  ---> System.ObjectDisposedException: Cannot access a closed Stream.
- Essentially, the THttpTransport._outputStream can get disposed while another asynchronous call is running. This is due to allocating the `_outputStream` at the class level. The stream should be allocated on a call-by-call basis.
- backward compatibility must be maintained for the shared buffer semantics.
- ensure race conditions are handled safely.
- ensure disposing buffers and clients are done safely in both the happy path and any situation where an exception may interrupt the execution path.
- ensure the time-outs are handled properly from end-to-end.
- ensure TDD practices are followed.
- update the thrift\compiler\cpp\src\thrift\generate\t_netstd_generator.cc with any required changes to the TBaseClient constructors.
- ensure any new public and protected members have a documentation comment, including the t_netstd_generator.cc additions.
- if t_netstd_generator.cc has additions, update the documentation of the existing code to clarify why one would use the previous versions.
- add a tutorial addition for the per-call transport usage (test to ensure tutorial works). 
- update the netstd/README.md with per-call documentation and usage.

## Constraints and Non-Goals
