// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Thrift.Tests.Transports
{
    [TestClass]
    public class TBaseClientPerCallTests
    {
        [TestMethod]
        public async Task Existing_protocol_constructor_keeps_shared_protocols()
        {
            var transport = new TMemoryBufferTransport(null);
            var protocol = new TBinaryProtocol.Factory().GetProtocol(transport);
            using var client = new TestClient(protocol);

            await client.RunAsync(() =>
            {
                Assert.AreSame(protocol, client.InputProtocol);
                Assert.AreSame(protocol, client.OutputProtocol);
                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public void Provider_constructor_rejects_null_transport()
        {
            try
            {
                _ = new TestClient((TTransport)null!);
                Assert.Fail("Expected the provider constructor to reject a null transport.");
            }
            catch (ArgumentNullException exception)
            {
                Assert.AreEqual("transport", exception.ParamName);
            }
        }

        [TestMethod]
        public void Per_call_operation_rejects_null_delegate()
        {
            using var rootTransport = new MemoryPerCallTransport();
            using var client = new TestClient(rootTransport);

            try
            {
                client.RunAsync((Func<Task>)null!);
                Assert.Fail("Expected the per-call API to reject a null operation.");
            }
            catch (ArgumentNullException exception)
            {
                Assert.AreEqual("operation", exception.ParamName);
            }
        }

        [TestMethod]
        public async Task Per_call_provider_rejects_null_transport_result()
        {
            using var rootTransport = new NullPerCallTransport();
            using var client = new TestClient(rootTransport);

            try
            {
                await client.RunAsync(() => Task.CompletedTask);
                Assert.Fail("Expected the client to reject a null transport result.");
            }
            catch (InvalidOperationException exception)
            {
                Assert.AreEqual("The per-call transport provider returned null.", exception.Message);
            }
        }

        [TestMethod]
        public async Task Provider_constructor_creates_protocols_for_each_call()
        {
            using var rootTransport = new MemoryPerCallTransport();
            using var client = new TestClient(rootTransport);
            TTransport? firstCallTransport = null;
            TTransport? secondCallTransport = null;

            await client.RunAsync(() =>
            {
                firstCallTransport = client.InputProtocol.Transport;
                Assert.AreSame(firstCallTransport, client.OutputProtocol.Transport);
                return Task.CompletedTask;
            });

            await client.RunAsync(() =>
            {
                secondCallTransport = client.InputProtocol.Transport;
                Assert.AreSame(secondCallTransport, client.OutputProtocol.Transport);
                return Task.CompletedTask;
            });

            Assert.AreNotSame(rootTransport, firstCallTransport);
            Assert.AreNotSame(firstCallTransport, secondCallTransport);
        }

        [TestMethod]
        public async Task Per_call_timeout_cancels_operation_token_without_canceling_caller_token()
        {
            using var rootTransport = new MemoryPerCallTransport(TimeSpan.FromMilliseconds(100));
            using var client = new TestClient(rootTransport);
            using var callerCancellation = new CancellationTokenSource();
            var operationStarted = NewSignal();
            var operationToken = CancellationToken.None;

            var operation = client.RunAsync<int>(async cancellationToken =>
            {
                operationToken = cancellationToken;
                operationStarted.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return 1;
            }, callerCancellation.Token);

            await operationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var completed = await Task.WhenAny(operation, Task.Delay(TimeSpan.FromSeconds(2)));
            if (completed != operation)
            {
                callerCancellation.Cancel();
                await CaptureTransportExceptionAsync(() => operation);
                Assert.Fail("The per-call timeout did not cancel the operation token.");
            }
            var exception = await CaptureTransportExceptionAsync(() => operation);

            Assert.AreEqual(TTransportException.ExceptionType.Interrupted, exception.Type);
            Assert.IsTrue(operationToken.IsCancellationRequested);
            Assert.IsFalse(callerCancellation.IsCancellationRequested);
        }

        [TestMethod]
        public async Task Caller_token_is_combined_with_per_call_timeout()
        {
            using var rootTransport = new MemoryPerCallTransport(TimeSpan.FromSeconds(5));
            using var client = new TestClient(rootTransport);
            using var callerCancellation = new CancellationTokenSource();
            var operationStarted = NewSignal();
            var operationToken = CancellationToken.None;

            var operation = client.RunAsync<int>(async cancellationToken =>
            {
                operationToken = cancellationToken;
                operationStarted.TrySetResult(true);
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return 1;
            }, callerCancellation.Token);

            await operationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            callerCancellation.Cancel();
            var exception = await CaptureTransportExceptionAsync(() => operation);

            Assert.AreEqual(TTransportException.ExceptionType.Interrupted, exception.Type);
            Assert.IsTrue(operationToken.IsCancellationRequested);
            Assert.IsTrue(callerCancellation.IsCancellationRequested);
        }

        [TestMethod]
        public async Task Generated_rpc_uses_the_linked_per_call_token_for_send()
        {
            using var rootTransport = new BlockingPerCallTransport(TimeSpan.FromMilliseconds(100));
            var protocolFactory = new TBinaryProtocol.Factory();
            using var client = new global::ThriftTest.ThriftTest.Client(rootTransport,
                protocolFactory, protocolFactory);
            using var callerCancellation = new CancellationTokenSource();

            var operation = client.testVoid(callerCancellation.Token);
            var completed = await Task.WhenAny(operation, Task.Delay(TimeSpan.FromSeconds(2)));
            if (completed != operation)
            {
                callerCancellation.Cancel();
                await CaptureTransportExceptionAsync(() => operation);
                Assert.Fail("The generated RPC did not use the per-call timeout token.");
            }

            var exception = await CaptureTransportExceptionAsync(() => operation);
            Assert.AreEqual(TTransportException.ExceptionType.Interrupted, exception.Type);
            Assert.IsFalse(callerCancellation.IsCancellationRequested);
        }

        [TestMethod]
        public async Task Buffered_wrapper_delegates_per_call_transport_creation()
        {
            using var rootTransport = new MemoryPerCallTransport();
            var sharedWrapper = new TBufferedTransport(rootTransport);
            using var client = new TestClient(sharedWrapper);
            TBufferedTransport? callWrapper = null;
            TTransport? callUnderlyingTransport = null;

            await client.RunAsync(() =>
            {
                callWrapper = client.InputProtocol.Transport as TBufferedTransport;
                Assert.IsNotNull(callWrapper);
                Assert.AreSame(callWrapper, client.OutputProtocol.Transport);
                callUnderlyingTransport = callWrapper.UnderlyingTransport;
                return Task.CompletedTask;
            });

            Assert.AreNotSame(sharedWrapper, callWrapper);
            Assert.AreNotSame(rootTransport, callUnderlyingTransport);
        }

        [TestMethod]
        public async Task Buffered_wrapper_without_per_call_support_keeps_shared_protocols()
        {
            var sharedTransport = new TBufferedTransport(new TMemoryBufferTransport(null));
            using var client = new TestClient(sharedTransport);

            await client.RunAsync(() =>
            {
                Assert.AreSame(sharedTransport, client.InputProtocol.Transport);
                Assert.AreSame(sharedTransport, client.OutputProtocol.Transport);
                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public async Task Framed_wrapper_delegates_per_call_transport_creation()
        {
            using var rootTransport = new MemoryPerCallTransport();
            var sharedWrapper = new TFramedTransport(rootTransport);
            using var client = new TestClient(sharedWrapper);
            TFramedTransport? callWrapper = null;
            TTransport? callUnderlyingTransport = null;

            await client.RunAsync(() =>
            {
                callWrapper = client.InputProtocol.Transport as TFramedTransport;
                Assert.IsNotNull(callWrapper);
                Assert.AreSame(callWrapper, client.OutputProtocol.Transport);
                callUnderlyingTransport = callWrapper.InnerTransport;
                return Task.CompletedTask;
            });

            Assert.AreNotSame(sharedWrapper, callWrapper);
            Assert.AreNotSame(rootTransport, callUnderlyingTransport);
            Assert.AreEqual(rootTransport.PerCallTimeout, sharedWrapper.PerCallTimeout);
        }

        [TestMethod]
        public async Task Framed_wrapper_without_per_call_support_keeps_shared_protocols()
        {
            var sharedTransport = new TFramedTransport(new TMemoryBufferTransport(null));
            using var client = new TestClient(sharedTransport);

            await client.RunAsync(() =>
            {
                Assert.AreSame(sharedTransport, client.InputProtocol.Transport);
                Assert.AreSame(sharedTransport, client.OutputProtocol.Transport);
                return Task.CompletedTask;
            });
        }

        [TestMethod]
        public async Task Buffered_wrapper_delegates_through_framed_transport()
        {
            using var rootTransport = new MemoryPerCallTransport();
            var sharedFramedTransport = new TFramedTransport(rootTransport);
            var sharedBufferedTransport = new TBufferedTransport(sharedFramedTransport);
            using var client = new TestClient(sharedBufferedTransport);
            TBufferedTransport? callBufferedTransport = null;
            TFramedTransport? callFramedTransport = null;
            TTransport? callUnderlyingTransport = null;

            await client.RunAsync(() =>
            {
                callBufferedTransport = client.InputProtocol.Transport as TBufferedTransport;
                Assert.IsNotNull(callBufferedTransport);
                Assert.AreSame(callBufferedTransport, client.OutputProtocol.Transport);
                callFramedTransport = callBufferedTransport.UnderlyingTransport as TFramedTransport;
                Assert.IsNotNull(callFramedTransport);
                callUnderlyingTransport = callFramedTransport.InnerTransport;
                return Task.CompletedTask;
            });

            Assert.AreNotSame(sharedBufferedTransport, callBufferedTransport);
            Assert.AreNotSame(sharedFramedTransport, callFramedTransport);
            Assert.AreNotSame(rootTransport, callUnderlyingTransport);
            Assert.AreEqual(rootTransport.PerCallTimeout, sharedBufferedTransport.PerCallTimeout);
        }

        [TestMethod]
        public void Layered_transport_base_does_not_advertise_per_call_support()
        {
            Assert.IsFalse(typeof(ITPerCallTransportProvider).IsAssignableFrom(typeof(TLayeredTransport)));
        }

        private sealed class MemoryPerCallTransport : TMemoryBufferTransport, ITPerCallTransportProvider
        {
            public MemoryPerCallTransport(TimeSpan? perCallTimeout = null)
                : base(null)
            {
                PerCallTimeout = perCallTimeout ?? Timeout.InfiniteTimeSpan;
            }

            public TimeSpan PerCallTimeout { get; }

            public bool SupportsPerCallTransport => true;

            public Task<TTransport> CreatePerCallTransportAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<TTransport>(new TMemoryBufferTransport(Configuration));
            }
        }

        private sealed class BlockingPerCallTransport : TMemoryBufferTransport, ITPerCallTransportProvider
        {
            public BlockingPerCallTransport(TimeSpan perCallTimeout)
                : base(null)
            {
                PerCallTimeout = perCallTimeout;
            }

            public TimeSpan PerCallTimeout { get; }

            public bool SupportsPerCallTransport => true;

            public Task<TTransport> CreatePerCallTransportAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<TTransport>(new BlockingMemoryBufferTransport(Configuration));
            }
        }

        private sealed class NullPerCallTransport : TMemoryBufferTransport, ITPerCallTransportProvider
        {
            public NullPerCallTransport()
                : base(null)
            {
            }

            public TimeSpan PerCallTimeout => Timeout.InfiniteTimeSpan;

            public bool SupportsPerCallTransport => true;

            public Task<TTransport> CreatePerCallTransportAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<TTransport>(null!);
            }
        }

        private sealed class BlockingMemoryBufferTransport : TMemoryBufferTransport
        {
            public BlockingMemoryBufferTransport(TConfiguration configuration)
                : base(configuration)
            {
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count,
                CancellationToken cancellationToken)
            {
                return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
        }

        private sealed class TestClient : TBaseClient, System.IDisposable
        {
            private static readonly TProtocolFactory ProtocolFactory = new TBinaryProtocol.Factory();

            public TestClient(TProtocol protocol)
                : base(protocol, protocol)
            {
            }

            public TestClient(TTransport transport)
                : base(transport, ProtocolFactory, ProtocolFactory)
            {
            }

            public Task RunAsync(System.Func<Task> operation)
            {
                return ExecutePerCallAsync(operation, default);
            }

            public Task<TResult> RunAsync<TResult>(Func<CancellationToken, Task<TResult>> operation,
                CancellationToken cancellationToken)
            {
                return ExecutePerCallAsync(operation, cancellationToken);
            }
        }

        private static TaskCompletionSource<bool> NewSignal()
        {
            return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private static async Task<TTransportException> CaptureTransportExceptionAsync(Func<Task> operation)
        {
            try
            {
                await operation();
            }
            catch (TTransportException exception)
            {
                return exception;
            }

            throw new InvalidOperationException("Expected a transport interruption.");
        }
    }
}