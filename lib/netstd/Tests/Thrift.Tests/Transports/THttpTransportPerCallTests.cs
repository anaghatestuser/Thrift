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
using System.Buffers.Binary;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Thrift.Tests.Transports
{
    [TestClass]
    public class THttpTransportPerCallTests
    {
        [TestMethod]
        public Task Interleaved_http_calls_keep_request_and_response_state_isolated()
        {
            return RunInterleavedCallsAsync(useBufferedTransport: false);
        }

        [TestMethod]
        public Task Buffered_transport_read_ahead_stays_with_its_call()
        {
            return RunInterleavedCallsAsync(useBufferedTransport: true);
        }

        [TestMethod]
        public Task Framed_http_calls_keep_frame_buffers_per_call()
        {
            return RunInterleavedCallsAsync(useBufferedTransport: false, useFramedTransport: true);
        }

        [TestMethod]
        public Task Buffered_over_framed_http_calls_keep_each_layer_per_call()
        {
            return RunInterleavedCallsAsync(useBufferedTransport: true, useFramedTransport: true);
        }

        [TestMethod]
        public async Task Successful_call_disposes_its_response_content()
        {
            var content = new TrackingContent(new byte[] { 30, 31 });
            var handler = new RequestHandler((_, _) =>
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
            });
            using var httpClient = new HttpClient(handler);
            using var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"));
            using var client = new TestClient(rootTransport);

            var result = await SendAndReceiveAsync(client, 30, CancellationToken.None);

            CollectionAssert.AreEqual(new byte[] { 30, 31 }, result);
            Assert.IsTrue(content.IsDisposed);
        }

        [TestMethod]
        public async Task Failed_call_releases_resources_and_leaves_sibling_usable()
        {
            var handler = new RequestHandler((payload, _) =>
            {
                if (payload == 40)
                {
                    throw new HttpRequestException("Expected request failure.");
                }
                return Task.FromResult(Response(payload));
            });
            using var httpClient = new HttpClient(handler);
            using var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"));
            using var client = new TestClient(rootTransport);

            var exception = await CaptureExceptionAsync<TTransportException>(
                () => SendAndReceiveAsync(client, 40, CancellationToken.None));
            Assert.AreEqual(TTransportException.ExceptionType.Unknown, exception.Type);

            var result = await SendAndReceiveAsync(client, 41, CancellationToken.None);
            CollectionAssert.AreEqual(new byte[] { 41, 42 }, result);
        }

        [TestMethod]
        public async Task Caller_cancellation_interrupts_only_its_call()
        {
            var started = NewSignal();
            var handler = new RequestHandler(async (payload, cancellationToken) =>
            {
                if (payload == 50)
                {
                    started.TrySetResult(true);
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                return Response(payload);
            });
            using var httpClient = new HttpClient(handler);
            using var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"));
            using var client = new TestClient(rootTransport);
            using var cancellation = new CancellationTokenSource();

            var interruptedCall = SendAndReceiveAsync(client, 50, cancellation.Token);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var siblingResult = await SendAndReceiveAsync(client, 51, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            cancellation.Cancel();

            var exception = await CaptureExceptionAsync<TTransportException>(() => interruptedCall);
            Assert.AreEqual(TTransportException.ExceptionType.Interrupted, exception.Type);
            CollectionAssert.AreEqual(new byte[] { 51, 52 }, siblingResult);
        }

        [TestMethod]
        public async Task Configured_timeout_interrupts_request_without_canceling_sibling()
        {
            var started = NewSignal();
            var handler = new RequestHandler(async (payload, cancellationToken) =>
            {
                if (payload == 60)
                {
                    started.TrySetResult(true);
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                return Response(payload);
            });
            using var httpClient = new HttpClient(handler);
            using var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"))
            {
                ConnectTimeout = 1000
            };
            using var client = new TestClient(rootTransport);

            var timedOutCall = SendAndReceiveAsync(client, 60, CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var siblingResult = await SendAndReceiveAsync(client, 61, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));

            var exception = await CaptureExceptionAsync<TTransportException>(
                () => timedOutCall.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.AreEqual(TTransportException.ExceptionType.Interrupted, exception.Type);
            CollectionAssert.AreEqual(new byte[] { 61, 62 }, siblingResult);
        }

        [TestMethod]
        public async Task Root_disposal_waits_for_active_per_call_transport()
        {
            var started = NewSignal();
            var release = NewSignal();
            var handler = new RequestHandler(async (payload, cancellationToken) =>
            {
                started.TrySetResult(true);
                await release.Task.WaitAsync(cancellationToken);
                return Response(payload);
            });
            var httpClient = new HttpClient(handler);
            var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"));
            var client = new TestClient(rootTransport);

            var call = SendAndReceiveAsync(client, 70, CancellationToken.None);
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
            rootTransport.Dispose();
            release.TrySetResult(true);

            var result = await call.WaitAsync(TimeSpan.FromSeconds(5));
            CollectionAssert.AreEqual(new byte[] { 70, 71 }, result);
            client.Dispose();
            await CaptureExceptionAsync<ObjectDisposedException>(
                () => httpClient.GetAsync("http://localhost"));
            httpClient.Dispose();
        }

        private static async Task RunInterleavedCallsAsync(bool useBufferedTransport, bool useFramedTransport = false)
        {
            var handler = new InterleavingHandler(useFramedTransport);
            using var httpClient = new HttpClient(handler);
            using var rootTransport = new THttpTransport(httpClient, null, new Uri("http://localhost"));
            TTransport transport = useFramedTransport ? new TFramedTransport(rootTransport) : rootTransport;
            if (useBufferedTransport)
            {
                transport = new TBufferedTransport(transport);
            }
            using var client = new TestClient(transport);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            try
            {
                var firstCall = SendAndReceiveAsync(client, 10, timeout.Token);
                await handler.FirstRequestStarted.WaitAsync(timeout.Token);
                var secondCall = SendAndReceiveAsync(client, 20, timeout.Token);

                var results = await Task.WhenAll(firstCall, secondCall).WaitAsync(timeout.Token);
                CollectionAssert.AreEqual(new byte[] { 10, 11 }, results[0]);
                CollectionAssert.AreEqual(new byte[] { 20, 21 }, results[1]);
            }
            finally
            {
                handler.ReleaseFirstRequest();
            }
        }

        private static Task<byte[]> SendAndReceiveAsync(TestClient client, byte requestByte,
            CancellationToken cancellationToken)
        {
            return client.RunAsync(async () =>
            {
                var request = new[] { requestByte };
                var outputTransport = client.OutputProtocol.Transport;
                await outputTransport.WriteAsync(request, 0, request.Length, cancellationToken);
                await outputTransport.FlushAsync(cancellationToken);

                var response = new byte[2];
                await client.InputProtocol.Transport.ReadAllAsync(response, 0, response.Length, cancellationToken);
                return response;
            }, cancellationToken);
        }

        private static HttpResponseMessage Response(byte payload)
        {
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(new[] { payload, (byte)(payload + 1) })
            };
        }

        private static TaskCompletionSource<bool> NewSignal()
        {
            return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private static async Task<TException> CaptureExceptionAsync<TException>(Func<Task> operation)
            where TException : Exception
        {
            try
            {
                await operation();
            }
            catch (TException exception)
            {
                return exception;
            }

            throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
        }

        private sealed class RequestHandler : HttpMessageHandler
        {
            private readonly Func<byte, CancellationToken, Task<HttpResponseMessage>> _responseFactory;

            public RequestHandler(Func<byte, CancellationToken, Task<HttpResponseMessage>> responseFactory)
            {
                _responseFactory = responseFactory;
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                var content = request.Content ?? throw new InvalidOperationException("The request content was missing.");
                var body = await content.ReadAsByteArrayAsync(cancellationToken);
                if (body.Length == 0)
                {
                    throw new InvalidOperationException("The request body was empty.");
                }
                return await _responseFactory(body[0], cancellationToken);
            }
        }

        private sealed class TrackingContent : ByteArrayContent
        {
            public TrackingContent(byte[] content)
                : base(content)
            {
            }

            public bool IsDisposed { get; private set; }

            protected override void Dispose(bool disposing)
            {
                IsDisposed = true;
                base.Dispose(disposing);
            }
        }

        private sealed class InterleavingHandler : HttpMessageHandler
        {
            private readonly TaskCompletionSource<bool> _firstRequestStarted =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<bool> _releaseFirstRequest =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly bool _useFramedTransport;
            private int _requestCount;

            public InterleavingHandler(bool useFramedTransport)
            {
                _useFramedTransport = useFramedTransport;
            }

            public Task FirstRequestStarted => _firstRequestStarted.Task;

            public void ReleaseFirstRequest()
            {
                _releaseFirstRequest.TrySetResult(true);
            }

            protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                var requestNumber = Interlocked.Increment(ref _requestCount);
                if (requestNumber == 1)
                {
                    _firstRequestStarted.TrySetResult(true);
                    await _releaseFirstRequest.Task.WaitAsync(cancellationToken);
                }

                byte[] requestBody;
                try
                {
                    var content = request.Content ?? throw new InvalidOperationException("The request content was missing.");
                    requestBody = await content.ReadAsByteArrayAsync(cancellationToken);
                }
                finally
                {
                    if (requestNumber == 2)
                    {
                        _releaseFirstRequest.TrySetResult(true);
                    }
                }

                var payloadOffset = 0;
                if (_useFramedTransport)
                {
                    if (requestBody.Length < sizeof(int))
                    {
                        throw new InvalidOperationException("The framed request was shorter than its header.");
                    }

                    var frameLength = BinaryPrimitives.ReadInt32BigEndian(requestBody.AsSpan(0, sizeof(int)));
                    if (frameLength != requestBody.Length - sizeof(int))
                    {
                        throw new InvalidOperationException("The framed request length did not match its payload.");
                    }

                    payloadOffset = sizeof(int);
                }

                var requestValue = requestBody[payloadOffset];
                var responsePayload = new[] { requestValue, (byte)(requestValue + 1) };
                if (_useFramedTransport)
                {
                    var framedResponse = new byte[sizeof(int) + responsePayload.Length];
                    BinaryPrimitives.WriteInt32BigEndian(framedResponse.AsSpan(0, sizeof(int)), responsePayload.Length);
                    Buffer.BlockCopy(responsePayload, 0, framedResponse, sizeof(int), responsePayload.Length);
                    responsePayload = framedResponse;
                }

                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(responsePayload)
                };
            }
        }

        private sealed class TestClient : TBaseClient, IDisposable
        {
            private static readonly TProtocolFactory ProtocolFactory = new TBinaryProtocol.Factory();

            public TestClient(TTransport transport)
                : base(transport, ProtocolFactory, ProtocolFactory)
            {
            }

            public Task<TResult> RunAsync<TResult>(Func<Task<TResult>> operation,
                CancellationToken cancellationToken)
            {
                return ExecutePerCallAsync(operation, cancellationToken);
            }
        }
    }
}