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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable IDE0079  // unneeded suppression -> all except net8
#pragma warning disable IDE0301  // simplify collection init -> net8 only
#pragma warning disable IDE0305  // simplify collection init -> net8 only

namespace Thrift.Transport.Client
{
    // ReSharper disable once InconsistentNaming
    public class THttpTransport : TEndpointTransport, ITPerCallTransportProvider
    {
        private readonly X509Certificate[] _certificates;
        private readonly Uri _uri;

        private int _connectTimeout = 30000; // Timeouts in milliseconds
        private HttpClient _httpClient;
        private readonly HttpClientLease _httpClientLease;
        private Stream _inputStream;
        private MemoryStream _outputStream = new MemoryStream();
        private HttpResponseMessage _responseMessage;
        private bool _isDisposed;
        private int _leaseReleased;

        public THttpTransport(Uri uri, TConfiguration config, IDictionary<string, string> customRequestHeaders = null, string userAgent = null)
            : this(uri, config, Enumerable.Empty<X509Certificate>(), customRequestHeaders, userAgent)
        {
        }

        public THttpTransport(Uri uri, TConfiguration config, IEnumerable<X509Certificate> certificates,
            IDictionary<string, string> customRequestHeaders, string userAgent = null)
            : base(config)
        {
            _uri = uri;
            _certificates = (certificates ?? Enumerable.Empty<X509Certificate>()).ToArray();

            if (!string.IsNullOrEmpty(userAgent))
                UserAgent = userAgent;

            // due to current bug with performance of Dispose in netcore https://github.com/dotnet/corefx/issues/8809
            // this can be switched to default way (create client->use->dispose per flush) later
            _httpClient = CreateClient(customRequestHeaders);
            ConfigureClient(_httpClient);
            _httpClientLease = new HttpClientLease(_httpClient);
        }

        /// <summary>
        /// Constructor that takes a <c>HttpClient</c> instance to support using <c>IHttpClientFactory</c>.
        /// </summary>
        /// <remarks>As the <c>HttpMessageHandler</c> of the client must be configured at the time of creation, it
        /// is assumed that the consumer has already added any certificates and configured decompression methods. The
        /// consumer can use the <c>CreateHttpClientHandler</c> method to get a handler with these set.</remarks>
        /// <param name="httpClient">Client configured with the desired message handler, user agent, and URI if not
        /// specified in the <c>uri</c> parameter. A default user agent will be used if not set.</param>
        /// <param name="config">Thrift configuration object</param>
        /// <param name="uri">Optional URI to use for requests, if not specified the base address of <c>httpClient</c>
        /// is used.</param>
        public THttpTransport(HttpClient httpClient, TConfiguration config, Uri uri = null)
            : base(config)
        {
            _httpClient = httpClient;

            _uri = uri ?? httpClient.BaseAddress;
            httpClient.BaseAddress = _uri;

            var userAgent = _httpClient.DefaultRequestHeaders.UserAgent.ToString();
            if (!string.IsNullOrEmpty(userAgent))
                UserAgent = userAgent;

            ConfigureClient(_httpClient, configureTimeout: false);
            _httpClientLease = new HttpClientLease(_httpClient);
        }

        private THttpTransport(HttpClientLease httpClientLease, TConfiguration config, Uri uri)
            : base(config)
        {
            _httpClientLease = httpClientLease;
            _httpClient = httpClientLease.Client;
            _uri = uri;
            _connectTimeout = (int)_httpClient.Timeout.TotalMilliseconds;
            UserAgent = _httpClient.DefaultRequestHeaders.UserAgent.ToString();
        }

        // According to RFC 2616 section 3.8, the "User-Agent" header may not carry a version number
        public readonly string UserAgent = "Thrift netstd THttpClient";

        public int ConnectTimeout
        {
            set
            {
                _connectTimeout = value;
                if(_httpClient != null)
                    _httpClient.Timeout = TimeSpan.FromMilliseconds(_connectTimeout);
            }
            get
            {
                if (_httpClient == null)
                    return _connectTimeout;
                return (int)_httpClient.Timeout.TotalMilliseconds;
            }
        }

        public override bool IsOpen => true;

        public HttpRequestHeaders RequestHeaders => _httpClient.DefaultRequestHeaders;

        public MediaTypeHeaderValue ContentType { get; set; }

        /// <summary>
        /// Gets the configured timeout for a complete client call using this transport.
        /// </summary>
        public TimeSpan PerCallTimeout => _httpClient?.Timeout ?? TimeSpan.FromMilliseconds(_connectTimeout);

        /// <summary>
        /// Gets whether this transport can create an independent transport for each call.
        /// </summary>
        public bool SupportsPerCallTransport => true;

        /// <summary>
        /// Creates a transport with independent request and response buffers for one client call.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel transport creation.</param>
        /// <returns>A new transport owned by the caller.</returns>
        public Task<TTransport> CreatePerCallTransportAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_isDisposed || _httpClient == null)
            {
                throw new ObjectDisposedException(nameof(THttpTransport));
            }

            _httpClientLease.AddReference();
            try
            {
                return Task.FromResult<TTransport>(new THttpTransport(_httpClientLease, Configuration, _uri)
                {
                    ContentType = ContentType
                });
            }
            catch
            {
                _httpClientLease.Release();
                throw;
            }
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public override void Close()
        {
            DisposeResources();
        }

        public override async ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_inputStream == null)
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "No request has been sent");

            CheckReadBytesAvailable(length);

            try
            {
#if NET5_0_OR_GREATER
                var ret = await _inputStream.ReadAsync(new Memory<byte>(buffer, offset, length), cancellationToken);
#else
                var ret = await _inputStream.ReadAsync(buffer, offset, length, cancellationToken);
#endif
                if (ret == -1)
                {
                    throw new TTransportException(TTransportException.ExceptionType.EndOfFile, "No more data available");
                }

                CountConsumedMessageBytes(ret);
                return ret;
            }
            catch (IOException iox)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown, iox.ToString(), iox);
            }
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

#if NET5_0_OR_GREATER
            await _outputStream.WriteAsync(buffer.AsMemory(offset, length), cancellationToken);
#else
            await _outputStream.WriteAsync(buffer, offset, length, cancellationToken);
#endif
        }

        /// <summary>
        /// Get a client handler configured with recommended properties to use with the <c>HttpClient</c> constructor
        /// and an <c>IHttpClientFactory</c>.
        /// </summary>
        /// <param name="certificates">An optional array of client certificates to associate with the handler.</param>
        /// <returns>
        /// A client handler with deflate and gZip compression-decompression algorithms and any client
        /// certificates passed in via <c>certificates</c>.
        /// </returns>
        public virtual HttpClientHandler CreateHttpClientHandler(X509Certificate[] certificates = null)
        {
            var handler = new HttpClientHandler();
            if (certificates != null)
                handler.ClientCertificates.AddRange(certificates);
            handler.AutomaticDecompression = System.Net.DecompressionMethods.Deflate | System.Net.DecompressionMethods.GZip;
            return handler;
        }

        private HttpClient CreateClient(IDictionary<string, string> customRequestHeaders)
        {
            var handler = CreateHttpClientHandler(_certificates);
            var httpClient = new HttpClient(handler);


            if (customRequestHeaders != null)
            {
                foreach (var item in customRequestHeaders)
                {
                    httpClient.DefaultRequestHeaders.Add(item.Key, item.Value);
                }
            }

            return httpClient;
        }

        private void ConfigureClient(HttpClient httpClient, bool configureTimeout = true)
        {
            if (configureTimeout && _connectTimeout > 0)
            {
                httpClient.Timeout = TimeSpan.FromMilliseconds(_connectTimeout);
            }

            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/x-thrift"));

            // Clear any user agent values to avoid drift with the field value
            httpClient.DefaultRequestHeaders.UserAgent.Clear();
            httpClient.DefaultRequestHeaders.UserAgent.TryParseAdd(UserAgent);

            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            try
            {
                _outputStream.Seek(0, SeekOrigin.Begin);

                using (var contentStream = new StreamContent(_outputStream))
                {
                    contentStream.Headers.ContentType = ContentType ?? new MediaTypeHeaderValue(@"application/x-thrift");

                    var response = await _httpClient.PostAsync(_uri, contentStream, cancellationToken);
                    _inputStream?.Dispose();
                    _responseMessage?.Dispose();
                    _responseMessage = response;
                    response.EnsureSuccessStatusCode();
#if NET5_0_OR_GREATER
                    _inputStream = await _responseMessage.Content.ReadAsStreamAsync(cancellationToken);
#else
                    _inputStream = await _responseMessage.Content.ReadAsStreamAsync();
#endif
                    if (_inputStream.CanSeek)
                    {
                        _inputStream.Seek(0, SeekOrigin.Begin);
                    }
                }
            }
            catch (IOException iox)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown, iox.ToString(), iox);
            }
            catch (HttpRequestException wx)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown,
                    "Couldn't connect to server: " + wx, wx);
            }
            catch (OperationCanceledException ocx)
            {
                throw new TTransportException(TTransportException.ExceptionType.Interrupted, ocx.Message, ocx);
            }
            catch (Exception ex)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown, ex.Message, ex);
            }
            finally
            {
                _outputStream = new MemoryStream();
                ResetMessageSizeAndConsumedBytes();
            }
        }


        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
                DisposeResources();
        }

        private void DisposeResources()
        {
            if (_isDisposed)
            {
                return;
            }

            _isDisposed = true;
            try
            {
                _inputStream?.Dispose();
            }
            finally
            {
                try
                {
                    _outputStream?.Dispose();
                }
                finally
                {
                    try
                    {
                        _responseMessage?.Dispose();
                    }
                    finally
                    {
                        _inputStream = null;
                        _outputStream = null;
                        _responseMessage = null;
                        _httpClient = null;
                        if (Interlocked.Exchange(ref _leaseReleased, 1) == 0)
                        {
                            _httpClientLease.Release();
                        }
                    }
                }
            }
        }

        private sealed class HttpClientLease
        {
            private readonly object _gate = new object();
            private int _referenceCount = 1;
            private bool _isDisposed;

            public HttpClientLease(HttpClient client)
            {
                Client = client ?? throw new ArgumentNullException(nameof(client));
            }

            public HttpClient Client { get; }

            public void AddReference()
            {
                lock (_gate)
                {
                    if (_isDisposed)
                    {
                        throw new ObjectDisposedException(nameof(HttpClientLease));
                    }
                    _referenceCount++;
                }
            }

            public void Release()
            {
                var disposeClient = false;
                lock (_gate)
                {
                    if (_referenceCount == 0)
                    {
                        return;
                    }
                    _referenceCount--;
                    if (_referenceCount == 0)
                    {
                        _isDisposed = true;
                        disposeClient = true;
                    }
                }

                if (disposeClient)
                {
                    Client.Dispose();
                }
            }
        }
    }
}
