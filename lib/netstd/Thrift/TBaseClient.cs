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
using Thrift.Protocol;
using Thrift.Transport;

namespace Thrift
{
    // ReSharper disable once InconsistentNaming
    /// <summary>
    ///     TBaseClient.
    ///     Base client for generated clients.
    ///     Do not change this class without checking generated code (namings, etc.)
    /// </summary>
    public abstract class TBaseClient
    {
        private readonly TProtocol _inputProtocol;
        private readonly TProtocol _outputProtocol;
        private readonly TProtocolFactory _inputProtocolFactory;
        private readonly TProtocolFactory _outputProtocolFactory;
        private readonly ITPerCallTransportProvider _perCallTransportProvider;
        private readonly AsyncLocal<PerCallProtocols> _perCallProtocols = new AsyncLocal<PerCallProtocols>();
        private bool _isDisposed;
        private int _seqId;
        public readonly Guid ClientId = Guid.NewGuid();

        protected TBaseClient(TProtocol inputProtocol, TProtocol outputProtocol)
        {
            _inputProtocol = inputProtocol ?? throw new ArgumentNullException(nameof(inputProtocol));
            _outputProtocol = outputProtocol ?? throw new ArgumentNullException(nameof(outputProtocol));
        }

        /// <summary>
        /// Initializes a client with protocols created from the supplied transport and protocol factories.
        /// </summary>
        /// <param name="transport">The shared transport, which may provide a new transport for each call.</param>
        /// <param name="inputProtocolFactory">The factory used to create input protocols.</param>
        /// <param name="outputProtocolFactory">The factory used to create output protocols.</param>
        /// <exception cref="ArgumentNullException">A required constructor argument is null.</exception>
        /// <exception cref="InvalidOperationException">A protocol factory returns null.</exception>
        protected TBaseClient(TTransport transport, TProtocolFactory inputProtocolFactory,
            TProtocolFactory outputProtocolFactory)
        {
            transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _inputProtocolFactory = inputProtocolFactory ?? throw new ArgumentNullException(nameof(inputProtocolFactory));
            _outputProtocolFactory = outputProtocolFactory ?? throw new ArgumentNullException(nameof(outputProtocolFactory));
            _perCallTransportProvider = transport as ITPerCallTransportProvider;
            if (_perCallTransportProvider != null && !_perCallTransportProvider.SupportsPerCallTransport)
            {
                _perCallTransportProvider = null;
            }

            _inputProtocol = CreateProtocol(_inputProtocolFactory, transport);
            try
            {
                _outputProtocol = CreateProtocol(_outputProtocolFactory, transport);
            }
            catch
            {
                _inputProtocol.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Gets the input protocol for the current call, or the shared input protocol when no per-call transport is active.
        /// </summary>
        public TProtocol InputProtocol => _perCallProtocols.Value?.InputProtocol ?? _inputProtocol;

        /// <summary>
        /// Gets the output protocol for the current call, or the shared output protocol when no per-call transport is active.
        /// </summary>
        public TProtocol OutputProtocol => _perCallProtocols.Value?.OutputProtocol ?? _outputProtocol;

        /// <summary>
        /// Gets the next client sequence identifier.
        /// </summary>
        public int SeqId
        {
            get { return Interlocked.Increment(ref _seqId); }
        }

        /// <summary>
        /// Executes an operation with protocols scoped to a new transport when the shared transport supports per-call transports.
        /// </summary>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The token used to cancel per-call transport creation and opening.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        protected Task ExecutePerCallAsync(Func<Task> operation, CancellationToken cancellationToken)
        {
            operation = operation ?? throw new ArgumentNullException(nameof(operation));

            return ExecutePerCallAsync(new Func<CancellationToken, Task>(_ => operation()), cancellationToken);
        }

        /// <summary>
        /// Executes an operation using the cancellation token linked to the caller and per-call timeout.
        /// </summary>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The caller's cancellation token.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        protected Task ExecutePerCallAsync(Func<CancellationToken, Task> operation,
            CancellationToken cancellationToken)
        {
            return ExecutePerCallScopeAsync(operation, cancellationToken);
        }

        private async Task ExecutePerCallScopeAsync(Func<CancellationToken, Task> operation,
            CancellationToken cancellationToken)
        {
            operation = operation ?? throw new ArgumentNullException(nameof(operation));

            if (_perCallTransportProvider == null)
            {
                await operation(cancellationToken);
                return;
            }

            using (var callCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                var timeout = _perCallTransportProvider.PerCallTimeout;
                if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
                {
                    throw new InvalidOperationException("Per-call timeout must be non-negative or infinite.");
                }

                if (timeout == TimeSpan.Zero)
                {
                    callCancellation.Cancel();
                }
                else if (timeout != Timeout.InfiniteTimeSpan)
                {
                    callCancellation.CancelAfter(timeout);
                }

                await ExecutePerCallCoreAsync(operation, callCancellation.Token);
            }
        }

        private async Task ExecutePerCallCoreAsync(Func<CancellationToken, Task> operation,
            CancellationToken cancellationToken)
        {
            TTransport transport;
            try
            {
                transport = await _perCallTransportProvider.CreatePerCallTransportAsync(cancellationToken);
            }
            catch (OperationCanceledException exception)
            {
                throw new TTransportException(TTransportException.ExceptionType.Interrupted, exception.Message, exception);
            }

            transport = transport ?? throw new InvalidOperationException("The per-call transport provider returned null.");

            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;
            try
            {
                inputProtocol = CreateProtocol(_inputProtocolFactory, transport);
                outputProtocol = CreateProtocol(_outputProtocolFactory, transport);
                var previousProtocols = _perCallProtocols.Value;
                _perCallProtocols.Value = new PerCallProtocols(inputProtocol, outputProtocol);
                try
                {
                    if (!inputProtocol.Transport.IsOpen)
                    {
                        await inputProtocol.Transport.OpenAsync(cancellationToken);
                    }
                    if (!ReferenceEquals(inputProtocol.Transport, outputProtocol.Transport) && !outputProtocol.Transport.IsOpen)
                    {
                        await outputProtocol.Transport.OpenAsync(cancellationToken);
                    }
                    await operation(cancellationToken);
                }
                catch (OperationCanceledException exception)
                {
                    throw new TTransportException(TTransportException.ExceptionType.Interrupted, exception.Message, exception);
                }
                finally
                {
                    _perCallProtocols.Value = previousProtocols;
                }
            }
            finally
            {
                try
                {
                    outputProtocol?.Dispose();
                }
                finally
                {
                    try
                    {
                        if (!ReferenceEquals(inputProtocol, outputProtocol))
                        {
                            inputProtocol?.Dispose();
                        }
                    }
                    finally
                    {
                        if (inputProtocol == null && outputProtocol == null)
                        {
                            transport.Dispose();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Executes an operation with protocols scoped to a new transport when the shared transport supports per-call transports.
        /// </summary>
        /// <typeparam name="TResult">The type of result returned by the operation.</typeparam>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The token used to cancel per-call transport creation and opening.</param>
        /// <returns>A task containing the operation result.</returns>
        protected async Task<TResult> ExecutePerCallAsync<TResult>(Func<Task<TResult>> operation,
            CancellationToken cancellationToken)
        {
            operation = operation ?? throw new ArgumentNullException(nameof(operation));

            return await ExecutePerCallAsync(new Func<CancellationToken, Task<TResult>>(
                _ => operation()), cancellationToken);
        }

        /// <summary>
        /// Executes a result-producing operation using the cancellation token linked to the caller and per-call timeout.
        /// </summary>
        /// <typeparam name="TResult">The type of result returned by the operation.</typeparam>
        /// <param name="operation">The operation to execute.</param>
        /// <param name="cancellationToken">The caller's cancellation token.</param>
        /// <returns>A task containing the operation result.</returns>
        protected async Task<TResult> ExecutePerCallAsync<TResult>(
            Func<CancellationToken, Task<TResult>> operation, CancellationToken cancellationToken)
        {
            operation = operation ?? throw new ArgumentNullException(nameof(operation));

            TResult result = default(TResult);
            await ExecutePerCallScopeAsync(new Func<CancellationToken, Task>(async callCancellationToken =>
            {
                result = await operation(callCancellationToken);
            }), cancellationToken);
            return result;
        }

        public virtual async Task OpenTransportAsync()
        {
            await OpenTransportAsync(CancellationToken.None);
        }

        public virtual async Task OpenTransportAsync(CancellationToken cancellationToken)
        {
            if (!_inputProtocol.Transport.IsOpen)
            {
                await _inputProtocol.Transport.OpenAsync(cancellationToken);
            }

            if (!_outputProtocol.Transport.IsOpen)
            {
                await _outputProtocol.Transport.OpenAsync(cancellationToken);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                try
                {
                    if (disposing)
                    {
                        try
                        {
                            _inputProtocol?.Dispose();
                        }
                        finally
                        {
                            _outputProtocol?.Dispose();
                        }
                    }
                }
                finally
                {
                    _isDisposed = true;
                }
            }
        }

        private static TProtocol CreateProtocol(TProtocolFactory factory, TTransport transport)
        {
            return factory.GetProtocol(transport) ??
                throw new InvalidOperationException("A protocol factory returned null.");
        }

        private sealed class PerCallProtocols
        {
            public PerCallProtocols(TProtocol inputProtocol, TProtocol outputProtocol)
            {
                InputProtocol = inputProtocol;
                OutputProtocol = outputProtocol;
            }

            public TProtocol InputProtocol { get; }
            public TProtocol OutputProtocol { get; }
        }
    }
}
