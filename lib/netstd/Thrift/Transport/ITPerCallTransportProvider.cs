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

using System.Threading;
using System.Threading.Tasks;
using System;

namespace Thrift.Transport
{
    /// <summary>
    /// Provides a mechanism to create a separate transport for each client call.
    /// The caller owns each returned transport and must dispose it when the call completes.
    /// </summary>
    public interface ITPerCallTransportProvider
    {
        /// <summary>
        /// Gets whether this transport can create an independent transport for each call.
        /// </summary>
        bool SupportsPerCallTransport { get; }

        /// <summary>
        /// Gets the maximum duration for a complete per-call operation, or <see cref="Timeout.InfiniteTimeSpan"/> if unbounded.
        /// </summary>
        TimeSpan PerCallTimeout { get; }

        /// <summary>
        /// Creates a new transport for the current client call.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel transport creation.</param>
        /// <returns>A new transport owned by the caller.</returns>
        Task<TTransport> CreatePerCallTransportAsync(CancellationToken cancellationToken);
    }
}