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

using System.Net.Http;
using System.Net.Http.Headers;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Transport.Client;

namespace Thrift.Tests.Transports
{
    [TestClass]
    public class THttpTransportTests
    {
        [TestMethod]
        public void THttpTransport_Uses_Configured_ConnectionTimeout_Test()
        {
            var client = new HttpClient();
            var httpClientTransport = new THttpTransport(client, null)
            {
                ConnectTimeout = 5000
            };

            Assert.IsTrue(client.Timeout.TotalMilliseconds == 5000);
            Assert.IsTrue(httpClientTransport.ConnectTimeout == 5000);
        }

        [TestMethod]
        public void THttpTransport_Preserves_Provided_HttpClient_Timeout()
        {
            var timeout = TimeSpan.FromSeconds(7);
            using var client = new HttpClient { Timeout = timeout };
            using var transport = new THttpTransport(client, null);

            Assert.AreEqual(timeout, client.Timeout);
            Assert.AreEqual(timeout, transport.PerCallTimeout);
            Assert.AreEqual((int)timeout.TotalMilliseconds, transport.ConnectTimeout);
        }

        [TestMethod]
        public void THttpTransport_Preserves_Infinite_Provided_HttpClient_Timeout()
        {
            using var client = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
            using var transport = new THttpTransport(client, null);

            Assert.AreEqual(Timeout.InfiniteTimeSpan, client.Timeout);
            Assert.AreEqual(Timeout.InfiniteTimeSpan, transport.PerCallTimeout);
        }

        [TestMethod]
        public async Task THttpTransport_PerCallTransport_Preserves_ContentType()
        {
            using var client = new HttpClient();
            using var transport = new THttpTransport(client, null)
            {
                ContentType = new MediaTypeHeaderValue("application/custom-thrift")
            };

            using var callTransport = (THttpTransport)await transport.CreatePerCallTransportAsync(default);

            Assert.AreEqual(transport.ContentType, callTransport.ContentType);
        }
    }
}
