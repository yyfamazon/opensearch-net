/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/
/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*
*  Licensed to Elasticsearch B.V. under one or more contributor
*  license agreements. See the NOTICE file distributed with
*  this work for additional information regarding copyright
*  ownership. Elasticsearch B.V. licenses this file to you under
*  the Apache License, Version 2.0 (the "License"); you may
*  not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
* 	http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using OpenSearch.Net;

namespace OpenSearch.Client
{
	internal class MultiGetResponseBuilder : CustomResponseBuilderBase
	{
		private static readonly MethodInfo CreateMultiHitStjMethod =
			typeof(MultiGetResponseBuilder).GetMethod(nameof(CreateMultiHitStj), BindingFlags.Static | BindingFlags.NonPublic);

		public MultiGetResponseBuilder(IMultiGetRequest request)
		{
			Formatter = new MultiGetResponseFormatter(request);
			Request = request;
		}

		private MultiGetResponseFormatter Formatter { get; }
		private IMultiGetRequest Request { get; }

		public override object DeserializeResponse(IOpenSearchSerializer builtInSerializer, IApiCallDetails response, Stream stream)
		{
			if (!response.Success)
				return new MultiGetResponse();

			// Try legacy Utf8Json path first
			if (builtInSerializer is IInternalSerializer internalSerializer
				&& internalSerializer.TryGetJsonFormatter(out _))
			{
				return builtInSerializer.CreateStateful(Formatter).Deserialize<MultiGetResponse>(stream);
			}

			// System.Text.Json path: deserialize the raw response and dispatch each doc to its typed hit
			return DeserializeWithStj(builtInSerializer, stream);
		}

		public override async Task<object> DeserializeResponseAsync(
			IOpenSearchSerializer builtInSerializer,
			IApiCallDetails response,
			Stream stream,
			CancellationToken ctx = default
		)
		{
			if (!response.Success)
				return new MultiGetResponse();

			// Try legacy Utf8Json path first
			if (builtInSerializer is IInternalSerializer internalSerializer
				&& internalSerializer.TryGetJsonFormatter(out _))
			{
				return await builtInSerializer.CreateStateful(Formatter)
					.DeserializeAsync<MultiGetResponse>(stream, ctx)
					.ConfigureAwait(false);
			}

			// System.Text.Json path
			return DeserializeWithStj(builtInSerializer, stream);
		}

		private object DeserializeWithStj(IOpenSearchSerializer serializer, Stream stream)
		{
			var response = new MultiGetResponse();
			if (Request?.Documents == null)
				return response;

			// Parse the full JSON response
			using var jsonDoc = JsonDocument.Parse(stream);
			var root = jsonDoc.RootElement;

			if (!root.TryGetProperty("docs", out var docsElement) || docsElement.ValueKind != JsonValueKind.Array)
				return response;

			var docs = new List<JsonElement>();
			foreach (var item in docsElement.EnumerateArray())
				docs.Add(item);

			// Zip docs with request descriptors to get the CLR type for each hit
			var withMeta = docs.Zip(Request.Documents,
				(doc, desc) => (Doc: doc, Descriptor: desc));

			foreach (var (doc, descriptor) in withMeta)
			{
				var clrType = descriptor.ClrType;
				var method = CreateMultiHitStjMethod.MakeGenericMethod(clrType);
				method.Invoke(null, new object[] { doc, serializer, response.InternalHits });
			}

			return response;
		}

		private static void CreateMultiHitStj<T>(
			JsonElement docElement,
			IOpenSearchSerializer serializer,
			ICollection<IMultiGetHit<object>> collection)
			where T : class
		{
			// Serialize the element back to bytes and deserialize as MultiGetHit<T>
			var rawBytes = System.Text.Encoding.UTF8.GetBytes(docElement.GetRawText());
			using var ms = new MemoryStream(rawBytes);
			var hit = serializer.Deserialize<MultiGetHit<T>>(ms);
			if (hit != null)
				collection.Add(hit);
		}
	}
}
