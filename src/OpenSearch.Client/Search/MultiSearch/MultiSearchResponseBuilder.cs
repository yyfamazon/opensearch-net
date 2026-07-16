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
	internal class MultiSearchResponseBuilder : CustomResponseBuilderBase
	{
		private static readonly MethodInfo CreateSearchResponseStjMethod =
			typeof(MultiSearchResponseBuilder).GetMethod(nameof(CreateSearchResponseStj), BindingFlags.Static | BindingFlags.NonPublic);

		public MultiSearchResponseBuilder(IRequest request)
		{
			Formatter = new MultiSearchResponseFormatter(request);
			Request = request;
		}

		private MultiSearchResponseFormatter Formatter { get; }
		private IRequest Request { get; }

		public override object DeserializeResponse(IOpenSearchSerializer builtInSerializer, IApiCallDetails response, Stream stream)
		{
			if (!response.Success)
				return new MultiSearchResponse();

			// Try legacy Utf8Json path first
			if (builtInSerializer is IInternalSerializer internalSerializer
				&& internalSerializer.TryGetJsonFormatter(out _))
			{
				return builtInSerializer.CreateStateful(Formatter).Deserialize<MultiSearchResponse>(stream);
			}

			// System.Text.Json path
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
				return new MultiSearchResponse();

			// Try legacy Utf8Json path first
			if (builtInSerializer is IInternalSerializer internalSerializer
				&& internalSerializer.TryGetJsonFormatter(out _))
			{
				return await builtInSerializer.CreateStateful(Formatter)
					.DeserializeAsync<MultiSearchResponse>(stream, ctx)
					.ConfigureAwait(false);
			}

			// System.Text.Json path
			return DeserializeWithStj(builtInSerializer, stream);
		}

		private object DeserializeWithStj(IOpenSearchSerializer serializer, Stream stream)
		{
			var response = new MultiSearchResponse();

			// Parse the full JSON response
			using var jsonDoc = JsonDocument.Parse(stream);
			var root = jsonDoc.RootElement;

			if (root.TryGetProperty("took", out var tookElement) && tookElement.ValueKind == JsonValueKind.Number)
				response.Took = tookElement.GetInt64();

			if (!root.TryGetProperty("responses", out var responsesElement) || responsesElement.ValueKind != JsonValueKind.Array)
				return response;

			var responses = new List<JsonElement>();
			foreach (var item in responsesElement.EnumerateArray())
				responses.Add(item);

			// Get the operations from the request to determine types
			IEnumerable<(JsonElement Doc, string Key, Type ClrType)> withMeta;
			switch (Request)
			{
				case IMultiSearchRequest multiSearch:
					withMeta = responses.Zip(multiSearch.Operations,
						(doc, desc) => (Doc: doc, Key: desc.Key, ClrType: ((ITypedSearchRequest)desc.Value).ClrType ?? typeof(object)));
					break;
				case IMultiSearchTemplateRequest multiSearchTemplate:
					withMeta = responses.Zip(multiSearchTemplate.Operations,
						(doc, desc) => (Doc: doc, Key: desc.Key, ClrType: ((ITypedSearchRequest)desc.Value).ClrType ?? typeof(object)));
					break;
				default:
					return response;
			}

			foreach (var (doc, key, clrType) in withMeta)
			{
				var method = CreateSearchResponseStjMethod.MakeGenericMethod(clrType);
				method.Invoke(null, new object[] { doc, key, serializer, response.Responses });
			}

			return response;
		}

		private static void CreateSearchResponseStj<T>(
			JsonElement docElement,
			string key,
			IOpenSearchSerializer serializer,
			IDictionary<string, IResponse> collection)
			where T : class
		{
			var rawBytes = System.Text.Encoding.UTF8.GetBytes(docElement.GetRawText());
			using var ms = new MemoryStream(rawBytes);
			var searchResponse = serializer.Deserialize<SearchResponse<T>>(ms);
			if (searchResponse != null)
				collection[key] = searchResponse;
		}
	}
}
