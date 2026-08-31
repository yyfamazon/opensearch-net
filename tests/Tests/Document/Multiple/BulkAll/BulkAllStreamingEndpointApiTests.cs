/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using OpenSearch.Net;
using FluentAssertions;
using OpenSearch.Client;
using Tests.Domain.Extensions;

namespace Tests.Document.Multiple.BulkAll
{
	// BulkAll(...).UseStreamingEndpoint() routes each batch to _bulk/stream and aggregates its newline-delimited
	// response across all chunks (one {took,errors,items} object per server-side batch). Runs under both serializer
	// engines, exercising the retained low-level _bulk/stream request converter and response builder via the public API.
	public class BulkAllStreamingEndpointApiTests
	{
		private class SmallObject
		{
			public int Id { get; set; }
			public string Name { get; set; }
		}

		[U] public void RoutesToBulkStreamEndpointUtf8Json() => AssertRoutesToStreamEndpoint(useSystemTextJson: false);

		[U] public void RoutesToBulkStreamEndpointSystemTextJson() => AssertRoutesToStreamEndpoint(useSystemTextJson: true);

		private static void AssertRoutesToStreamEndpoint(bool useSystemTextJson)
		{
			var connection = new FixedNdjsonConnection(new[] { 201, 201, 201 });
			var run = RunStreaming(connection, documents: 3, useSystemTextJson);

			run.Error.Should().BeNull();
			connection.LastPath.Should().EndWith("_bulk/stream", "UseStreamingEndpoint() must dispatch to _bulk/stream");
			run.Observer.TotalDocumentsProcessed.Should().Be(3);
		}

		[U] public void AggregatesAllStreamedChunksUtf8Json() => AssertAggregatesChunks(useSystemTextJson: false);

		[U] public void AggregatesAllStreamedChunksSystemTextJson() => AssertAggregatesChunks(useSystemTextJson: true);

		// Guards the original bug: a non-first document failing across separate NDJSON chunks must still be seen — here it
		// is a non-retryable 400 that must reach the dropped-document callback rather than being lost with chunks 1..N-1.
		private static void AssertAggregatesChunks(bool useSystemTextJson)
		{
			const int failingId = 3;
			var statuses = Enumerable.Range(0, 5).Select(i => i == failingId ? 400 : 201).ToArray();
			var connection = new FixedNdjsonConnection(statuses);

			var dropped = new List<int>();
			var run = RunStreaming(connection, documents: 5, useSystemTextJson,
				configure: f => f.ContinueAfterDroppedDocuments().DroppedDocumentCallback((item, doc) => dropped.Add(doc.Id)));

			run.Error.Should().BeNull();
			dropped.Should().ContainSingle().Which.Should().Be(failingId,
				"the failure on a non-first document, in a later NDJSON chunk, must be aggregated and routed");
		}

		private readonly struct RunResult
		{
			public RunResult(BulkAllObserver observer, Exception error)
			{
				Observer = observer;
				Error = error;
			}

			public BulkAllObserver Observer { get; }
			public Exception Error { get; }
		}

		private static RunResult RunStreaming(
			IConnection connection, int documents, bool useSystemTextJson, Action<BulkAllDescriptor<SmallObject>> configure = null)
		{
			var settings = new ConnectionSettings(new SingleNodeConnectionPool(new Uri("http://localhost:9200")), connection)
				.ApplyDomainSettings()
				.UseSystemTextJson(useSystemTextJson);
			var client = new OpenSearchClient(settings);

			var docs = Enumerable.Range(0, documents).Select(i => new SmallObject { Id = i, Name = $"doc-{i}" });
			Exception error = null;
			var handle = new ManualResetEventSlim(false);
			var observer = new BulkAllObserver(
				onNext: _ => { },
				onError: e => { error = e; handle.Set(); },
				onCompleted: () => handle.Set());

			var observable = client.BulkAll(docs, f =>
			{
				f.MaxDegreeOfParallelism(1)
					.Size(documents == 0 ? 1 : documents) // a single batch that streams every document back
					.Index("bulkall-streaming")
					.UseStreamingEndpoint();
				configure?.Invoke(f);
				return f;
			});

			observable.Subscribe(observer);
			handle.Wait(TimeSpan.FromSeconds(30)).Should().BeTrue("the run must finish within the timeout");
			return new RunResult(observer, error);
		}

		// Returns a fixed newline-delimited _bulk/stream response (one {took,errors,items:[one item]} object per status)
		// and records the request path so the test can confirm the streaming endpoint was used.
		private sealed class FixedNdjsonConnection : InMemoryConnection
		{
			private readonly byte[] _body;

			public FixedNdjsonConnection(int[] statuses)
				: base(Array.Empty<byte>(), 200, null, RequestData.MimeType) =>
				_body = BuildNdjson(statuses);

			public string LastPath { get; private set; }

			public override System.Threading.Tasks.Task<TResponse> RequestAsync<TResponse>(RequestData requestData, CancellationToken cancellationToken)
			{
				LastPath = requestData.Uri.AbsolutePath;
				return ReturnConnectionStatusAsync<TResponse>(requestData, cancellationToken, _body, 200);
			}

			public override TResponse Request<TResponse>(RequestData requestData)
			{
				LastPath = requestData.Uri.AbsolutePath;
				return ReturnConnectionStatus<TResponse>(requestData, _body, 200);
			}

			private static byte[] BuildNdjson(int[] statuses)
			{
				var sb = new StringBuilder();
				for (var i = 0; i < statuses.Length; i++)
				{
					var status = statuses[i];
					var failed = status < 200 || status >= 300;
					sb.Append("{\"took\":1,\"errors\":").Append(failed ? "true" : "false")
						.Append(",\"items\":[{\"index\":{")
						.Append("\"_index\":\"bulkall-streaming\",\"_id\":\"").Append(i).Append("\",\"_version\":1,\"status\":").Append(status)
						.Append(failed
							? ",\"error\":{\"type\":\"test_failure\",\"reason\":\"injected failure\"}"
							: ",\"result\":\"created\"")
						.Append("}}]}\n");
				}
				return Encoding.UTF8.GetBytes(sb.ToString());
			}
		}
	}
}
