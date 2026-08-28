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
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using OpenSearch.Net;
using FluentAssertions;
using OpenSearch.Client;
using Tests.Core.ManagedOpenSearch.Clusters;
using Tests.Domain.Extensions;

namespace Tests.Document.Multiple.BulkStreamAll
{
	// The _bulk/stream endpoint streams newline-delimited JSON — one {took, errors, items} object per server-side
	// batch, with batch_size defaulting to 1 — so a request for N documents comes back as N objects. These tests
	// assert the response deserializer aggregates every streamed object (not just the first), under both the default
	// Utf8Json engine and the opt-in System.Text.Json engine, and that failures on documents other than the first
	// reach the retry / dropped-document machinery.
	public class BulkStreamResponseParsingApiTests : BulkStreamAllApiTestsBase
	{
		public BulkStreamResponseParsingApiTests(IntrusiveOperationCluster cluster) : base(cluster) { }

		[U] public void AggregatesAllStreamedChunksUtf8Json() => AssertAggregatesAllChunks(useSystemTextJson: false);

		[U] public void AggregatesAllStreamedChunksSystemTextJson() => AssertAggregatesAllChunks(useSystemTextJson: true);

		private static void AssertAggregatesAllChunks(bool useSystemTextJson)
		{
			const int numberOfDocuments = 5;
			const int failIndex = 3; // a non-first document fails, so only-first-chunk parsing would miss it

			var statuses = Enumerable.Range(0, numberOfDocuments)
				.Select(i => i == failIndex ? 429 : 201)
				.ToArray();

			var client = CreateClient(NewlineDelimitedResponse(statuses), useSystemTextJson);

			var response = client.BulkStream(s => s
				.Index("bulkstreamall-parsing")
				.IndexMany(Enumerable.Range(0, numberOfDocuments).Select(i => new SmallObject { Id = i, Name = $"doc-{i}" }))
			);

			// Every streamed chunk must be represented, in wire order.
			response.Items.Should().HaveCount(numberOfDocuments, "every streamed batch object must be aggregated");
			response.Items.Select(i => i.Status).Should().Equal(statuses);
			response.Items[failIndex].IsValid.Should().BeFalse();
			response.Errors.Should().BeTrue("a failed item sets errors on its chunk and that must be OR-ed across chunks");
		}

		[U] public void NonFirstDocumentFailureReachesDroppedCallbackUtf8Json() => AssertNonFirstFailureIsRouted(useSystemTextJson: false);

		[U] public void NonFirstDocumentFailureReachesDroppedCallbackSystemTextJson() => AssertNonFirstFailureIsRouted(useSystemTextJson: true);

		// Reproduces the reviewer's scenario: a non-retryable failure on a document other than the first must reach
		// DroppedDocumentCallback. Before the aggregation fix only document 0 was inspected, so this never fired.
		private static void AssertNonFirstFailureIsRouted(bool useSystemTextJson)
		{
			const int numberOfDocuments = 5;
			const int failIndex = 2;

			var statuses = Enumerable.Range(0, numberOfDocuments)
				.Select(i => i == failIndex ? 400 : 201) // 400 is not retryable under the default predicate, so it drops
				.ToArray();

			var client = CreateClient(NewlineDelimitedResponse(statuses), useSystemTextJson);

			var documents = Enumerable.Range(0, numberOfDocuments).Select(i => new SmallObject { Id = i, Name = $"doc-{i}" });
			var dropped = new List<int>();

			var observable = client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(1)
				.Size(numberOfDocuments) // a single buffer -> a single request that streams every document back
				.Index("bulkstreamall-parsing")
				.ContinueAfterDroppedDocuments()
				.DroppedDocumentCallback((item, doc) => dropped.Add(doc.Id))
				.BufferToBulk((r, buffer) => r.IndexMany(buffer))
			);

			observable.Wait(TimeSpan.FromSeconds(30), _ => { });

			dropped.Should().ContainSingle().Which.Should().Be(failIndex,
				"the non-retryable failure on a non-first document must reach the dropped-document callback");
		}

		private static IOpenSearchClient CreateClient(byte[] responseBody, bool useSystemTextJson)
		{
			var settings = new ConnectionSettings(
					new SingleNodeConnectionPool(new Uri("http://localhost:9200")),
					new InMemoryConnection(responseBody, 200))
				.ApplyDomainSettings()
				.UseSystemTextJson(useSystemTextJson);

			return new OpenSearchClient(settings);
		}

		// Builds a _bulk/stream response body: one {took, errors, items:[{index:{...}}]} object per status, joined by
		// newlines, mirroring how the server streams one object per batch with batch_size = 1.
		private static byte[] NewlineDelimitedResponse(int[] statuses)
		{
			var sb = new StringBuilder();
			for (var i = 0; i < statuses.Length; i++)
			{
				var status = statuses[i];
				var failed = status < 200 || status >= 300;
				var error = failed
					? ",\"error\":{\"type\":\"test_failure\",\"reason\":\"injected failure\"}"
					: ",\"result\":\"created\"";

				sb.Append('{')
					.Append("\"took\":1,")
					.Append("\"errors\":").Append(failed ? "true" : "false").Append(',')
					.Append("\"items\":[{\"index\":{")
					.Append("\"_index\":\"bulkstreamall-parsing\",")
					.Append("\"_id\":\"").Append(i).Append("\",")
					.Append("\"_version\":1,")
					.Append("\"status\":").Append(status)
					.Append(error)
					.Append("}}]}")
					.Append('\n');
			}

			return Encoding.UTF8.GetBytes(sb.ToString());
		}
	}
}
