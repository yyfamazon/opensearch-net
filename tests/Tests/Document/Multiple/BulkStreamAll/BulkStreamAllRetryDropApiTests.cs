/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using OpenSearch.Net;
using FluentAssertions;
using OpenSearch.Client;
using Tests.Core.ManagedOpenSearch.Clusters;
using Tests.Domain.Extensions;

namespace Tests.Document.Multiple.BulkStreamAll
{
	// Live coverage for the retry / dropped-document partition logic in BulkStreamAllObservable.BulkAsync, driven by a
	// programmable in-memory connection that returns per-request outcomes and records exactly which documents were sent.
	public class BulkStreamAllRetryDropApiTests : BulkStreamAllApiTestsBase
	{
		public BulkStreamAllRetryDropApiTests(IntrusiveOperationCluster cluster) : base(cluster) { }

		[U]
		public void PartialFailureRetriesOnlyTheFailingDocument()
		{
			const int failingId = 2;

			// First attempt: only doc-2 returns 429. The retry (any later request) succeeds.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => ordinal == 0 && id == failingId ? 429 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 5, size: 5, maxDegreeOfParallelism: 1, maxRetries: 3);

			run.Error.Should().BeNull();
			run.Observer.TotalNumberOfRetries.Should().Be(1, "one partial failure means exactly one retry");
			connection.RequestCount.Should().Be(2, "the initial batch plus a single retry");
			connection.RequestedDocIds[1].Should().Equal(new[] { failingId },
				"only the failing document may be re-sent, not the whole batch");
		}

		[U]
		public void PersistentFailureExhaustsRetriesThenThrows()
		{
			const int maxRetries = 2;

			// doc-2 fails with 429 on every attempt.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => id == 2 ? 429 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 5, size: 5, maxDegreeOfParallelism: 1, maxRetries: maxRetries);

			run.Error.Should().BeOfType<OpenSearchClientException>();
			run.Error.Message.Should().Contain("after retrying");
			connection.RequestCount.Should().Be(maxRetries + 1, "initial attempt plus MaxRetries retries");
			run.Observer.TotalNumberOfFailedBuffers.Should().Be(1);
		}

		[U]
		public void ContinueAfterDroppedDocumentsFalseHaltsAndDispatchesNoFurtherBatches()
		{
			// doc-0 (in the first batch) fails with a non-retryable 400.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => id == 0 ? 400 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 4, size: 2, maxDegreeOfParallelism: 1, maxRetries: 3,
				continueAfterDroppedDocuments: false);

			run.Error.Should().NotBeNull();
			run.Error.Message.Should().Contain("halted");
			connection.RequestBodies.Should().NotContain(body => body.Contains("\"doc-2\""),
				"the second batch must never be dispatched once the first halts on a dropped document");
		}

		[U]
		public void RetryPreservesAffinityOrdering()
		{
			// Same affinity key => both batches run on one worker. Batch 0 (page 0) hits a 429 and backs off; batch 1
			// (page 1) must still complete after it.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => ordinal == 0 && id == 0 ? 429 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 4, size: 2, maxDegreeOfParallelism: 4, maxRetries: 3,
				affinityKey: "same-key");

			run.Error.Should().BeNull();
			run.Observer.TotalNumberOfRetries.Should().Be(1);
			run.Pages.Should().Equal(new long[] { 0, 1 },
				"batches sharing an affinity key must complete in dispatch order even when an earlier batch retries");
		}

		[U]
		public void CustomRetryPredicateRetriesAnOtherwiseNonRetryableStatus()
		{
			const int failingId = 1;

			// doc-1 returns 400 on the first attempt (not retryable by default), then succeeds.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => ordinal == 0 && id == failingId ? 400 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 4, size: 4, maxDegreeOfParallelism: 1, maxRetries: 3,
				configure: f => f.RetryDocumentPredicate((item, doc) => item.Status == 400));

			run.Error.Should().BeNull();
			run.Observer.TotalNumberOfRetries.Should().Be(1, "the custom predicate makes the 400 retryable");
			connection.RequestedDocIds[1].Should().Equal(new[] { failingId });
		}

		[U]
		public void CustomRetryPredicateSuppressesRetryForOtherwiseRetryableStatus()
		{
			const int failingId = 1;
			var droppedIds = new List<int>();

			// doc-1 always returns 429 (retryable by default), but the predicate refuses to retry it.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => id == failingId ? 429 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 4, size: 4, maxDegreeOfParallelism: 1, maxRetries: 3,
				configure: f => f
					.RetryDocumentPredicate((item, doc) => false)
					.DroppedDocumentCallback((item, doc) => droppedIds.Add(doc.Id)));

			run.Error.Should().BeNull();
			run.Observer.TotalNumberOfRetries.Should().Be(0, "the predicate suppresses all retries");
			connection.RequestCount.Should().Be(1, "a suppressed retry must not re-send the batch");
			droppedIds.Should().ContainSingle().Which.Should().Be(failingId);
		}

		[U]
		public void DroppedDocumentCallbackReceivesTheFailingItemAndDocument()
		{
			const int failingId = 3;
			BulkResponseItemBase droppedItem = null;
			SmallObject droppedDocument = null;

			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => id == failingId ? 400 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 5, size: 5, maxDegreeOfParallelism: 1, maxRetries: 3,
				configure: f => f.DroppedDocumentCallback((item, doc) =>
				{
					droppedItem = item;
					droppedDocument = doc;
				}));

			run.Error.Should().BeNull();
			droppedDocument.Should().NotBeNull();
			droppedDocument.Id.Should().Be(failingId, "the callback must receive the document that failed");
			droppedItem.Status.Should().Be(400, "the callback must receive that document's response item");
		}

		[U]
		public void ContinueAfterDroppedDocumentsAllowsSubsequentBatches()
		{
			// doc-0 (first batch) is dropped, but ContinueAfterDroppedDocuments (the default) lets the run proceed.
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) =>
				ids.Select(id => id == 0 ? 400 : 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 4, size: 2, maxDegreeOfParallelism: 1, maxRetries: 3);

			run.Error.Should().BeNull();
			run.Pages.Should().Contain(new long[] { 0, 1 }, "both batches must complete despite the dropped document");
			connection.RequestBodies.Should().Contain(body => body.Contains("\"doc-2\""),
				"the second batch must still be dispatched");
		}

		[U]
		public void BulkResponseCallbackIsInvokedForEachStreamedResponse()
		{
			var callbackCount = 0;
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) => ids.Select(_ => 201).ToArray());

			// 6 documents at size 2 => 3 batches, all succeeding => 3 responses.
			var run = RunBulkStreamAll(connection, documents: 6, size: 2, maxDegreeOfParallelism: 1, maxRetries: 3,
				configure: f => f.BulkResponseCallback(_ => Interlocked.Increment(ref callbackCount)));

			run.Error.Should().BeNull();
			callbackCount.Should().Be(3, "the callback fires once per dispatched bulk request");
		}

		[U]
		public void TracksTotalDocumentsProcessed()
		{
			const int documents = 6;
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) => ids.Select(_ => 201).ToArray());

			var run = RunBulkStreamAll(connection, documents, size: 2, maxDegreeOfParallelism: 1, maxRetries: 3);

			run.Error.Should().BeNull();
			run.Observer.TotalDocumentsProcessed.Should().Be(documents);
		}

		[U]
		public void EmptyDocumentStreamCompletesWithoutRequests()
		{
			var connection = new ProgrammableBulkStreamConnection((ordinal, ids) => ids.Select(_ => 201).ToArray());

			var run = RunBulkStreamAll(connection, documents: 0, size: 10, maxDegreeOfParallelism: 4, maxRetries: 3);

			run.Error.Should().BeNull();
			run.Pages.Should().BeEmpty();
			connection.RequestCount.Should().Be(0, "an empty stream must not issue any request");
		}

		private readonly struct RunResult
		{
			public RunResult(BulkStreamAllObserver observer, Exception error, List<long> pages)
			{
				Observer = observer;
				Error = error;
				Pages = pages;
			}

			public BulkStreamAllObserver Observer { get; }
			public Exception Error { get; }
			public List<long> Pages { get; }
		}

		private static RunResult RunBulkStreamAll(
			IConnection connection, int documents, int size, int maxDegreeOfParallelism, int maxRetries,
			bool continueAfterDroppedDocuments = true, string affinityKey = null,
			Action<BulkStreamAllDescriptor<SmallObject>> configure = null)
		{
			var settings = new ConnectionSettings(
					new SingleNodeConnectionPool(new Uri("http://localhost:9200")), connection)
				.ApplyDomainSettings();
			var client = new OpenSearchClient(settings);

			var docs = Enumerable.Range(0, documents).Select(i => new SmallObject { Id = i, Name = $"doc-{i}" });
			var pages = new List<long>();
			Exception error = null;
			var handle = new ManualResetEventSlim(false);

			var observer = new BulkStreamAllObserver(
				onNext: r => { lock (pages) pages.Add(r.Page); },
				onError: e => { error = e; handle.Set(); },
				onCompleted: () => handle.Set());

			var observable = client.BulkStreamAll(docs, f =>
			{
				f.MaxDegreeOfParallelism(maxDegreeOfParallelism)
					.Size(size)
					.Index("bulkstreamall-retrydrop")
					.MaxRetries(maxRetries)
					.RetryBaseDelay(TimeSpan.FromMilliseconds(1))
					.RetryMaxDelay(TimeSpan.FromMilliseconds(5))
					.ContinueAfterDroppedDocuments(continueAfterDroppedDocuments)
					.BufferToBulk((r, buffer) => r.IndexMany(buffer));
				if (affinityKey != null) f.DocumentAffinityKey(_ => affinityKey);
				configure?.Invoke(f);
				return f;
			});

			observable.Subscribe(observer);
			handle.Wait(TimeSpan.FromSeconds(30)).Should().BeTrue("the run must finish within the timeout");

			return new RunResult(observer, error, pages);
		}

		// In-memory connection that maps each request to a per-document status via a caller-supplied resolver
		// (ordinal, docIdsInRequestOrder) => status[], streams one NDJSON object per document, and records the
		// documents seen on each request so tests can assert exactly what was (re-)sent.
		private sealed class ProgrammableBulkStreamConnection : InMemoryConnection
		{
			private static readonly Regex DocIdPattern = new Regex("doc-(\\d+)", RegexOptions.Compiled);

			private readonly Func<int, IReadOnlyList<int>, int[]> _statusResolver;
			private readonly object _gate = new object();
			private readonly List<string> _requestBodies = new List<string>();
			private readonly List<IReadOnlyList<int>> _requestedDocIds = new List<IReadOnlyList<int>>();

			public ProgrammableBulkStreamConnection(Func<int, IReadOnlyList<int>, int[]> statusResolver)
				: base(Array.Empty<byte>(), 200, null, RequestData.MimeType) =>
				_statusResolver = statusResolver;

			public IReadOnlyList<string> RequestBodies
			{
				get { lock (_gate) return _requestBodies.ToArray(); }
			}

			public IReadOnlyList<IReadOnlyList<int>> RequestedDocIds
			{
				get { lock (_gate) return _requestedDocIds.ToArray(); }
			}

			public int RequestCount
			{
				get { lock (_gate) return _requestBodies.Count; }
			}

			public override async Task<TResponse> RequestAsync<TResponse>(RequestData requestData, CancellationToken cancellationToken)
			{
				var body = await ReadBodyAsync(requestData, cancellationToken).ConfigureAwait(false);
				var responseBytes = Handle(body);
				return await ReturnConnectionStatusAsync<TResponse>(requestData, cancellationToken, responseBytes, 200).ConfigureAwait(false);
			}

			public override TResponse Request<TResponse>(RequestData requestData)
			{
				var body = ReadBodyAsync(requestData, CancellationToken.None).GetAwaiter().GetResult();
				return ReturnConnectionStatus<TResponse>(requestData, Handle(body), 200);
			}

			private byte[] Handle(string body)
			{
				var ids = ParseDocIds(body);
				int ordinal;
				lock (_gate)
				{
					ordinal = _requestBodies.Count;
					_requestBodies.Add(body);
					_requestedDocIds.Add(ids);
				}

				var statuses = _statusResolver(ordinal, ids);
				return BuildNewlineDelimitedResponse(ids, statuses);
			}

			private static IReadOnlyList<int> ParseDocIds(string body)
			{
				var ids = new List<int>();
				foreach (Match match in DocIdPattern.Matches(body))
					ids.Add(int.Parse(match.Groups[1].Value));
				return ids;
			}

			private static async Task<string> ReadBodyAsync(RequestData requestData, CancellationToken cancellationToken)
			{
				if (requestData.PostData == null) return string.Empty;

				using (var stream = requestData.MemoryStreamFactory.Create())
				{
					await requestData.PostData.WriteAsync(stream, requestData.ConnectionSettings, cancellationToken).ConfigureAwait(false);
					return Encoding.UTF8.GetString(stream.ToArray());
				}
			}

			private static byte[] BuildNewlineDelimitedResponse(IReadOnlyList<int> ids, int[] statuses)
			{
				var sb = new StringBuilder();
				for (var i = 0; i < ids.Count; i++)
				{
					var status = i < statuses.Length ? statuses[i] : 201;
					var failed = status < 200 || status >= 300;

					sb.Append("{\"took\":1,\"errors\":").Append(failed ? "true" : "false")
						.Append(",\"items\":[{\"index\":{")
						.Append("\"_index\":\"bulkstreamall-retrydrop\",")
						.Append("\"_id\":\"").Append(ids[i]).Append("\",")
						.Append("\"_version\":1,")
						.Append("\"status\":").Append(status)
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
