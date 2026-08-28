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
using System.Threading;
using System.Threading.Tasks;
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using OpenSearch.Net;
using FluentAssertions;
using Newtonsoft.Json;
using OpenSearch.Client;
using Tests.Core.ManagedOpenSearch.Clusters;
using Tests.Domain.Extensions;

namespace Tests.Document.Multiple.BulkStreamAll
{
	public class BulkStreamAllAffinityStallApiTests : BulkStreamAllApiTestsBase
	{
		public BulkStreamAllAffinityStallApiTests(IntrusiveOperationCluster cluster) : base(cluster) { }

		// Regression test for the affinity path stalling all workers behind one slow/retrying worker.
		//
		// A single affinity key is routed to one worker whose bulk requests block until we release them. The
		// documents are ordered so that worker fills two batches up front, then the rest of the stream targets a
		// different worker. We only release the blocked worker once the OTHER worker has completed several batches
		// — proving it kept draining while its neighbour was stuck.
		//
		// Before the fix the single producer awaited the blocked worker's in-flight batch on its second flush and
		// froze intake for every worker, so the other worker never completed enough batches to trigger the release
		// and the run deadlocked until the timeout. With the per-worker queue fix it completes promptly.
		[U]
		public void SlowAffinityWorkerDoesNotStallOtherWorkers()
		{
			const int mdop = 2;
			const int size = 10;
			const int slowBatches = 2;   // >= 2 so the old code reaches the second-flush stall on the slow worker
			const int fastBatches = 20;  // plenty of runway for the fast worker to prove progress

			// Pick two keys that hash to different workers so the slow key monopolises exactly one worker.
			var fastKey = FindKeyForWorker("fast-", targetWorker: 0, mdop);
			var slowKey = FindKeyForWorker("slow-", targetWorker: 1, mdop);

			// Slow docs first (they fill the slow worker's two batches), then all the fast docs.
			var documents = SlowThenFastDocuments(slowKey, slowBatches * size, fastKey, fastBatches * size);

			// The slow worker's requests block on this until the fast worker has demonstrably made progress.
			var releaseSlow = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			const int fastCompletionsProvingProgress = 5;
			var fastCompleted = 0;

			var connection = new AffinityStallConnection(slowKey, releaseSlow.Task);
			var settings = new ConnectionSettings(new SingleNodeConnectionPool(new Uri("http://localhost:9200")), connection)
				.ApplyDomainSettings();
			var client = new OpenSearchClient(settings);

			var completed = new ManualResetEventSlim(false);
			Exception error = null;

			var observable = client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(mdop)
				.Size(size)
				.Index("bulkstreamall-affinity-stall")
				.DocumentAffinityKey(d => d.Name)
				.BufferToBulk((r, buffer) => r.IndexMany(buffer))
			);

			try
			{
				using (observable.Subscribe(new BulkStreamAllObserver(
					onNext: _ =>
					{
						// Only the fast worker completes before the release, so this counts fast-worker progress.
						if (Interlocked.Increment(ref fastCompleted) == fastCompletionsProvingProgress)
							releaseSlow.TrySetResult(true);
					},
					onError: e => { error = e; releaseSlow.TrySetResult(true); completed.Set(); },
					onCompleted: () => completed.Set()
				)))
				{
					// With the fix this finishes in well under the timeout; the old code deadlocks and times out.
					var finished = completed.Wait(TimeSpan.FromSeconds(30));

					finished.Should().BeTrue(
						"the fast worker must keep completing batches while the slow worker is blocked, rather than the "
						+ "producer freezing all workers behind the slow one");
					error.Should().BeNull();
					fastCompleted.Should().BeGreaterThanOrEqualTo(fastCompletionsProvingProgress);
				}
			}
			finally
			{
				// Never leave the blocked requests hanging, even if an assertion above fails.
				releaseSlow.TrySetResult(true);
			}
		}

		private static IEnumerable<SmallObject> SlowThenFastDocuments(string slowKey, int slowCount, string fastKey, int fastCount)
		{
			var id = 0;
			for (var i = 0; i < slowCount; i++)
				yield return new SmallObject { Id = id++, Name = slowKey };
			for (var i = 0; i < fastCount; i++)
				yield return new SmallObject { Id = id++, Name = fastKey };
		}

		// Finds the first key with the given prefix that the observable's affinity hashing routes to targetWorker.
		// Mirrors BulkStreamAllObservable's worker assignment so the test can pin a key to a specific worker.
		private static string FindKeyForWorker(string prefix, int targetWorker, int maxDegreeOfParallelism)
		{
			for (var i = 0; i < 10_000; i++)
			{
				var key = prefix + i;
				var worker = (int)((uint)StableHashCode(key) % (uint)maxDegreeOfParallelism);
				if (worker == targetWorker) return key;
			}

			throw new InvalidOperationException($"Could not find a '{prefix}' key mapping to worker {targetWorker}.");
		}

		// Copy of BulkStreamAllObservable.GetStableHashCode (private there) so the test routes keys identically.
		private static int StableHashCode(string str)
		{
			if (str == null) return 0;

			unchecked
			{
				var hash1 = 5381;
				var hash2 = hash1;

				for (var i = 0; i < str.Length && str[i] != '\0'; i += 2)
				{
					hash1 = ((hash1 << 5) + hash1) ^ str[i];
					if (i == str.Length - 1)
						break;
					hash2 = ((hash2 << 5) + hash2) ^ str[i + 1];
				}

				return hash1 + hash2 * 1566083941;
			}
		}

		// In-memory connection that returns a valid _bulk/stream success response, but blocks any request whose
		// body carries the designated slow affinity key until the supplied task is released.
		private sealed class AffinityStallConnection : InMemoryConnection
		{
			private readonly string _slowKey;
			private readonly Task _releaseSlow;

			public AffinityStallConnection(string slowKey, Task releaseSlow)
				: base(Array.Empty<byte>(), 200, null, RequestData.MimeType)
			{
				_slowKey = slowKey;
				_releaseSlow = releaseSlow;
			}

			public override async Task<TResponse> RequestAsync<TResponse>(RequestData requestData, CancellationToken cancellationToken)
			{
				var body = await ReadBodyAsync(requestData, cancellationToken).ConfigureAwait(false);

				// Block only the slow key's requests; everything else returns immediately.
				if (body.Contains(_slowKey))
					await _releaseSlow.ConfigureAwait(false);

				var responseBytes = BuildBulkSuccessResponse(CountDocuments(body));
				return await ReturnConnectionStatusAsync<TResponse>(requestData, cancellationToken, responseBytes, 200)
					.ConfigureAwait(false);
			}

			public override TResponse Request<TResponse>(RequestData requestData)
			{
				// The helper only ever issues async requests, but implement the sync path for completeness.
				var body = ReadBodyAsync(requestData, CancellationToken.None).GetAwaiter().GetResult();
				if (body.Contains(_slowKey))
					_releaseSlow.GetAwaiter().GetResult();
				return ReturnConnectionStatus<TResponse>(requestData, BuildBulkSuccessResponse(CountDocuments(body)), 200);
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

			// The bulk body is newline-delimited action/source pairs, so the document count is half the line count.
			private static int CountDocuments(string body)
			{
				var lines = body.Split('\n').Count(line => !string.IsNullOrWhiteSpace(line));
				return lines / 2;
			}

			private static byte[] BuildBulkSuccessResponse(int documentCount)
			{
				var items = new List<object>(documentCount);
				for (var i = 0; i < documentCount; i++)
				{
					items.Add(new
					{
						index = new
						{
							_index = "bulkstreamall-affinity-stall",
							_id = i.ToString(),
							_version = 1,
							result = "created",
							_shards = new { total = 2, successful = 1, failed = 0 },
							_seq_no = i,
							_primary_term = 1,
							status = 201
						}
					});
				}

				var response = new { took = 1, errors = false, items };
				return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(response));
			}
		}
	}
}
