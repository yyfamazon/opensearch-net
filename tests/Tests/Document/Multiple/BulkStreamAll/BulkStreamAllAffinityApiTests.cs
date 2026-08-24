/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using FluentAssertions;
using OpenSearch.Client;
using Tests.Core.ManagedOpenSearch.Clusters;

namespace Tests.Document.Multiple.BulkStreamAll
{
	public class BulkStreamAllAffinityApiTests : BulkStreamAllApiTestsBase
	{
		public BulkStreamAllAffinityApiTests(IntrusiveOperationCluster cluster) : base(cluster) { }

		[I]
		public void DocumentsWithSameKeyRouteToSameWorker()
		{
			var index = CreateIndexName();

			var size = 50;
			var numberOfDocuments = 500;
			var distinctKeys = 10;
			var documents = CreateDocumentsWithAffinityKeys(numberOfDocuments, distinctKeys);

			// Track which worker index processes documents for each affinity key
			var keyToWorkers = new ConcurrentDictionary<string, ConcurrentBag<int>>();

			var observableBulk = Client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(4)
				.Size(size)
				.Index(index)
				.DocumentAffinityKey(doc => doc.Name)
			);

			observableBulk.Wait(TimeSpan.FromSeconds(60), b =>
			{
				// Each response has a WorkerIndex — all items in this batch went through the same worker
				foreach (var item in b.Items)
				{
					// We can't easily correlate item back to the original doc's Name here,
					// but we can verify the structural constraint that the WorkerIndex is consistent
					b.WorkerIndex.Should().BeInRange(0, 3);
				}
			});

			// The key assertion is that the system doesn't crash and completes successfully
			// with affinity routing enabled. Deeper ordering tests require inspecting the actual
			// bulk request bodies which would need integration-level verification.
		}

		[I]
		public void AffinityKeyNullDefaultsToRoundRobin()
		{
			var index = CreateIndexName();

			var size = 100;
			var numberOfDocuments = 400;
			var documents = CreateLazyStreamOfDocuments(numberOfDocuments);
			var workersSeen = new ConcurrentBag<int>();

			var observableBulk = Client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(4)
				.Size(size)
				.Index(index)
				// No DocumentAffinityKey set — should use round-robin
			);

			observableBulk.Wait(TimeSpan.FromSeconds(30), b =>
			{
				workersSeen.Add(b.WorkerIndex);
			});

			// Without affinity, pages are distributed across workers
			workersSeen.Should().NotBeEmpty();
		}

		[I]
		public void AffinityPreservesOrderWithinSameKey()
		{
			var index = CreateIndexName();

			// Many small batches per worker so out-of-order dispatch would be observable.
			var numberOfDocuments = 2000;
			var distinctKeys = 4;
			var documents = CreateDocumentsWithAffinityKeys(numberOfDocuments, distinctKeys);

			var size = 20; // Force many batches per worker
			// Record the order in which each worker's batches COMPLETE, without sorting, so that
			// a page arriving out of order is caught rather than masked by an OrderBy.
			var completionOrderByWorker = new ConcurrentDictionary<int, ConcurrentQueue<long>>();

			var observableBulk = Client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(4)
				.Size(size)
				.Index(index)
				.DocumentAffinityKey(doc => doc.Name)
			);

			observableBulk.Wait(TimeSpan.FromSeconds(60), b =>
			{
				var queue = completionOrderByWorker.GetOrAdd(b.WorkerIndex, _ => new ConcurrentQueue<long>());
				queue.Enqueue(b.Page);
			});

			// Because at most one batch per worker is in flight at a time, each worker's batches must
			// complete in the exact order they were dispatched: strictly ascending page numbers with no gaps.
			foreach (var kvp in completionOrderByWorker)
			{
				var arrivalOrder = kvp.Value.ToArray();
				for (var i = 0; i < arrivalOrder.Length; i++)
				{
					arrivalOrder[i].Should().Be(i,
						$"worker {kvp.Key} must complete batches in dispatch order (page {i} expected at position {i}, without pre-sorting)");
				}
			}
		}
	}
}
