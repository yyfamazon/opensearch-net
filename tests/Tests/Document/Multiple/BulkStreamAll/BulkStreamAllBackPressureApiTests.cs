/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Threading;
using OpenSearch.OpenSearch.Xunit.XunitPlumbing;
using FluentAssertions;
using OpenSearch.Client;
using Tests.Core.ManagedOpenSearch.Clusters;

namespace Tests.Document.Multiple.BulkStreamAll
{
	public class BulkStreamAllBackPressureApiTests : BulkStreamAllApiTestsBase
	{
		public BulkStreamAllBackPressureApiTests(IntrusiveOperationCluster cluster) : base(cluster) { }

		// Tracks the peak number of bulk requests in flight at once. BufferToBulk runs as a request is being
		// built (dispatch start) and BulkResponseCallback runs once its response returns (dispatch end).
		private sealed class ConcurrencyProbe
		{
			private readonly object _lock = new object();
			private int _inFlight;
			public int Peak { get; private set; }

			public void OnDispatchStart()
			{
				lock (_lock)
				{
					_inFlight++;
					if (_inFlight > Peak) Peak = _inFlight;
				}
			}

			public void OnDispatchEnd()
			{
				lock (_lock) _inFlight--;
			}
		}

		[I]
		public void BackPressureCapsInFlightRequestsOnRoundRobinPath()
		{
			var index = CreateIndexName();

			var size = 100;
			var numberOfDocuments = 1000;
			var documents = CreateLazyStreamOfDocuments(numberOfDocuments);
			var seenPages = 0;
			var probe = new ConcurrencyProbe();

			// Slots = maxConcurrency * backPressureFactor = 1. MaxDegreeOfParallelism is deliberately higher (4)
			// so that if backpressure were ignored on the round-robin path, up to 4 requests would run at once.
			var observableBulk = Client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(4)
				.BackPressure(1, 1)
				.Size(size)
				.Index(index)
				.BufferToBulk((descriptor, buffer) =>
				{
					probe.OnDispatchStart();
					descriptor.IndexMany(buffer);
				})
				.BulkResponseCallback(_ => probe.OnDispatchEnd())
			);

			var observer = observableBulk.Wait(TimeSpan.FromSeconds(60), b =>
			{
				Interlocked.Increment(ref seenPages);
			});

			seenPages.Should().Be(10); // 1000 / 100
			observer.TotalNumberOfFailedBuffers.Should().Be(0);
			probe.Peak.Should().BeLessThanOrEqualTo(1, "backpressure must throttle in-flight requests to the configured slot count");
		}

		[I]
		public void BackPressureCapsInFlightRequestsWithAffinity()
		{
			var index = CreateIndexName();

			var size = 50;
			var numberOfDocuments = 500;
			var distinctKeys = 5;
			var documents = CreateDocumentsWithAffinityKeys(numberOfDocuments, distinctKeys);
			var seenPages = 0;
			var probe = new ConcurrencyProbe();

			var observableBulk = Client.BulkStreamAll(documents, f => f
				.MaxDegreeOfParallelism(4)
				.BackPressure(1, 1)
				.Size(size)
				.Index(index)
				.DocumentAffinityKey(doc => doc.Name)
				.BufferToBulk((descriptor, buffer) =>
				{
					probe.OnDispatchStart();
					descriptor.IndexMany(buffer);
				})
				.BulkResponseCallback(_ => probe.OnDispatchEnd())
			);

			var observer = observableBulk.Wait(TimeSpan.FromSeconds(60), b =>
			{
				Interlocked.Increment(ref seenPages);
			});

			seenPages.Should().BeGreaterThan(0);
			observer.TotalNumberOfFailedBuffers.Should().Be(0);
			probe.Peak.Should().BeLessThanOrEqualTo(1, "backpressure must throttle in-flight requests on the affinity path too");
		}
	}
}
