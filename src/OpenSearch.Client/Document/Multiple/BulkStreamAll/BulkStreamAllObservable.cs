/* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using OpenSearch.Net;

namespace OpenSearch.Client
{
	internal static class BulkStreamAllDefaults
	{
		public const int MaxRetriesDefault = 3;
		public const int MaxDegreeOfParallelismDefault = 4;
		public const int SizeDefault = 1000;

		// How many batches a single affinity worker may have accepted but not yet completed (one dispatching plus
		// the rest queued) before the producer must wait on that worker. Bounds memory while giving the producer
		// enough runway to keep feeding the other workers when one worker is slow or retrying.
		public const int AffinityWorkerQueueDepthDefault = 2;
	}

	public class BulkStreamAllObservable<T> : IDisposable, IObservable<BulkStreamAllResponse> where T : class
	{
		private readonly int _bulkSize;
		private readonly IOpenSearchClient _client;
		private readonly IBulkStreamAllRequest<T> _request;
		private readonly int _maxDegreeOfParallelism;
		private readonly Func<T, string> _affinityKeySelector;

		private readonly CancellationToken _compositeCancelToken;
		private readonly CancellationTokenSource _compositeCancelTokenSource;

		private Action _incrementFailed = () => { };
		private Action _incrementRetries = () => { };
		private Action<long> _addDocumentsProcessed = _ => { };

		public BulkStreamAllObservable(
			IOpenSearchClient client,
			IBulkStreamAllRequest<T> request,
			CancellationToken cancellationToken = default
		)
		{
			_client = client;
			_request = request;
			// Clamp to at least 1: a Size <= 0 makes PartitionHelper loop forever on empty batches, and a
			// MaxDegreeOfParallelism <= 0 divides by zero when routing/round-robining.
			_bulkSize = Math.Max(1, request.Size ?? BulkStreamAllDefaults.SizeDefault);
			_maxDegreeOfParallelism = Math.Max(1, request.MaxDegreeOfParallelism ?? BulkStreamAllDefaults.MaxDegreeOfParallelismDefault);
			_affinityKeySelector = request.DocumentAffinityKey;
			_compositeCancelTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			_compositeCancelToken = _compositeCancelTokenSource.Token;
		}

		public void Dispose()
		{
			_compositeCancelTokenSource?.Cancel();
			_compositeCancelTokenSource?.Dispose();
		}

		public IDisposable Subscribe(IObserver<BulkStreamAllResponse> observer)
		{
			observer.ThrowIfNull(nameof(observer));
			BulkStreamAll(observer);
			return this;
		}

		public IDisposable Subscribe(BulkStreamAllObserver observer)
		{
			_incrementFailed = observer.IncrementTotalNumberOfFailedBuffers;
			_incrementRetries = observer.IncrementTotalNumberOfRetries;
			_addDocumentsProcessed = observer.AddDocumentsProcessed;
			return Subscribe((IObserver<BulkStreamAllResponse>)observer);
		}

		private void BulkStreamAll(IObserver<BulkStreamAllResponse> observer)
		{
#pragma warning disable 4014
			RunAsync(observer);
#pragma warning restore 4014
		}

		private async Task RunAsync(IObserver<BulkStreamAllResponse> observer)
		{
			var observerLock = new object();
			Exception dispatchException = null;

			async Task DispatchAsync(IList<T> batch, long page, int workerIndex)
			{
				try
				{
					var result = await BulkAsync(batch, page, 0, workerIndex).ConfigureAwait(false);
					if (result != null)
					{
						_addDocumentsProcessed(batch.Count);
						lock (observerLock)
							observer.OnNext(result);
					}
				}
				catch (Exception ex)
				{
					// Record the first failure and cancel so the producer and any in-flight batches stop promptly.
					Interlocked.CompareExchange(ref dispatchException, ex, null);
					try { _compositeCancelTokenSource.Cancel(); }
					catch (ObjectDisposedException) { /* already disposed; nothing left to cancel */ }
					throw;
				}
			}

			try
			{
				if (_affinityKeySelector != null)
					await RunWithAffinityAsync(DispatchAsync).ConfigureAwait(false);
				else
					await RunRoundRobinAsync(DispatchAsync).ConfigureAwait(false);

				OnCompleted(null, observer);
			}
			catch (Exception ex)
			{
				OnCompleted(dispatchException ?? ex, observer);
			}
		}

		// Round-robin dispatch: batches run concurrently up to MaxDegreeOfParallelism and are scheduled by
		// whichever finishes first (as BulkAllObservable does via ForEachAsync). Without a DocumentAffinityKey
		// there is no ordering to protect, so a slow or retrying batch only occupies one of the N in-flight
		// slots instead of stalling a whole worker while the others sit idle.
		private async Task RunRoundRobinAsync(Func<IList<T>, long, int, Task> dispatch)
		{
			var inFlight = new List<Task>();
			var partitioned = new PartitionHelper<T>(_request.Documents, _bulkSize);
			long page = 0;

			try
			{
				foreach (var batch in partitioned)
				{
					_compositeCancelToken.ThrowIfCancellationRequested();

					// Reap finished dispatches (propagating any failure) so the list can't grow unbounded when
					// back pressure holds the in-flight count below MaxDegreeOfParallelism.
					for (var i = inFlight.Count - 1; i >= 0; i--)
					{
						if (!inFlight[i].IsCompleted) continue;
						var finished = inFlight[i];
						inFlight.RemoveAt(i);
						await finished.ConfigureAwait(false);
					}

					// Cap concurrency at MaxDegreeOfParallelism, freeing a slot as soon as any batch completes.
					while (inFlight.Count >= _maxDegreeOfParallelism)
					{
						var completed = await Task.WhenAny(inFlight).ConfigureAwait(false);
						inFlight.Remove(completed);
						await completed.ConfigureAwait(false);
					}

					// Throttle the producer against consumer progress when back pressure is configured.
					if (_request.BackPressure != null)
						await _request.BackPressure.WaitAsync(_compositeCancelToken).ConfigureAwait(false);

					inFlight.Add(dispatch(batch, page, (int)(page % _maxDegreeOfParallelism)));
					page++;
				}

				await Task.WhenAll(inFlight).ConfigureAwait(false);
			}
			catch
			{
				// Observe every scheduled dispatch so faulted batches never surface as unobserved task exceptions.
				try { await Task.WhenAll(inFlight).ConfigureAwait(false); }
				catch { /* the originating failure is surfaced by the caller */ }
				throw;
			}
		}

		// Affinity dispatch: hash each document's key to a worker so operations sharing a key always land on the
		// same worker, and give every worker its own bounded queue drained by a dedicated consumer. Each consumer
		// dispatches its batches strictly FIFO with at most one in flight, preserving on-the-wire ordering for a
		// key. Crucially the producer only ever waits on the *one* worker whose queue is full — a batch that is
		// slow or backing off holds up only its own worker, so the remaining workers keep draining instead of the
		// whole ingestion stalling behind it (the same guarantee the round-robin path already provides).
		private async Task RunWithAffinityAsync(Func<IList<T>, long, int, Task> dispatch)
		{
			var buffers = new List<T>[_maxDegreeOfParallelism];
			var pageCounters = new long[_maxDegreeOfParallelism];
			var queues = new AffinityWorkerQueue[_maxDegreeOfParallelism];
			var consumers = new Task[_maxDegreeOfParallelism];

			for (var i = 0; i < _maxDegreeOfParallelism; i++)
			{
				buffers[i] = new List<T>(_bulkSize);
				queues[i] = new AffinityWorkerQueue(BulkStreamAllDefaults.AffinityWorkerQueueDepthDefault);
				var workerIndex = i;
				consumers[i] = ConsumeWorkerAsync(queues[workerIndex], dispatch, workerIndex);
			}

			try
			{
				try
				{
					foreach (var document in _request.Documents)
					{
						_compositeCancelToken.ThrowIfCancellationRequested();

						var key = _affinityKeySelector(document);
						var workerIndex = (int)((uint)GetStableHashCode(key) % (uint)_maxDegreeOfParallelism);
						buffers[workerIndex].Add(document);

						if (buffers[workerIndex].Count < _bulkSize) continue;

						var batch = new List<T>(buffers[workerIndex]);
						buffers[workerIndex].Clear();
						await queues[workerIndex].EnqueueAsync(batch, pageCounters[workerIndex]++, _compositeCancelToken).ConfigureAwait(false);
					}

					// Flush any partially-filled buffers, then signal each worker that no more batches are coming.
					for (var i = 0; i < _maxDegreeOfParallelism; i++)
					{
						if (buffers[i].Count > 0)
							await queues[i].EnqueueAsync(buffers[i], pageCounters[i]++, _compositeCancelToken).ConfigureAwait(false);
						queues[i].Complete();
					}

					await Task.WhenAll(consumers).ConfigureAwait(false);
				}
				catch
				{
					// Unblock any worker still waiting for input, then observe every consumer so a faulted batch
					// never surfaces as an unobserved task exception.
					for (var i = 0; i < _maxDegreeOfParallelism; i++)
						queues[i].Complete();
					try { await Task.WhenAll(consumers).ConfigureAwait(false); }
					catch { /* the originating failure is surfaced by the caller */ }
					throw;
				}
			}
			finally
			{
				for (var i = 0; i < _maxDegreeOfParallelism; i++)
					queues[i].Dispose();
			}
		}

		// Drains one worker's queue, dispatching each batch to completion before taking the next so that at most
		// one batch per worker is ever in flight and batches complete in the order they were enqueued.
		private async Task ConsumeWorkerAsync(AffinityWorkerQueue queue, Func<IList<T>, long, int, Task> dispatch, int workerIndex)
		{
			while (true)
			{
				var next = await queue.DequeueAsync(_compositeCancelToken).ConfigureAwait(false);
				if (next == null) break; // queue completed and drained

				if (_request.BackPressure != null)
					await _request.BackPressure.WaitAsync(_compositeCancelToken).ConfigureAwait(false);

				try
				{
					await dispatch(next.Value.Batch, next.Value.Page, workerIndex).ConfigureAwait(false);
				}
				finally
				{
					// Free the worker's slot only once the batch is fully done (including retries), so a slow or
					// backing-off batch counts against this worker's depth and eventually backpressures the
					// producer for this key alone — never for the other workers.
					queue.ReleaseSlot();
				}
			}
		}

		// A bounded single-consumer FIFO queue of pending batches for one affinity worker. EnqueueAsync blocks the
		// producer only when this worker already holds `capacity` unfinished batches (queued plus in flight);
		// DequeueAsync yields batches in order and returns null once the queue has been completed and fully drained.
		// ReleaseSlot frees a slot when a batch finishes.
		private sealed class AffinityWorkerQueue : IDisposable
		{
			private readonly Queue<(IList<T> Batch, long Page)> _items = new Queue<(IList<T>, long)>();
			private readonly SemaphoreSlim _itemAvailable = new SemaphoreSlim(0);
			private readonly SemaphoreSlim _freeCapacity;
			private readonly object _gate = new object();
			private bool _completed;

			public AffinityWorkerQueue(int capacity) => _freeCapacity = new SemaphoreSlim(capacity, capacity);

			public async Task EnqueueAsync(IList<T> batch, long page, CancellationToken cancellationToken)
			{
				await _freeCapacity.WaitAsync(cancellationToken).ConfigureAwait(false);
				lock (_gate)
					_items.Enqueue((batch, page));
				_itemAvailable.Release();
			}

			public async Task<(IList<T> Batch, long Page)?> DequeueAsync(CancellationToken cancellationToken)
			{
				await _itemAvailable.WaitAsync(cancellationToken).ConfigureAwait(false);
				lock (_gate)
				{
					if (_items.Count == 0)
						return null; // woken by Complete() with nothing left to hand out
					return _items.Dequeue();
				}
			}

			// Returns a slot to the producer once a dequeued batch has finished processing.
			public void ReleaseSlot() => _freeCapacity.Release();

			public void Complete()
			{
				lock (_gate)
				{
					if (_completed) return;
					_completed = true;
				}
				_itemAvailable.Release(); // wake the consumer so it can observe completion once drained
			}

			public void Dispose()
			{
				_itemAvailable.Dispose();
				_freeCapacity.Dispose();
			}
		}

		private void OnCompleted(Exception exception, IObserver<BulkStreamAllResponse> observer)
		{
			if (exception != null)
				observer.OnError(exception);
			else
			{
				try
				{
					RefreshOnCompleted();
					observer.OnCompleted();
				}
				catch (Exception e)
				{
					observer.OnError(e);
				}
			}
		}

		private void RefreshOnCompleted()
		{
			if (!_request.RefreshOnCompleted) return;

			var indices = _request.RefreshIndices ?? _request.Index;
			if (indices == null) return;

			var refresh = _client.Indices.Refresh(indices, r => r.RequestConfiguration(rc =>
			{
				switch (_request)
				{
					case IHelperCallable helperCallable when helperCallable.ParentMetaData is object:
						rc.RequestMetaData(helperCallable.ParentMetaData);
						break;
					default:
						rc.RequestMetaData(RequestMetaDataFactory.BulkHelperRequestMetaData());
						break;
				}
				return rc;
			}));

			if (!refresh.IsValid)
				throw Throw("Refreshing after all documents have indexed failed", refresh.ApiCall);
		}

		private async Task<BulkStreamAllResponse> BulkAsync(IList<T> buffer, long page, int attempt, int workerIndex)
		{
			_compositeCancelToken.ThrowIfCancellationRequested();

			var response = await _client.BulkStreamAsync(s =>
			{
				s.Index(_request.Index);
				s.Timeout(_request.Timeout);

				if (_request.BufferToBulk != null)
					_request.BufferToBulk(s, buffer);
				else
					s.IndexMany(buffer);

				if (!string.IsNullOrEmpty(_request.Pipeline)) s.Pipeline(_request.Pipeline);
				if (_request.Routing != null) s.Routing(_request.Routing);
				if (_request.WaitForActiveShards.HasValue) s.WaitForActiveShards(_request.WaitForActiveShards.ToString());

				switch (_request)
				{
					case IHelperCallable helperCallable when helperCallable.ParentMetaData is object:
						s.RequestConfiguration(rc => rc.RequestMetaData(helperCallable.ParentMetaData));
						break;
					default:
						s.RequestConfiguration(rc => rc.RequestMetaData(RequestMetaDataFactory.BulkHelperRequestMetaData()));
						break;
				}

				return s;
			}, _compositeCancelToken).ConfigureAwait(false);

			_compositeCancelToken.ThrowIfCancellationRequested();
			_request.BulkResponseCallback?.Invoke(response);

			if (!response.ApiCall.Success)
				return await HandleBulkFailure(buffer, page, attempt, workerIndex, response).ConfigureAwait(false);

			var retryableDocuments = new List<T>();
			var droppedDocuments = new List<Tuple<BulkResponseItemBase, T>>();
			var retryPredicate = _request.RetryDocumentPredicate ?? DefaultRetryPredicate;
			var droppedCallback = _request.DroppedDocumentCallback ?? DefaultDroppedCallback;

			foreach (var documentWithResponse in response.Items.Zip(buffer, Tuple.Create))
			{
				if (documentWithResponse.Item1.IsValid) continue;

				if (retryPredicate(documentWithResponse.Item1, documentWithResponse.Item2))
					retryableDocuments.Add(documentWithResponse.Item2);
				else
					droppedDocuments.Add(documentWithResponse);
			}

			HandleDroppedDocuments(droppedDocuments, droppedCallback, response);

			var maxRetries = _request.MaxRetries ?? BulkStreamAllDefaults.MaxRetriesDefault;

			if (retryableDocuments.Count > 0 && attempt < maxRetries)
				return await RetryDocuments(page, attempt + 1, retryableDocuments, workerIndex).ConfigureAwait(false);

			if (retryableDocuments.Count > 0)
				throw ThrowOnBadBulk(response, $"Bulk indexing failed after retrying {attempt} times");

			_request.BackPressure?.Release();

			return new BulkStreamAllResponse
			{
				Page = page,
				WorkerIndex = workerIndex,
				Retries = attempt,
				Items = response.Items,
				Took = response.Took
			};
		}

		private async Task<BulkStreamAllResponse> HandleBulkFailure(
			IList<T> buffer, long page, int attempt, int workerIndex, BulkStreamResponse response)
		{
			var maxRetries = _request.MaxRetries ?? BulkStreamAllDefaults.MaxRetriesDefault;
			var clientException = response.ApiCall.OriginalException as OpenSearchClientException;
			var failureReason = clientException?.FailureReason;
			var reason = failureReason?.GetStringValue() ?? nameof(PipelineFailure.BadRequest);

			switch (failureReason)
			{
				case PipelineFailure.MaxRetriesReached:
					if (response.ApiCall.AuditTrail.Last().Event == AuditEvent.FailedOverAllNodes)
						throw ThrowOnBadBulk(response, $"{nameof(BulkStreamAllObservable<T>)} halted after attempted bulk failed over all the active nodes");

					ThrowOnExhaustedRetries();
					return await RetryDocuments(page, attempt + 1, buffer, workerIndex).ConfigureAwait(false);
				case PipelineFailure.CouldNotStartSniffOnStartup:
				case PipelineFailure.BadAuthentication:
				case PipelineFailure.NoNodesAttempted:
				case PipelineFailure.SniffFailure:
				case PipelineFailure.Unexpected:
					throw ThrowOnBadBulk(response,
						$"{nameof(BulkStreamAllObservable<T>)} halted after {nameof(PipelineFailure)}.{reason} from _bulk/stream");
				case PipelineFailure.BadResponse:
				case PipelineFailure.PingFailure:
				case PipelineFailure.MaxTimeoutReached:
				case PipelineFailure.BadRequest:
				default:
					ThrowOnExhaustedRetries();
					return await RetryDocuments(page, attempt + 1, buffer, workerIndex).ConfigureAwait(false);
			}

			void ThrowOnExhaustedRetries()
			{
				if (attempt >= maxRetries)
					throw ThrowOnBadBulk(response,
						$"{nameof(BulkStreamAllObservable<T>)} halted after {nameof(PipelineFailure)}.{reason} from _bulk/stream and exhausting retries ({attempt})");
			}
		}

		private void HandleDroppedDocuments(
			List<Tuple<BulkResponseItemBase, T>> droppedDocuments,
			Action<BulkResponseItemBase, T> droppedCallback,
			BulkStreamResponse response)
		{
			if (droppedDocuments.Count <= 0) return;

			foreach (var dropped in droppedDocuments)
				droppedCallback(dropped.Item1, dropped.Item2);

			if (!_request.ContinueAfterDroppedDocuments)
				throw ThrowOnBadBulk(response, $"{nameof(BulkStreamAllObservable<T>)} halted after receiving failures that can not be retried from _bulk/stream");
		}

		private async Task<BulkStreamAllResponse> RetryDocuments(long page, int attempt, IList<T> retryDocuments, int workerIndex)
		{
			_incrementRetries();
			var baseDelay = _request.RetryBaseDelay ?? RetryStrategy.DefaultBaseDelay;
			var maxDelay = _request.RetryMaxDelay ?? RetryStrategy.DefaultMaxDelay;
			var delay = RetryStrategy.ComputeDelay(attempt - 1, baseDelay, maxDelay);
			await Task.Delay(delay, _compositeCancelToken).ConfigureAwait(false);
			return await BulkAsync(retryDocuments, page, attempt, workerIndex).ConfigureAwait(false);
		}

		private Exception ThrowOnBadBulk(IOpenSearchResponse response, string message)
		{
			_incrementFailed();
			_request.BackPressure?.Release();
			return Throw(message, response.ApiCall);
		}

		private static OpenSearchClientException Throw(string message, IApiCallDetails details) =>
			new OpenSearchClientException(PipelineFailure.BadResponse, message, details);

		private static bool DefaultRetryPredicate(BulkResponseItemBase bulkResponseItem, T d) => bulkResponseItem.Status == 429;

		private static void DefaultDroppedCallback(BulkResponseItemBase bulkResponseItem, T d) { }

		/// <summary>
		/// A stable hash code implementation that is consistent across different .NET runtimes.
		/// (string.GetHashCode() is randomized in .NET Core).
		/// </summary>
		private static int GetStableHashCode(string str)
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
	}
}
