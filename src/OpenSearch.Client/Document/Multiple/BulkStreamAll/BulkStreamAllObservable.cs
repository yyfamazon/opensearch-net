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
			_bulkSize = request.Size ?? BulkStreamAllDefaults.SizeDefault;
			_maxDegreeOfParallelism = request.MaxDegreeOfParallelism ?? BulkStreamAllDefaults.MaxDegreeOfParallelismDefault;
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
			// One slot per worker holding that worker's most recently scheduled dispatch. A new batch for a
			// worker is chained after its previous batch so at most one batch per worker is ever in flight;
			// this preserves on-the-wire ordering for batches routed to the same worker (e.g. all operations
			// sharing a DocumentAffinityKey). Total in-flight batches are therefore bounded by MaxDegreeOfParallelism.
			var workerTails = new Task[_maxDegreeOfParallelism];
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

			async Task FlushAsync(int workerIndex, IList<T> batch, long page)
			{
				// Serialize on the worker's previous batch before scheduling the next one.
				var previous = workerTails[workerIndex];
				if (previous != null)
					await previous.ConfigureAwait(false);

				// Throttle the producer against consumer progress on every dispatch path (affinity and round-robin).
				if (_request.BackPressure != null)
					await _request.BackPressure.WaitAsync(_compositeCancelToken).ConfigureAwait(false);

				workerTails[workerIndex] = DispatchAsync(batch, page, workerIndex);
			}

			try
			{
				if (_affinityKeySelector != null)
					await ProduceWithAffinityAsync(FlushAsync).ConfigureAwait(false);
				else
					await ProduceRoundRobinAsync(FlushAsync).ConfigureAwait(false);

				await Task.WhenAll(workerTails.Where(t => t != null)).ConfigureAwait(false);
				OnCompleted(null, observer);
			}
			catch (Exception ex)
			{
				// Observe every scheduled dispatch so faulted batches never surface as unobserved task exceptions.
				try { await Task.WhenAll(workerTails.Where(t => t != null)).ConfigureAwait(false); }
				catch { /* the originating failure is surfaced below */ }

				OnCompleted(dispatchException ?? ex, observer);
			}
		}

		private async Task ProduceRoundRobinAsync(Func<int, IList<T>, long, Task> flush)
		{
			var partitioned = new PartitionHelper<T>(_request.Documents, _bulkSize);
			long page = 0;
			foreach (var batch in partitioned)
			{
				_compositeCancelToken.ThrowIfCancellationRequested();
				var workerIndex = (int)(page % _maxDegreeOfParallelism);
				await flush(workerIndex, batch, page).ConfigureAwait(false);
				page++;
			}
		}

		private async Task ProduceWithAffinityAsync(Func<int, IList<T>, long, Task> flush)
		{
			// Per-worker buffers filled by hashing the affinity key, so all documents sharing a key are
			// batched and dispatched by the same worker.
			var buffers = new List<T>[_maxDegreeOfParallelism];
			for (var i = 0; i < _maxDegreeOfParallelism; i++)
				buffers[i] = new List<T>(_bulkSize);

			var pageCounters = new long[_maxDegreeOfParallelism];

			foreach (var document in _request.Documents)
			{
				_compositeCancelToken.ThrowIfCancellationRequested();

				var key = _affinityKeySelector(document);
				var workerIndex = (int)((uint)GetStableHashCode(key) % (uint)_maxDegreeOfParallelism);
				buffers[workerIndex].Add(document);

				if (buffers[workerIndex].Count < _bulkSize) continue;

				var batch = new List<T>(buffers[workerIndex]);
				buffers[workerIndex].Clear();
				await flush(workerIndex, batch, pageCounters[workerIndex]++).ConfigureAwait(false);
			}

			// Flush any partially-filled buffers.
			for (var i = 0; i < _maxDegreeOfParallelism; i++)
			{
				if (buffers[i].Count == 0) continue;
				await flush(i, buffers[i], pageCounters[i]++).ConfigureAwait(false);
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
