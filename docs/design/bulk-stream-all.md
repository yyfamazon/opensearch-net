# Design: BulkStreamAll — High-Level Streaming Bulk Ingestion Helper

## Status
**Implemented** — this document describes the shipped feature. Items that were considered but are
**not** part of this implementation are collected under [Future Work](#future-work) so the reader is
never left guessing which parts exist.

## Problem Statement

Customers consuming high-throughput event streams (Kafka, Kinesis, change feeds) need to bulk-ingest
documents into OpenSearch with:
- Automatic batching by document count
- Retry with exponential backoff for transient failures (429, transport errors)
- Backpressure so a fast producer does not overwhelm the pipeline
- Progress reporting for observability
- Document-affinity so operations for the same document are never reordered

The existing `BulkAllObservable<T>` partially addresses this but distributes work round-robin (no
ordering guarantee) and uses fixed-delay retries. `BulkStreamAll` is a **new, parallel API** (not a
replacement) built on the `_bulk/stream` endpoint from #935.

## Context: What Exists Today

### PR #935 — Low-Level Bulk Stream API (landed)

Adds the `_bulk/stream` endpoint to the client:
- `BulkStreamRequest` / `BulkStreamDescriptor` — request types
- `BulkStreamResponse` — response with `Errors`, `Items`, `Took`
- `client.BulkStreamAsync(...)`

### `BulkAllObservable<T>` — Existing High-Level Helper

| Feature | `BulkAll` | `BulkStreamAll` |
|---------|-----------|-----------------|
| Wire endpoint | `_bulk` | `_bulk/stream` |
| Batching by count | ✅ | ✅ |
| Retry | fixed delay | exponential backoff + jitter |
| Backpressure | `ProducerConsumerBackPressure` | `ProducerConsumerBackPressure` |
| Progress reporting | `IObservable<BulkAllResponse>` | `IObservable<BulkStreamAllResponse>` |
| Document affinity | ❌ round-robin only | ✅ hash-routed with in-order dispatch |

## Decision: New Type vs. Extend Existing

A new `BulkStreamAllObservable<T>` is added alongside `BulkAllObservable<T>` rather than changing the
existing type, because the wire protocol differs (`_bulk/stream` vs `_bulk`) and `BulkAllObservable<T>`
is a public API with users depending on its behavior. Existing users keep `BulkAll`; new users targeting
`_bulk/stream` adopt `BulkStreamAll`.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                            User Code                               │
│                     IEnumerable<T> source                          │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ documents (lazily enumerated)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    BulkStreamAllObservable<T>                      │
│                                                                    │
│  Producer loop                Per-worker dispatch (N = MaxDOP)     │
│  ┌───────────────┐            ┌──────────┐ ┌──────────┐           │
│  │ read source   │            │ worker 0 │ │ worker 1 │  ...       │
│  │ route:        │──batch────▶│ 1 batch  │ │ 1 batch  │           │
│  │  • hash(key)  │            │ in flight│ │ in flight│           │
│  │  • or page%N  │            └────┬─────┘ └────┬─────┘           │
│  └───────────────┘                 │            │                  │
│         │ optional BackPressure     ▼            ▼                  │
│         │ (WaitAsync per dispatch) client.BulkStreamAsync(...)      │
│         ▼                                                          │
│  Response processing & retry                                       │
│   • inspect BulkStreamResponse.Items                               │
│   • retry retryable items (default: 429) w/ exponential backoff    │
│   • route non-retryable items to DroppedDocumentCallback           │
│   • emit BulkStreamAllResponse per successful batch                │
└──────────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Producer loop** — Enumerates the source (`IEnumerable<T>`, lazily) and routes each document to a
   worker. With `DocumentAffinityKey`, the routing key is hashed to pick a worker (via a runtime-stable
   hash, since `string.GetHashCode()` is randomized on .NET Core). Without it, full batches are assigned
   round-robin by page number.

2. **Per-worker dispatch** — Each worker sends **at most one batch at a time**. A worker's next batch is
   chained after its previous batch completes, so batches routed to the same worker are sent and
   acknowledged in dispatch order. Total in-flight batches are therefore bounded by
   `MaxDegreeOfParallelism`.

3. **Batching** — A batch is dispatched when a worker accumulates `Size` documents (default 1000);
   partially-filled buffers are flushed when the source is exhausted.

4. **Retry engine** — `RetryStrategy.ComputeDelay` implements exponential backoff with ±25% jitter:
   `min(RetryMaxDelay, RetryBaseDelay * 2^attempt * jitter)`, `jitter ∈ [0.75, 1.25]`. Only the
   retryable items from a response are re-sent (per `RetryDocumentPredicate`, default `status == 429`).

5. **Backpressure** — Optional `ProducerConsumerBackPressure`. When configured, the producer calls
   `WaitAsync` before scheduling each dispatch (on **both** the affinity and round-robin paths) and each
   completed bulk calls `Release`, throttling how far the producer runs ahead of consumers.

6. **Progress reporting** — `IObservable<BulkStreamAllResponse>` emitted per successful batch.

## API Surface

### Configuration — `IBulkStreamAllRequest<T>`

```csharp
public interface IBulkStreamAllRequest<T> where T : class
{
    // Source
    IEnumerable<T> Documents { get; }              // lazily evaluated

    // Batching & parallelism
    int? Size { get; set; }                        // docs per bulk request (default 1000)
    int? MaxDegreeOfParallelism { get; set; }      // parallel workers (default 4)

    // Retry
    int? MaxRetries { get; set; }                  // default 3
    TimeSpan? RetryBaseDelay { get; set; }         // default 1s
    TimeSpan? RetryMaxDelay { get; set; }          // default 30s
    Func<BulkResponseItemBase, T, bool> RetryDocumentPredicate { get; set; } // default: status == 429

    // Backpressure
    ProducerConsumerBackPressure BackPressure { get; set; }

    // Document routing
    Func<T, string> DocumentAffinityKey { get; set; } // null => round-robin (no ordering guarantee)

    // Target
    IndexName Index { get; set; }
    string Pipeline { get; set; }
    Routing Routing { get; set; }
    Time Timeout { get; set; }
    int? WaitForActiveShards { get; set; }

    // Behavior
    Action<BulkStreamDescriptor, IList<T>> BufferToBulk { get; set; }  // default: IndexMany
    Action<BulkResponseItemBase, T> DroppedDocumentCallback { get; set; }
    bool ContinueAfterDroppedDocuments { get; set; }  // default TRUE (continue on non-retryable failures)
    bool RefreshOnCompleted { get; set; }
    Indices RefreshIndices { get; set; }

    // Callbacks
    Action<BulkStreamResponse> BulkResponseCallback { get; set; } // every response, incl. retries
}
```

### Response — `BulkStreamAllResponse`

```csharp
public class BulkStreamAllResponse
{
    public long Page { get; internal set; }        // batch number (per worker for affinity, global otherwise)
    public int WorkerIndex { get; internal set; }
    public int Retries { get; internal set; }
    public IReadOnlyCollection<BulkResponseItemBase> Items { get; internal set; }
    public long Took { get; internal set; }         // server-side milliseconds
}
```

### Observable & Observer

```csharp
public class BulkStreamAllObservable<T> : IDisposable, IObservable<BulkStreamAllResponse>
    where T : class
{
    public BulkStreamAllObservable(IOpenSearchClient client, IBulkStreamAllRequest<T> request,
        CancellationToken cancellationToken = default);

    public IDisposable Subscribe(IObserver<BulkStreamAllResponse> observer);
    public IDisposable Subscribe(BulkStreamAllObserver observer);
    public void Dispose();  // cancels the operation
}

public class BulkStreamAllObserver : CoordinatedRequestObserverBase<BulkStreamAllResponse>
{
    public long TotalNumberOfFailedBuffers { get; }
    public long TotalNumberOfRetries { get; }
    public long TotalDocumentsProcessed { get; }
}
```

### Client Extension

```csharp
public partial interface IOpenSearchClient
{
    BulkStreamAllObservable<T> BulkStreamAll<T>(
        IEnumerable<T> documents,
        Func<BulkStreamAllDescriptor<T>, IBulkStreamAllRequest<T>> selector,
        CancellationToken cancellationToken = default) where T : class;

    BulkStreamAllObservable<T> BulkStreamAll<T>(
        IBulkStreamAllRequest<T> request,
        CancellationToken cancellationToken = default) where T : class;
}
```

### Blocking Extension (convenience)

```csharp
public static class BulkStreamAllExtensions
{
    // Subscribes and blocks until completion or timeout, returning the observer with summary counters.
    public static BulkStreamAllObserver Wait<T>(
        this BulkStreamAllObservable<T> observable,
        TimeSpan maximumRunTime,
        Action<BulkStreamAllResponse> onNext) where T : class;
}
```

## Document Affinity & Ordering (relates to opensearch-go#464)

When `DocumentAffinityKey` is set, the producer hashes the key to select a worker:

```csharp
var workerIndex = (int)((uint)GetStableHashCode(key) % (uint)numWorkers);
```

Because a worker dispatches at most one batch at a time and chains each new batch after the previous
one, this guarantees:
- All operations sharing a key are handled by the same worker.
- That worker's batches are sent and acknowledged in dispatch order (no two same-key batches race on
  the wire).

When `DocumentAffinityKey` is null, full batches are distributed round-robin for maximum throughput
(no ordering guarantee — same as `BulkAllObservable<T>`).

## Retry Strategy

```
Delay = min(RetryMaxDelay, RetryBaseDelay * 2^attempt * jitter),  jitter ∈ [0.75, 1.25]
```

Per response:
1. Send batch → get `BulkStreamResponse`.
2. Partition items into succeeded / retryable (per predicate) / dropped.
3. Invoke `DroppedDocumentCallback` for dropped items. If `ContinueAfterDroppedDocuments` is false,
   halt; otherwise continue.
4. If retryable items remain and attempts < `MaxRetries`, wait the backoff delay and re-send only the
   retryable items.
5. If retries are exhausted with items still failing, throw.

## Usage Examples

### Basic — index documents from a collection
```csharp
var observable = client.BulkStreamAll(documents, b => b
    .Index("my-index")
    .Size(500)
    .MaxDegreeOfParallelism(4)
    .MaxRetries(3)
    .DroppedDocumentCallback((item, doc) => logger.Warn($"Dropped: {item.Id}"))
);

var observer = observable.Wait(TimeSpan.FromMinutes(10), response =>
    logger.Info($"Page {response.Page} indexed {response.Items.Count} docs in {response.Took}ms"));
```

### With document affinity and backpressure
```csharp
var observable = client.BulkStreamAll(orders, b => b
    .Index("orders")
    .Size(1000)
    .MaxDegreeOfParallelism(8)
    .BackPressure(maxConcurrency: 8, backPressureFactor: 4)
    .DocumentAffinityKey(order => order.OrderId)   // same order always same worker
    .BufferToBulk((descriptor, batch) =>
    {
        foreach (var order in batch)
            descriptor.Index<OrderEvent>(i => i.Document(order).Id(order.OrderId));
    })
    .RetryDocumentPredicate((item, doc) => item.Status == 429 || item.Status == 503)
    .BulkResponseCallback(r => metrics.RecordBulkLatency(r.Took))
);
```

## File Layout

```
src/OpenSearch.Client/Document/Multiple/BulkStreamAll/
├── BulkStreamAllRequest.cs            // IBulkStreamAllRequest<T> + POCO + defaults
├── BulkStreamAllDescriptor.cs         // Fluent descriptor
├── BulkStreamAllObservable.cs         // Core orchestrator (producer loop, per-worker dispatch, retry)
├── BulkStreamAllObserver.cs           // Observer with atomic counters
├── BulkStreamAllResponse.cs           // Per-batch response DTO
├── BulkStreamAllExtensions.cs         // .Wait() blocking convenience method
├── OpenSearchClient-BulkStreamAll.cs  // Client extension methods
└── RetryStrategy.cs                   // Exponential backoff + jitter
```

## Testing

- **Integration tests** (`tests/Tests/Document/Multiple/BulkStreamAll/`) against a real cluster:
  end-to-end ingestion, document-affinity routing, **completion-order ordering** (each worker's batches
  complete in strictly ascending page order without pre-sorting), retry exhaustion, dropped-document
  callbacks, cancellation/dispose, observer counters, and **backpressure throttling** (peak in-flight
  requests capped below `MaxDegreeOfParallelism` when a tight `BackPressure` is configured).

## Future Work

These were considered during design but are **not implemented** in this change. They are recorded here
so the shipped surface above stays authoritative:

- **`IAsyncEnumerable<T>` source** — the current API accepts `IEnumerable<T>` only.
- **Byte-size batching (`MaxBatchBytes`)** and **timer-based flush (`FlushInterval`)** — batching is by
  document count (`Size`) only.
- **`Channel<T>`-based backpressure / `ChannelCapacity`** — backpressure uses
  `ProducerConsumerBackPressure`, not bounded channels.
- **`FlushAsync()` / `IAsyncDisposable`** (flush-without-close for warm, long-lived instances, cf.
  opensearch-go#336) — the observable is `IDisposable` only; `Dispose()` cancels.
- **Transparent fallback to `_bulk`** when a server lacks `_bulk/stream`.
- **Native OpenTelemetry metrics/spans** — use `BulkResponseCallback` for observability today.
