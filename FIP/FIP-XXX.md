# FIP-XXX: Support Per-Partition Bucket Count

|            |                                                                  |
|------------|------------------------------------------------------------------|
| **Current state** | Under Discussion |
| **Discussion thread** | TBD (`[DISCUSS] FIP-XXX Support Per-Partition Bucket Count`) |
| **Issue**  | [apache/fluss#3907](https://github.com/apache/fluss/issues/3907) |
| **Reference implementation** | [apache/fluss#3908](https://github.com/apache/fluss/pull/3908) |
| **Released** | \<Fluss Version\> |

## Motivation

Today a Fluss table and every one of its partitions share a single bucket count that is fixed at table-creation time. `bucket.num` is a structural property of the table, and a partition holds no independent bucket-count attribute: the number of buckets of a partition is simply the table-level `bucket.num` at the moment the partition assignment was generated, and nothing in the metadata records it.

This causes two problems.

**1. No per-partition flexibility.** In a typical time-partitioned table the data volume per partition grows over time. Yesterday's partition may be fine with 4 buckets while today's needs 32. Because the bucket count is table-wide, users must size the table for its future peak from day one, over-provisioning every historical partition, or accept that recent partitions are under-bucketed and become the write/read bottleneck.

**2. Rescaling is expensive.** Before this FIP, Fluss did not support changing the bucket count. The only way was to create a new table at the new bucket count and migrate all data — the most primitive and least efficient method: it redistributes **all** data, including historical partitions already tiered to the lake that should never be rewritten. For a large table this is a high-risk migration that also breaks the lake-side bucket layout of already-committed data.

In a real Fluss scenario, the problem we need to solve:

> **Scenario.** A Fluss table's bucket count needs to be adjusted at runtime to match changing data volumes, but historical partitions were written at the original bucket count and should not be changed.
>
> **Requirement.** As a Fluss user, I want to dynamically modify a table's bucket configuration, so that new partitions are written at the new configuration while historical partitions are left untouched, enabling flexible storage and performance tuning without rewriting old data.

This FIP answers that requirement directly: rescale can now act **only on partitions created after the change**, with existing partitions kept exactly as they are. This is sound because a partition's bucket layout only needs to be self-consistent — nothing requires two partitions of the same table to be bucketed identically, as long as every reader and writer routes by the *partition's own* bucket count.

This FIP makes the bucket count a **partition-level** attribute:

`ALTER TABLE ... SET ('bucket.num' = N)` affects only partitions created afterwards; existing partitions keep their bucket count and their data is not touched. This rule is inherently partition-scoped, so a non-partitioned table has nothing for it to act on.

### Goals

- A partitioned table may have partitions with different bucket counts.
- `ALTER TABLE ... SET ('bucket.num' = N)` is an online, data-movement-free metadata operation, supported in both directions — scaling up to spread load over more buckets, and scaling down to release the replicas and RocksDB instances that surplus buckets occupy.
- The user-facing surface stays **table-level only**: `ALTER TABLE ... SET ('bucket.num' = N)`. No partition-level `ALTER` syntax is introduced.
- Every read, write, lookup, tiering and union-read path routes by the target partition's actual bucket count.
- A client holding stale metadata can never silently write a record into the wrong bucket; it must fail loudly.

### Non-Goals

- **Automatic** rescale of existing data. This FIP does not rewrite existing partitions; that is left to a future FIP.
- Lake formats other than **Paimon**. Only Paimon is considered here; other formats will be considered later.

## New or Changed Public Interfaces

### RPC / Proto changes (`FlussApi.proto`)

All new fields are `optional`, so old and new peers remain wire-compatible.

Table-level epoch:

| Message | Field |
|---|---|
| `PbTableMetadata` | `optional int64 bucket_layout_epoch = 9` |
| `GetTableInfoResponse` | `optional int64 bucket_layout_epoch = 7` |

Per-partition bucket count:

| Message | Field |
|---|---|
| `PbPartitionMetadata` | `optional int32 bucket_count = 5` |
| `PbPartitionInfo` | `optional int32 bucket_count = 4` |

Bucket-routed requests (the value used to compute `bucket_id`):

| Message | Field |
|---|---|
| `PbProduceLogReqForBucket` | `optional int32 bucket_count = 4` |
| `PbPutKvReqForBucket` | `optional int32 bucket_count = 4` |
| `PbFetchLogReqForBucket` | `optional int32 bucket_count = 5` |
| `PbLookupReqForBucket` | `optional int32 bucket_count = 5` |
| `PbPrefixLookupReqForBucket` | `optional int32 bucket_count = 4` |
| `PbScanReqForBucket` | `optional int32 bucket_count = 5` |
| `PbTableStatsReqForBucket` | `optional int32 bucket_count = 3` |
| `LimitScanRequest` | `optional int32 bucket_count = 6` |
| `ListOffsetsRequest` | `optional int32 bucket_count = 7` |

### Error codes

| Code | Name | Exception | Meaning |
|---|---|---|---|
| 74 | `STALE_METADATA` | `StaleMetadataException` | The bucket count in the request does not match the server's actual bucket count. The client must refresh metadata before retrying. |

`StaleMetadataException` itself is not a new class (`@PublicEvolving`, `@since 0.7`); what is new is its registration in `Errors`, enabling it to be transmitted in RPC responses back to the client. It extends `InvalidMetadataException` and `RetriableException`, so the generic machinery would retry it; therefore the write and lookup paths intercept error code 74 explicitly before the retriable-error branch, see C.2 and C.3.

### ZooKeeper metadata format

| Znode | Change |
|---|---|
| `PartitionRegistration` (`/.../partitions/<name>`) | Adds optional `bucket_count`; serde `VERSION` bumped **1 → 2**. Absent in v1 data → read as `null`, resolved via `getBucketCountOrDefault`. |
| `TableRegistration` (`/.../tables/<table>`) | Adds `bucket_layout_epoch`; serde `VERSION` bumped **1 → 2**. Absent in v1 data → read as `0`, meaning the table has never been ALTERed. |

Both readers tolerate the field being absent. Note that the writers emit the field unconditionally once upgraded, and the deserializers ignore unknown fields — see *Migration Plan* for the downgrade implication.

### Configuration and DDL

No new configuration options are introduced. Two existing surfaces change meaning:

- **`bucket.num` semantics.** Previously "the number of buckets of a Fluss table". Now: "the target bucket count; for partitioned tables it applies to newly created partitions, while existing partitions retain their original bucket count."
- **`bucket.num` becomes alterable.** It is removed from the Flink connector's `ALTER_DISALLOW_OPTIONS`, enabling:

```sql
ALTER TABLE my_partitioned_table SET ('bucket.num' = '8');
```

## Proposed Change

### A. Introducing the partition-level `bucket.num.actual` semantic

This FIP splits the table-level and partition-level bucket count into two attributes, structurally symmetric but differing only in writability and query method:

| | `bucket.num` | `bucket.num.actual` |
|---|---|---|
| Scope | Table | Partition (for a non-partitioned table, it simultaneously holds both `bucket.num` and `bucket.num.actual`) |
| Meaning | **Target**: the intended end-state bucket count, used as the template for partitions created from now on | **Actual**: the bucket count a specific partition really uses |
| Writable by users | Yes, via DDL (`CREATE TABLE`, `ALTER TABLE ... SET`) | No — derived by the server at partition creation |
| Persisted as | `TableRegistration.bucketCount` | `PartitionRegistration.bucketCount` |
| Readable by users | Yes (`DESCRIBE`, connector options) | Via a stored procedure that inspects the `bucket.num.actual` of a partition or a non-partitioned table |

Both are `int` structural fields, both serialized as `bucket_count` in the znode JSON, neither stored in the property map. `bucket.num` additionally serves as a Flink connector option key (`FlinkConnectorOptions.BUCKET_NUMBER`) exposed to users via `WITH ('bucket.num' = N)`; `bucket.num.actual` has no connector-side key and cannot be set directly by users. The difference between the two lies only in writability and query method.

#### A.1 Consistency rule

After an `ALTER`, newly created partitions have `bucket.num.actual == bucket.num`, because a new partition copies the table-level `bucket.num` at creation time, and the table-level value has already been updated to the target value N by the ALTER.

Pre-existing partitions have `bucket.num.actual != bucket.num`, because these partitions were created before the ALTER, their `bucket.num.actual` copied the pre-ALTER table-level value, and the table-level value has now changed. This divergent state persists until the partition's data is rewritten (by a future FIP) and they reconverge.

A non-partitioned table simultaneously holds both attributes. After an `ALTER`, the table's `bucket.num` and `bucket.num.actual` diverge in the same way: `bucket.num` becomes N, `bucket.num.actual` retains the old value, and only a data rewrite (by a future FIP) brings them back together.

This consistency rule is the cornerstone of the entire design: it ensures that "which partitions are still on the old layout and which are on the new layout" can be determined at any moment by comparing `bucket.num` and `bucket.num.actual`. This is also the purpose of the stored procedure that queries `bucket.num.actual` — users can use it to determine which partitions need data redistribution.

#### A.2 Inheritance / snapshot semantics

A partition **copies** the table-level `bucket.num` **once, at the instant of its creation**, and persists it as its own `bucket.num.actual`. From then on that partition's `bucket.num.actual` is immutable until its data is rewritten.

This means: subsequent changes to the table-level `bucket.num` **do not** affect the `bucket.num.actual` of existing partitions — each partition "freezes" the table-level value at the moment of its creation. This is the core mechanism that enables "new partitions use the new bucket count, old partitions keep the old bucket count": no migration is needed for old partitions, they naturally stay unchanged.

Typical timeline:

1. Create table with `bucket.num=4` → every partition created in this period carries `bucket.num.actual=4`
2. User runs `ALTER TABLE ... SET ('bucket.num' = '8')` → table-level `bucket.num` becomes 8
3. Every partition created afterwards carries `bucket.num.actual=8`, copied from the new table-level value
4. Two generations of partitions (4-bucket and 8-bucket) coexist indefinitely in the same table, and read/write paths route by each partition's own `bucket.num.actual`

### B. The ALTER operation: modifying the table-level bucket num property

The entire ALTER is divided into two phases: **lake-first propagation** and **Fluss-side ZK transaction**. The two are not atomic across systems, but the design guarantees eventual consistency: lake propagation is idempotent and safe to re-run, and the Fluss-side old-partition backfill plus table-level update are committed atomically in a single ZK transaction.

#### B.1 Lake-first propagation

If the table is lake-enabled (`table.datalake.enabled = true`) and the bucket key is non-empty, the coordinator **first** propagates the new bucket count to the lake side, **then** modifies the Fluss side. This follows the existing `alterTableSchema` "lake-first" ordering: a lake failure aborts the ALTER with the Fluss side unchanged, and because propagation is idempotent, re-running the same ALTER after a Fluss-side failure converges both sides.

For Paimon, the coordinator calls `PaimonLakeCatalog.alterTable`, changing the Paimon table schema's `CoreOptions.BUCKET` to the target value N. This causes Paimon to use the new bucket count when creating new partitions, overriding `totalBuckets`.

Propagation is **idempotent**: repeatedly propagating the same N value produces no side effects — Paimon's `BUCKET` option is set to the same value, and repeated setting does not change the layout of existing partitions.

On propagation failure (lake unreachable, lake catalog exception, table not found), **the ALTER is aborted and the Fluss side is unchanged**. The user re-runs the same ALTER; lake propagation starts from scratch, and because it is idempotent, no dirty state is produced.

Iceberg / Hudi / Lance do not support this propagation; `alterTable` throws `UnsupportedOperationException` and the ALTER is rejected.

Non-lake-enabled tables or tables with an empty bucket key skip this phase; no lake propagation is needed.

#### B.2 Old-partition backfill

Partitions that existed before the ALTER ("old partitions") may not have persisted their own `bucketCount` — in particular, partitions created by versions predating this FIP have `PartitionRegistration.bucketCount` as `null`. The purpose of backfill is to populate `bucket.num.actual` for these partitions, using their **current actual** bucket count, derived from the bucket assignment size, rather than the new table-level value N.

This ensures that old partitions continue to run on their original layout after the ALTER — their `bucket.num.actual` is "frozen" at the pre-ALTER value, rather than changing along with the table-level value.

Backfill is **idempotent**: partitions that already have a `bucketCount` are skipped. Therefore, if the ALTER is retried, partitions that have already been backfilled are not processed again.

Backfill computation: for each existing partition without a persisted `bucketCount`, read its registration together with its znode version, and derive the bucket count from the bucket assignment size. If a partition is listed but its registration or assignment is unreadable, **abort the entire ALTER** — a partial backfill would leave that partition routed by the new table-level value, which is exactly the corruption this design exists to prevent.

The backfill results — each partition's `PartitionRegistration` and the corresponding znode version — are not committed separately, but merged with the Fluss-side table-level update in the same ZK transaction to guarantee atomicity.

#### B.3 Fluss-side atomic commit

Once backfill computation is complete, all partition backfills plus the new `TableRegistration` (containing the new bucket count and `bucketLayoutEpoch + 1`) are written in a single ZooKeeper multi-op transaction.

Each `setData` is CAS-guarded by the znode version captured at read time: if any znode was modified after it was read, CAS fails and the entire transaction rolls back. The entire transaction is also fenced by the coordinator-epoch znode: if the coordinator has been deposed (epoch mismatch), the transaction also fails.

Consequence: **no partition can observe the new table-level bucket count without also having its own `bucket.num.actual`.** Backfill and table-level update either take effect together or not at all; there is no intermediate state where "the table-level value has changed but an old partition does not yet have `bucket.num.actual`".

After a successful commit, if the table has auto-partitioning enabled, `AutoPartitionManager`'s cached `TableInfo` is refreshed so that newly auto-created partitions read the new bucket count fresh from ZK and stamp it into their own registration.

#### B.4 Concurrency control

A **striped fair `ReentrantReadWriteLock`** array guards the bucket layout. Fixed 1024 locks, `TablePath.hashCode() % 1024` maps to a fixed stripe. ALTER takes the write lock; the following operations take the read lock: `createPartition` (manual or dynamic), `dropPartition` (manual), `auto-partition` pre-creation, `auto-partition` historical-partition creation, `auto-partition` expired-partition cleanup, and creation/deletion during historical-partition `enable/disable`.

**Why striped rather than per-table map:** a per-table `ConcurrentHashMap<TablePath, ReadWriteLock>` requires `remove` on table drop to avoid lock-object leaks. But if a table with the same name is recreated after drop, the new lock and the old lock are different instances, and mutual exclusion may be lost. Striped locks have no lifecycle management — the same `TablePath` always maps to the same lock (via hashCode), even after drop/recreate, so mutual exclusion is never lost. 1024 stripes keep collision probability negligible, with memory of only a few KB, allocated once.

**Fair mode** (`ReentrantReadWriteLock(true)`) prevents ALTER starvation under a steady stream of partition creations. Concurrent partition creations remain parallel — read locks do not mutually exclude, only the write lock does.

When creating a partition, the table-level bucket count is **read fresh from ZooKeeper inside the read lock** (`metadataManager.getTableRegistration(tablePath).bucketCount`), rather than from a possibly-stale cached `TableInfo`, and stamped into the new partition's registration. This ensures that if a partition-creation request arrives while an ALTER is in progress, it reads the pre-ALTER value (because the ALTER has not committed yet), which is self-consistent. Auto-partition creation likewise reads fresh from ZK inside the read lock.

#### B.5 Failover and retry

**Consistency guarantee:** ALTER is divided into lake propagation and Fluss-side ZK transaction, two phases that are not atomic across systems. The design guarantees correctness through **idempotency + bounded retry + eventual consistency**.

- **Lake propagation failure: the entire ALTER has no effect.** Lake propagation executes before the Fluss ZK transaction. If the lake is unreachable or the lake catalog throws, the ALTER is aborted directly and the Fluss-side ZK transaction is not executed. Neither side is changed; full rollback. The user re-runs the same ALTER.

- **Lake propagation succeeds but Fluss ZK fails: sides diverge, error reported, user asked to re-run.** The lake side has been changed to BUCKET = N, but the Fluss side is unchanged (bucket.num = old value, epoch unchanged). The ZK transaction is **automatically retried**, up to 3 times. After 3 retries, a `FlussRuntimeException` is thrown to alert the user. When the user **manually re-runs** the same ALTER, the lake is re-propagated, but lake propagation is idempotent and produces no side effects (repeatedly setting the same N value). The Fluss-side ZK retries the commit. If the user does not re-run, correctness is not affected (the tiering writer uses the Fluss-side partition-level bucket count to override Paimon's BUCKET), but the two sides remain inconsistent.

- The tiering service touches both sides during read/write — it reads Fluss metadata (`tableInfo.getNumBuckets()`) and Paimon schema (`CoreOptions.BUCKET`); if the two are inconsistent, it alerts the user to re-run the ALTER so both sides converge.

**Idempotency guarantee:** all three parts are individually idempotent, so the same ALTER can be safely re-run and both sides eventually converge:
- **Lake propagation idempotent:** repeatedly setting Paimon's `CoreOptions.BUCKET` to the same N value produces no side effects.
- **Backfill idempotent:** `computePartitionBucketCountBackfill` skips partitions that already have a `bucketCount` when iterating. A backfill that did not complete on the first run continues where it left off on retry.
- **Fluss-side ZK transaction idempotent:** the table-level update writes an absolute target value N (not an incremental operation), CAS-guarded. If the previous run already committed, a re-run reads the new state, the `bucketCount` value is unchanged (still N), and CAS succeeds. Although epoch is incremented by an extra +1, epoch's most important role is the "has been ALTERed" flag, and an extra increment does not affect correctness here.

**Atomicity guarantee:** backfill + table-level update are committed in the same ZK multi-op transaction; either both take effect or neither does. No partition can observe an intermediate state where "the table-level value has changed but it does not yet have `bucket.num.actual`".

**Coordinator crash recovery:**

| Scenario | Recovery strategy |
|---|---|
| ZK transaction sent, coordinator crashes before receiving response | On restart, the coordinator **reads the current state from ZK** to determine whether it committed: read `TableRegistration`; if `bucketCount` = new value N and `bucketLayoutEpoch` = old+1, it is committed, ALTER is complete; if `bucketCount` = old value and `bucketLayoutEpoch` = old value, it is uncommitted, ALTER did not take effect, user re-runs. |
| ZK transaction succeeded, then coordinator crashes | ZK transaction is committed (`bucketCount` = N, `epoch` = old+1, all partition backfills complete). `AutoPartitionManager` refreshes on the next event-processing cycle. No special handling needed. |

### C. The bucket-count routing contract for read and write paths

#### C.1 Why bucket-routed requests must carry bucket_count

The client computes `bucket_id` using `hash(key) % bucketCount`. If the client uses a stale `bucketCount`, the resulting `bucket_id` still falls within `[0, actualCount)` and is still a **valid** bucket id — the server accepts it, but the record lands in the wrong bucket. This is a **silent mis-route**: invisible and irreparable after the fact.

The fix: make `bucket_count` part of the request. Every bucket-routed request gains an `optional int32 bucket_count` proto field, declaring the value the client used to compute `bucket_id`. The server compares it with the partition's actual bucket count; a mismatch returns `STALE_METADATA`, turning the silent mis-route into an explicit failure.

#### C.2 Write path

| Component | What it does on the write path |
|---|---|
| `WriterClient#doSend` | Before assigning a bucket, uses `cluster.getBucketCount(tablePartition)` to get the target partition's bucket count, falling back to `tableInfo.getNumBuckets()` if absent, and initializes the `BucketAssigner` with that value |
| `WriteBatch` | Stores the bucket count in its `final int bucketCount` field at creation time; never re-reads `Cluster` afterwards |
| `DynamicPartitionCreator` | Synchronously waits until both partitionId and bucketCount are visible in the client cluster metadata before proceeding; bounded by `client.request-timeout`, default 30s, with exponential backoff polling starting at 100ms, doubling, capped at 1s, with a forced metadata refresh each round |
| `ClientRpcMessageUtils` | When building `PbProduceLogReqForBucket` and `PbPutKvReqForBucket`, takes the anchored bucket count from the batch and fills the `bucketCount` field |
| `Sender#handleWriteBatchException` | Explicitly intercepts `Errors.STALE_METADATA` **before** the retriable-error branch: fails the batch without re-enqueuing, marks the table's metadata invalid, and invalidates the cached `BucketAssigner` for that bucket |

**Why `WriteBatch` must anchor the bucket count.** `Cluster` is a volatile immutable snapshot, replaced wholesale on refresh. If `Cluster` is read twice — once to compute `bucket_id` and once to fill `bucket_count` — the batching window may straddle an ALTER: `bucket_id` is computed from the old layout, while `bucket_count` carries the new layout's value, and the request declaration no longer matches the actual routing. Anchoring guarantees both come from the same snapshot.

**Why dynamic partition creation must wait synchronously.** Previously it was asynchronous — the writer issued `createPartition` and moved on, and bucket assignment used the table-level count. After an ALTER the table-level count no longer represents the new partition's count, so the partition-level count must be visible before a bucket can be assigned. Concurrent writers targeting the same new partition share one in-flight creation and all wait on the same metadata condition.

**Why STALE_METADATA must be explicitly intercepted.** `StaleMetadataException` extends `InvalidMetadataException` and `RetriableException`, so the generic machinery would retry it, but the batch's `bucket_id` is already fixed — retrying would only land in the wrong bucket again. Failing the batch and invalidating the cached `BucketAssigner` lets the next write re-resolve the bucket count from the refreshed `Cluster`.

#### C.3 Read path

| Component | What it does on the read path |
|---|---|
| `AbstractLookuper#resolvePartitionBucketCount` | Resolves the target partition's bucket count the same way as the write path: query `Cluster` first, fall back to the table-level value if absent |
| `LookupBatch`, `PrefixLookupBatch` | Anchor their `final int bucketCount` at construction; the request builder no longer takes a `Cluster` parameter |
| `ClientRpcMessageUtils` | When the batch's bucket count is positive, sets `bucketCount` on `PbLookupReqForBucket` and `PbPrefixLookupReqForBucket` |
| `TableScan`, `LimitBatchScanner`, `KvBatchScanner`, `LogFetcher` | Read the bucket count **live** from `Cluster` on each request; no anchoring |
| `ListOffsets`, `TableStats` requests | Also read the bucket count live from `Cluster`; set the proto field only if a value is present |
| `LookupSender#handleLookupError` | Explicitly intercepts `Errors.STALE_METADATA` **before** the retriable-error branch: first invalidates the table/partition metadata via the `InvalidMetadataException` branch, then exceptionally completes all lookups in the batch without entering the retry loop |

Lookup also batches, so the anchoring rationale is the same as `WriteBatch`. Scanners do not batch, so there is no window between reading the value and sending the request in which an ALTER could occur; live read suffices.

**Historical-partition lookup is resolved by the server.** A historical partition has already been dropped from Fluss; its data lives only in the lake, laid out with the bucket count in effect when it was tiered. After an ALTER, neither the table-level count nor any partition-level `bucket.num.actual` describes this value — only the lake metadata still knows it, and Paimon exposes it as `DataSplit#totalBuckets`. The server therefore resolves the tiered bucket count from the original partition's lake snapshot, locates the lake bucket the key belongs to under that count, and reads it. The client only supplies the key and the original partition name; it does not compute `bucket_id`, and the `bucket_count` validation does not apply to historical-partition lookup requests.

#### C.4 Bucket enumeration becomes partition-level

Every place that enumerated `[0, tableInfo.getNumBuckets())` for a partitioned table now enumerates `[0, partition.getBucketCount())`:

| Layer | Component | Enumeration scenario |
|---|---|---|
| Client | `TableScan` | Enumerate partition buckets during batch scan |
| Client | `LimitBatchScanner`, `KvBatchScanner` | Limit scan, KV snapshot scan |
| Client | `LogFetcher` | Enumerate buckets to fetch |
| Flink | `FlinkSourceEnumerator` | Enumerate partition buckets to generate splits |
| Flink | `FlussOnlyBatchSplitGenerator` | Log split generation |
| Flink | `LakeSplitGenerator` | Union-read split generation |
| Flink | `TieringSplitGenerator` | Tiering split generation |
| Flink | `TieringSplitReader` | Snapshot partition bucket count for lake writer |
| Flink | `RecoveryOffsetManager` | Undo-recovery bucket enumeration for offset rebuild |
| Flink | `PushdownUtils` | `count(*)` pushdown enumeration |
| Flink | `OrphanCleanUtils` | Orphan data cleanup enumeration |
| Spark | `SplitPlanner`, `FlussMicroBatchStream` | Batch and micro-batch split generation |
| Server | `RpcServiceBase`, `CoordinatorService#resolveNumBuckets` | Resolve partition bucket count from partition assignment rather than table-level value |

#### C.5 Client `Cluster` gains bucket-count maps

`Cluster` gains two bucket-count maps: `bucketCountByPartition`, keyed by `TablePartition(tableId, partitionId)` for partitioned tables; and `bucketCountByTable`, keyed by `tableId` for non-partitioned tables.

Previously `Cluster` had no explicit bucket-count map — the bucket count was implicit in the size of the bucket locations, because all partitions of the same table had the same bucket count. After an ALTER, different partitions have different bucket counts, so the size can no longer derive the target partition's count; an explicit per-partition bucket count must be stored.

`TablePartition` is chosen as the key rather than the path, because `partitionId` is the partition's stable identity: globally unique and non-reusable. When a partition is dropped and recreated, the path stays the same but `partitionId` is new; path-based keying cannot distinguish the two layouts, while `TablePartition`-based keying naturally isolates them.

### D. `bucketLayoutEpoch`: bucket-layout version and routing validation

#### D.1 What it is

`bucketLayoutEpoch` is a `long` field on `TableRegistration`, a table-level monotonically increasing counter. New tables default to `0`; legacy JSON without the field is also read as `0`. It is advanced only in `TableRegistration#withBucketCount`, incrementing by 1 each time `bucket.num` is ALTERed. The new bucket count and epoch + 1 are bound in the same atomic operation; no code path can publish a new bucket layout without also advancing the epoch.

`bucketLayoutEpoch` is propagated via `PbTableMetadata` and `GetTableInfoResponse` at the proto layer.

#### D.2 Why it is needed

The core question: when the server receives a bucket-routed request, how does it determine whether the client's `bucket_count` is stale?

If the server cannot distinguish "the table has never been ALTERed" from "the table has been ALTERed", there are only two choices:
- Always accept: after an ALTER, old partitions are routed by the new table-level value — **silent mis-route**
- Always reject: even tables that have never been ALTERed reject old clients — **not backward-compatible**

`bucketLayoutEpoch` is that distinguishing flag:
- `epoch == 0`: the table has never been ALTERed, all partition bucket counts equal the table-level value, falling back to the table level is **provably safe**
- `epoch > 0`: the table has been ALTERed, partition bucket counts are no longer **guaranteed** to equal the table-level value, fallback is not provably safe, so it is not done

The key point is "not guaranteed", not "necessarily different". The table-level bucket count has an ABA form: after changing from 4 to 8 and back to 4, the 4-generation partitions' bucket count happens to equal the current table-level value, the 8-generation does not, and both generations coexist in the same table. Equality is coincidental; the table-level value cannot tell which generation a partition belongs to, and falling back to the table level would route some partitions correctly and silently mis-route the rest. Therefore, once `epoch > 0`, fallback is never done; only the partition's own `bucket.num.actual` is used for routing.

#### D.3 How it works

`bucketLayoutEpoch` is used in three places:

**Step 1: Server validates routing requests.** `TabletService` calls `TabletServerMetadataCache#validateBucketCount` to validate the `bucket_count` in the request before processing each bucket-routed request. Bucket-routed requests include ProduceLog, PutKv, FetchLog, Lookup, PrefixLookup, Scan, LimitScan, ListOffsets, and TableStats. The validation logic is:

```
Request without bucket_count (optional field not set, read as 0):
  epoch == 0 → NONE, omission allowed, table-level fallback is safe
  epoch > 0  → STALE_METADATA, table has been ALTERed, omission not allowed

Request with bucket_count:
  Partitioned request: snapshot has the partition's bucket count and it differs → STALE_METADATA
  Non-partitioned request: snapshot's table bucket metadata is non-empty and size differs → STALE_METADATA
  Otherwise → NONE, including equality and the case where the snapshot lacks the partition's or table's bucket count
```

The server only judges a mismatch when it actually knows the target bucket count. When the snapshot lacks the partition's or table's bucket count, the request is allowed through, to avoid spurious rejections from incomplete snapshots during metadata propagation; if the client can fill in a partition-level bucket count, the value must have come from the server's metadata, so allowing it does not introduce a mis-route.

The server **does not infer client versions**; it only looks at the request content and the epoch. An old client never carries `bucket_count` — it is allowed at `epoch == 0` and rejected at `epoch > 0`. A new client at `epoch == 0` may also carry the table-level value, because the server did not return a partition-level count and the client fell back from `Cluster` to the table level; the table-level value equals the actual value at `epoch == 0`, so validation passes.

Before applying its first `UpdateMetadata`, the TabletServer does not participate in validation: the snapshot is empty and no bucket-count judgment can be made. `validateBucketCount` throws a retriable `LeaderNotAvailableException`; the client refreshes metadata and retries, rather than making a match or mismatch judgment on an empty snapshot.

**Step 2: Server returns partition metadata.** `RpcServiceBase#listPartitionInfos` reads the table registration first (to get `bucketLayoutEpoch`), then the partition registrations, and calls `PartitionRegistration#getBucketCountOrDefault` to fill `bucket_count` for each partition:

```
Partition has persisted bucketCount → use it
Partition has no bucketCount and epoch == 0 → fall back to table-level value, which equals the actual value
Partition has no bucketCount and epoch > 0 → throw StaleMetadataException, refuse table-level fallback
```

This means: at `epoch > 0`, the server **must return the partition's actual bucket count** and cannot fall back to the table-level value. The ALTER backfill process guarantees that at `epoch > 0` all partitions have a persisted `bucketCount`, so this branch is normally not triggered — it is a safety net that prevents silently returning a wrong value when metadata is inconsistent.

**Step 3: TabletServer does not accept a lower epoch.** When `TabletServerMetadataCache` receives an `UpdateMetadata` message, it compares the `bucketLayoutEpoch` in the message with the value it already holds. If the message's epoch is lower, **it discards that table's update**; other tables in the same message are applied normally. Discarding a lower epoch prevents an older bucket layout from replacing the one committed by a later ALTER `bucket.num`. The epoch orders only table-level bucket-layout changes; it is not a general metadata version: two updates with the same epoch may carry different schema, partition, or replica metadata.

## Migration Plan and Compatibility

### Upgrade contract

**1. No ALTER during rolling server upgrade.** During a rolling upgrade, old and new TabletServers coexist, and old TabletServers do not understand `bucket_layout_epoch` and `bucket_count`. If an ALTER is executed at this time, old TabletServers receive `UpdateMetadata` with the new fields but cannot handle them correctly, leading to silent mis-route. Therefore ALTER must not be executed until all TabletServers have been upgraded, and this is enforced server-side: the coordinator checks, before executing an ALTER, that all live TabletServers have reported a minimum version supporting per-partition bucket count; if any node does not satisfy this, the ALTER is rejected. Version reporting and checking follow the KIP-584 feature-version negotiation approach.

**2. Upgrade clients first, then the cluster.** If the cluster (server) is upgraded before the clients:
- Old clients can continue to operate while the table has not been ALTERed (epoch=0) — the server accepts requests without `bucket_count`
- Once the table is ALTERed (epoch>0), old clients' requests are explicitly rejected by the server with `STALE_METADATA` — old clients cannot read or write the table until upgraded

If clients are upgraded first:
- New clients are fully compatible with old clusters (new proto fields are optional, old servers ignore them)
- After cluster upgrade, seamless transition with no additional steps

**3. New clients seamlessly support both old-only and new-only clusters.**
- New client → old cluster: new proto fields are ignored by old servers; responses omit `bucket_layout_epoch` / `bucket_count`, the client reads epoch=0 and falls back to the table-level bucket count. Equivalent to old-client behavior.
- New client → new cluster: fully compatible, all features available.

### Compatibility matrix

Broken down by server version, client version, and table epoch:

| Server | Client | epoch | Behavior |
|---|---|---|---|
| New | New | 0 | Fully compatible. New client may omit `bucket_count` (when server does not return partition-level); epoch=0 allows omission, table-level fallback is safe. |
| New | New | `>0` | Fully compatible. New client carries `bucket_count`; server validates and passes. If the client's metadata is stale, `STALE_METADATA` is returned; client refreshes and recovers. |
| New | Old | 0 | Compatible. Old client omits `bucket_count`; epoch=0 allows omission. Table-level bucket count is correct for all partitions. |
| New | Old | `>0` | **Incompatible.** Old client omits `bucket_count`; epoch`>0` rejects with `STALE_METADATA`. Old client cannot read or write the table until upgraded. |
| Old | New | N/A | Compatible. New proto fields are ignored by old servers; client reads epoch=0, falls back to table-level. Equivalent to today's behavior. |
| Old | Old | N/A | Status quo, unchanged. |

Core rule: **whether `bucket_count` may be omitted is determined by the epoch** — at epoch=0, omission is safe (the table-level bucket count is correct for all partitions); at epoch>0, omission is rejected (the table-level bucket count no longer equals old partitions' bucket counts). Client newness only indirectly affects "whether `bucket_count` is filled"; the server does not infer client versions, it only looks at the request content and the epoch.

### Upgrade procedure

1. **Upgrade all clients / connectors** (Flink, Spark, Kafka-compatible, Rust, and any embedded users of `fluss-client`). New clients are compatible with old clusters and immediately usable after upgrade.
2. **Upgrade servers** (CoordinatorServer + TabletServers). Do not execute `ALTER TABLE ... SET ('bucket.num' = N)` during the rolling upgrade.
3. **After all servers are upgraded**, `ALTER TABLE ... SET ('bucket.num' = N)` may be used.

### Known limitations

- **`bucket.num` cannot be changed during the rolling-upgrade window.** As long as any live TabletServer has not reported a minimum version supporting per-partition bucket count, the coordinator rejects ALTER; the user must wait until all servers are upgraded.
- **Downgrade after an ALTER is unsafe.** Both `TableRegistration` and `PartitionRegistration` are now at serde version 2, and neither deserializer validates the version or retains unknown fields, so an older server reading a v2 znode would drop `bucket_layout_epoch` and `bucket_count` and then route every partition by the table-level count. Downgrading a cluster that has ALTERed any table is therefore not supported.
- Rescaling existing partitions is not yet supported (to be addressed by a future FIP).

## Rejected Alternatives

**1. Keep the bucket count table-wide and rescale by rewriting all data.**

Method: keep the bucket count as a table-level fixed property; the only way to change it is to create a new table at the new bucket count and rewrite all data.

Rejection reasons:
- Rewrites historical partitions that nobody wanted to change, including data already tiered to the lake that should never be rewritten.
- Breaks the lake-side bucket layout of already-committed data.
- For large tables, this is a long, high-risk migration.
- The core observation of this FIP is that a partition's bucket layout only needs to be internally self-consistent; different partitions of the same table can use different bucket counts. Rewriting cannot target only new partitions — old partitions are forced to be rewritten along with them, even though they do not need to change.

**2. Keep the client `Cluster`'s bucket count keyed by `PhysicalTablePath`.**

Method: key the client `Cluster`'s partition-level bucket-count map by `PhysicalTablePath`, i.e. the `db.table$partition` string.

Rejection reasons:
- Bucket count belongs to a concrete tableId or partitionId, not to a table name or partition name. For partitioned tables, the bucket count of `TablePartition(tableId, partitionId)` never changes; for non-partitioned tables, the bucket count of tableId never changes. ALTER bucket.num only changes the default value used by newly created partitions; existing TablePartitions retain their own `bucket.num.actual`.
- Table names and partition names do not provide this guarantee. A future INSERT OVERWRITE may replace an object with a new tableId or partitionId under the same name and with a different bucket count. The path stays the same while `TablePartition` differs; using the path as key would mix the old and new layouts together.

**3. Use coordinatorEpoch to replace bucketLayoutEpoch.**

Method: use the coordinator's leader-term epoch as the table-level version.

Rejection reasons:
- coordinatorEpoch represents coordinator leader switches, not per-table bucket.num changes.
- One coordinator switch may not involve any bucket.num change, and one bucket.num change does not necessarily trigger a coordinator switch; the two have no correspondence.

**4. Use client version or a boolean flag to replace bucketLayoutEpoch.**

Method: reject old clients' requests by client protocol version; or persist a boolean flag recording whether an ALTER has occurred.

Rejection reasons:
- Rejecting by client version would immediately reject all old clients after a server upgrade, even if the table has never been ALTERed.
- A boolean flag can record whether an ALTER has occurred, but two UpdateMetadata messages produced by two ALTERs both carry `true`, and the TabletServer cannot tell which is older. If a message carrying a stale table-level value arrives late, the boolean cannot have it ignored. A monotonically increasing epoch solves this via numeric comparison, which better matches the current implementation.
