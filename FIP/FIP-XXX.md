# FIP-xx: Support Per-Partition Bucket Count

# FIP-XXX: Support Per-Partition Bucket Count

| **Current state** | Under Discussion |
| --- | --- |
| **Discussion thread** | here (<- link to [https://lists.apache.org/list.html?dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org)) |
| **Vote thread** | here (<- link to [https://lists.apache.org/list.html?dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org)) |
| **Issue** | [apache/fluss#3907](https://github.com/apache/fluss/issues/3907) |
| **Release** | <Fluss Version> |

## Motivation

Today, a Fluss table and all of its partitions share a single bucket count fixed at table-creation time. `bucket.num` is a structural property of the table, and a partition holds no independent bucket-count attribute: a partition's bucket count is taken from the table-level `bucket.num` at the time the partition is created, but the partition metadata does not persist this value.

This causes two problems.

**1. No per-partition flexibility.** In a typical time-partitioned table, the data volume per partition grows over time. Earlier partitions may be fine with 4 buckets while current ones need 32. Because `bucket.num` is table-level, users must size the table for its future peak from day one, over-provisioning every historical partition, or accept that recent partitions are under-bucketed and become the write/read bottleneck.

**2. Rescaling is expensive.** Before this FIP, Fluss did not support changing the bucket count. The only way was to create a new table at the new bucket count and migrate all data, which is the most primitive and least efficient method: it redistributes all data, including historical partitions already tiered to the lake that should never be rewritten. For a large table this is a long, high-risk migration that also breaks the lake-side bucket layout of already-committed data.

In a real Fluss scenario, the problem we need to solve:

> **Scenario.** A Fluss table's bucket count needs to be adjusted at runtime to match changing data volumes, but historical partitions were written at the original bucket count and should not be changed.

> **Requirement.** As a Fluss user, I want to dynamically modify a table's bucket configuration, so that new partitions are written at the new configuration while historical partitions are left untouched, enabling flexible storage and performance tuning without rewriting old data.

This FIP answers that requirement directly: rescale can now act only on partitions created after the change, with existing partitions kept exactly as they are. This is sound because a partition's bucket layout only needs to be self-consistent — nothing requires two partitions of the same table to be bucketed identically, as long as every reader and writer routes by the partition's own bucket count.

This FIP makes the bucket count a partition-level attribute:

`ALTER TABLE ... SET ('bucket.num' = N)` affects only partitions created afterwards; existing partitions keep their bucket count and their data is not touched. This rule is inherently partition-scoped, so a non-partitioned table has nothing for it to act on.

### Goals

* A partitioned table may have partitions with different bucket counts.

* `ALTER TABLE ... SET ('bucket.num' = N)` is an online, data-movement-free metadata operation, supported in both directions: scaling up to spread load over more buckets, and scaling down to release the replicas and RocksDB instances that surplus buckets occupy.

* The user-facing surface stays table-level only: `ALTER TABLE ... SET ('bucket.num' = N)`.

* Every read, write, lookup, tiering and union-read path routes by the target partition's actual bucket count.

* A client holding stale metadata can never silently write a record into the wrong bucket; it must fail explicitly.


### Non-Goals

* This FIP does not consider rewriting existing data.

* Lake formats other than Paimon. Only Paimon is considered here; other formats will be considered later.


## New or Changed Public Interfaces

### RPC / Proto changes

All new fields are `optional`, so old and new peers remain wire-compatible.

Table-level `bucket_layout_epoch`:

| Message | Field |
| --- | --- |
| `PbTableMetadata` | `optional int64 bucket_layout_epoch` |
| `GetTableInfoResponse` | `optional int64 bucket_layout_epoch` |

Partition-level `bucket_count_actual`:

| Message | Field |
| --- | --- |
| `PbPartitionMetadata` | `optional int32 bucket_count_actual` |
| `PbPartitionInfo` | `optional int32 bucket_count_actual` |

Bucket-routed requests (the value used to compute `bucket_id`):

| Message | Field |
| --- | --- |
| `PbProduceLogReqForBucket` | `optional int32 bucket_count_actual` |
| `PbPutKvReqForBucket` | `optional int32 bucket_count_actual` |
| `PbFetchLogReqForBucket` | `optional int32 bucket_count_actual` |
| `PbLookupReqForBucket` | `optional int32 bucket_count_actual` |
| `PbPrefixLookupReqForBucket` | `optional int32 bucket_count_actual` |
| `PbScanReqForBucket` | `optional int32 bucket_count_actual` |
| `PbTableStatsReqForBucket` | `optional int32 bucket_count_actual` |
| `LimitScanRequest` | `optional int32 bucket_count_actual` |
| `ListOffsetsRequest` | `optional int32 bucket_count_actual` |

### New error codes

| Name | Exception | Meaning |
| --- | --- | --- |
| `STALE_METADATA` | `StaleMetadataException` | The `bucket_count_actual` in the request does not match the server's actual `bucket_count_actual`. The client must refresh metadata before retrying. |
| `TABLET_METADATA_NOT_READY` | `TabletMetadataNotReadyException` | The TabletServer has not yet received metadata for the requested table or partition. The client should retry the same request without refreshing metadata or rebuilding the bucket assigner, because the server simply has not caught up yet — the client's `bucket_count_actual` may be correct. |

`STALE_METADATA` and `TABLET_METADATA_NOT_READY` have different semantics: the former means the server has confirmed that the client's `bucket_count_actual` is wrong, and the client must refresh metadata; the latter means the server has not yet received metadata and cannot make a judgment, so the client should retry directly.

### ZooKeeper metadata format

| Znode | Change |
| --- | --- |
| `PartitionRegistration` (`/.../partitions/<name>`) | Adds optional `bucket_count_actual`; serde `VERSION` bumped **1 → 2**. Absent in v1 data → read as `null`. |
| `TableRegistration` (`/.../tables/<table>`) | Adds `bucket_layout_epoch`; serde `VERSION` bumped **1 → 2**. Absent in v1 data → read as `0`, meaning the table has never been ALTERed. |

### Configuration and DDL

No new configuration options are introduced. Two existing surfaces change meaning:

* `bucket.num` **semantics.** Previously "the number of buckets used by a Fluss table". Now: "the target bucket count for a Fluss table; for partitioned tables, it applies to newly created partitions, which are built using the new `bucket.num`; existing partitions retain their original bucket count."

* `bucket.num` **becomes alterable.** It is removed from the Flink connector's `ALTER_DISALLOW_OPTIONS`, enabling:


```sql
ALTER TABLE my_partitioned_table SET ('bucket.num' = '8');
```

## Proposed Change

### A. Introducing the partition-level `bucket.num.actual` semantic

This FIP splits the table-level and partition-level bucket count into two attributes, structurally symmetric but differing only in writability and query method:

| | `bucket.num` | `bucket.num.actual` |
| --- | --- | --- |
| Scope | Table | Partition |
| Meaning | **Target**: the intended end-state bucket count, used as the template for partitions created from now on | **Actual**: the bucket count a specific partition really uses |
| Writable by users | Yes, via DDL (`CREATE TABLE`, `ALTER TABLE ... SET`) | No, derived by the server at partition creation |
| Persisted as | `bucket_count` in the table znode | `bucket_count_actual` in the partition znode |
| Readable by users | Yes (`DESCRIBE`, connector options) | Via a stored procedure that inspects a partition's `bucket.num.actual` |

Both are `int` structural fields; the table-level one is serialized as `bucket_count` in the znode JSON, the partition-level one as `bucket_count_actual`, neither stored in the property map. `bucket.num` additionally serves as a Flink connector option key `bucket.num` exposed to users via `WITH ('bucket.num' = N)`; `bucket.num.actual` has no connector-side key and cannot be set directly by users. The difference between the two lies only in writability and query method.

#### A.1 Consistency rule

After an `ALTER`, for newly created partitions, `bucket.num.actual == bucket.num`, because a new partition copies the table-level `bucket.num` at creation time, and the table-level value has already been updated to the target value N by the ALTER.

For pre-existing partitions, `bucket.num.actual != bucket.num`, because these partitions were created before the ALTER, their `bucket.num.actual` copied the pre-ALTER table-level value, and the table-level value has now changed. This divergent state persists until the partition's data is rewritten (by a future FIP) and they reconverge.

#### A.2 Inheritance / snapshot semantics

A partition copies the table-level `bucket.num` once, at the instant of its creation, and persists it as its own `bucket.num.actual`. From then on that partition's `bucket.num.actual` is immutable until its data is rewritten.

This means: subsequent changes to the table-level `bucket.num` do not affect the `bucket.num.actual` of existing partitions — each partition "freezes" the table-level value at the moment of its creation. This is the core mechanism that enables "new partitions use the new bucket count, old partitions keep the old bucket count": no migration is needed for old partitions, they naturally stay unchanged.

Typical timeline:

1. Create table with `bucket.num=4` → every partition created in this period carries `bucket.num.actual=4`

2. User runs `ALTER TABLE ... SET ('bucket.num' = '8')` → table-level `bucket.num` becomes 8

3. Every partition created afterwards carries `bucket.num.actual=8`, copied from the new table-level value

4. Two generations of partitions (4-bucket and 8-bucket) coexist indefinitely in the same table, and read/write paths route by each partition's own `bucket.num.actual`


### B. The ALTER operation: modifying the table-level bucket num property

The entire ALTER is divided into two phases: lake-first propagation and Fluss-side ZK transaction. The two are not atomic across systems, but the design guarantees eventual consistency: lake propagation is idempotent and safe to re-run, and the Fluss-side old-partition backfill plus table-level update are committed atomically in a single ZK transaction.

#### B.1 Lake-first propagation

If the table is lake-enabled (`table.datalake.enabled = true`) and the bucket key is non-empty, the coordinator first propagates the new bucket count to the lake side, then modifies the Fluss side. This follows the existing lake-first ALTER flow: a lake failure aborts the ALTER with the Fluss side unchanged, and because propagation is idempotent, re-running the same ALTER after a Fluss-side failure converges both sides.

For Paimon, the coordinator calls the lake catalog's alterTable interface, changing the Paimon table schema's `CoreOptions.BUCKET` to the target value N. This causes Paimon to use the new bucket count when creating new partitions.

Propagation is idempotent: repeatedly propagating the same N value produces no side effects — Paimon's `BUCKET` option is set to the same value, and repeated setting does not change the layout of existing partitions.

On propagation failure (lake unreachable, lake catalog exception, table not found), the ALTER is aborted and the Fluss side is unchanged. The user re-runs the same ALTER; lake propagation starts from scratch, and because it is idempotent, no dirty state is produced.

Iceberg / Hudi / Lance do not yet support this propagation; `alterTable` throws `UnsupportedOperationException` and the ALTER is rejected.

Non-lake-enabled tables or tables with an empty bucket key skip this phase; no lake propagation is needed.

#### B.2 Old-partition backfill

Partitions that existed before the ALTER ("old partitions") may not have persisted their own `bucket.num.actual` — in particular, partitions created by versions predating this FIP have no persisted bucket count in their registration. The purpose of backfill is to populate `bucket.num.actual` for these partitions, using their current actual bucket count, inferred from the partition's existing bucket assignment, rather than the new table-level value N.

This ensures that old partitions continue to run on their original layout after the ALTER — their `bucket.num.actual` is "frozen" at the pre-ALTER value, rather than changing along with the table-level value.

Backfill is idempotent: partitions that already have a `bucket.num.actual` are skipped. Therefore, if the ALTER is retried, partitions that have already been backfilled are not processed again.

Backfill computation: for each existing partition without a persisted `bucket.num.actual`, read its registration together with its znode version, and infer the bucket count from the partition's existing bucket assignment. If a partition is listed but its registration or assignment is unreadable, abort the entire ALTER — a partial backfill would leave that partition routed by the new table-level value, which is exactly the corruption this design exists to prevent.

The backfill results — each partition's registration and the corresponding znode version — are not committed separately, but merged with the Fluss-side table-level update in the same ZK transaction to guarantee atomicity.

#### B.3 Fluss-side atomic commit

Once backfill computation is complete, all partition backfills plus the new table registration (containing the new bucket count and `bucketLayoutEpoch + 1`) are written in a single ZooKeeper multi-op transaction.

Each znode write is CAS-guarded by the znode version captured at read time: if any znode was modified after it was read, CAS fails and the entire transaction rolls back. The entire transaction is also fenced by the coordinator-epoch znode: if the coordinator has been deposed (epoch mismatch), the transaction also fails.

Consequence: no partition can observe the new table-level bucket count without also having its own `bucket.num.actual`. Backfill and table-level update either take effect together or not at all; there is no intermediate state where "the table-level value has changed but an old partition does not yet have `bucket.num.actual`".

After a successful commit, if the table has auto-partitioning enabled, `AutoPartitionManager`'s cached `TableInfo` is refreshed so that newly auto-created partitions read the new bucket count fresh from ZK and stamp it into their own registration.

#### B.4 Concurrency control

ALTER `bucket.num` races with partition creation and partition deletion throughout the entire process of reading the partition list, computing backfill, and committing the ZK transaction. These two types of operations modify the same set of znodes; without coordination, inconsistency arises.

Consider the following scenario: ALTER reads the partition list `[P1, P2, P3]` and begins computing backfill; meanwhile a new partition `P4` is created and written to ZK; when ALTER's transaction commits, `P4` is not in the backfill list and therefore has no persisted `bucket.num.actual`. After ALTER commits, `bucketLayoutEpoch` becomes greater than 0, and `P4` hits the "`bucketLayoutEpoch` > 0 and no `bucket.num.actual`" branch, throwing `StaleMetadataException` — the partition becomes unreadable and unwritable until manually repaired. Conversely, if ALTER has committed and the table-level bucket count has changed to N, but an in-progress partition-creation request still reads the old value 4, the new partition gets stamped with 4 — this is self-consistent (it is a pre-ALTER partition), but such unpredictable behavior is not allowed.

Therefore ALTER and partition creation/deletion are mutually exclusive: no partition creation or deletion is allowed during ALTER's commit, and vice versa. There is no scenario where partition creation reads an intermediate value while ALTER is in progress. Partition creations can still run concurrently with each other, because they do not affect each other's znodes. Partition creation reads the table-level `bucket.num` fresh from ZK and stamps it into the new partition's registration, rather than using a potentially stale cache, ensuring the value read is always the current committed value.

#### B.5 Failover and retry

**Consistency guarantee:** ALTER is divided into lake propagation and Fluss-side ZK transaction, two phases that are not atomic across systems. The design guarantees correctness through idempotency + bounded retry + eventual consistency.

* Lake propagation failure: the entire ALTER has no effect. Lake propagation executes before the Fluss ZK transaction. If the lake is unreachable or the lake catalog throws, the ALTER is aborted directly and the Fluss-side ZK transaction is not executed. Neither side is changed; full rollback. The user re-runs the same ALTER.

* **Lake propagation succeeds but Fluss ZK fails:** sides diverge, error reported, user asked to re-run. The lake side has been changed to `bucket.num` = N, but the Fluss side is unchanged (`bucket.num` = old value, `bucketLayoutEpoch` unchanged). The ZK transaction is automatically retried, up to 3 times. After 3 retries, a `FlussRuntimeException` is thrown to alert the user. When the user manually re-runs the same ALTER, the lake is re-propagated, but lake propagation is idempotent and produces no side effects (repeatedly setting the same N value). The Fluss-side ZK retries the commit. If the user does not re-run, correctness is not affected (the tiering writer uses the Fluss-side partition-level bucket count to override Paimon's BUCKET), but the two sides remain inconsistent.

* The tiering service touches both sides during read/write — it reads the Fluss metadata's table-level bucket count and the Paimon schema; if the two are inconsistent, it alerts the user to re-run the ALTER so both sides converge.


**Idempotency guarantee:** all three parts are individually idempotent, so the same ALTER can be safely re-run and both sides eventually converge:

* **Lake propagation idempotent:** repeatedly setting Paimon's `CoreOptions.BUCKET` to the same N value produces no side effects.

* **Backfill idempotent:** when iterating partitions during backfill, partitions that already have a `bucket.num.actual` are skipped. A backfill that did not complete on the first run continues where it left off on retry.

* **Fluss-side ZK transaction idempotent:** the table-level update writes an absolute target value N (not an incremental operation), CAS-guarded. If the previous run already committed, a re-run reads the new state, `bucket.num` is unchanged (still N), and CAS succeeds. Although `bucketLayoutEpoch` is incremented by an extra +1, `bucketLayoutEpoch`'s most important role is the "has been ALTERed" flag, and an extra increment does not affect correctness here.


**Atomicity guarantee:** backfill + table-level update are committed in the same ZK multi-op transaction; either both take effect or neither does. No partition can observe an intermediate state where "the table-level value has changed but it does not yet have `bucket.num.actual`".

**Coordinator crash recovery:**

| Scenario | Recovery strategy |
| --- | --- |
| ZK transaction sent, coordinator crashes before receiving response | On restart, the coordinator reads the current state from ZK to determine whether it committed: read the table registration; if `bucket.num` = new value N and `bucketLayoutEpoch` = old+1, it is committed, ALTER is complete; if `bucket.num` = old value and `bucketLayoutEpoch` = old value, it is uncommitted, ALTER did not take effect, user re-runs. |
| ZK transaction succeeded, then coordinator crashes | ZK transaction is committed (`bucket.num` = N, `bucketLayoutEpoch` = old+1, all partition backfills complete). `AutoPartitionManager` refreshes on the next event-processing cycle. No special handling needed. |

### C. The routing contract for read and write paths

#### C.1 Why bucket-routed requests must carry `bucket_count_actual`

The client computes `bucket_id` using `hash(key) % bucket.num.actual`. If the client uses a stale `bucket.num.actual`, the resulting `bucket_id` still falls within `[0, actual bucket count)` and is still a valid `bucket_id` — the server accepts it, but the record lands in the wrong bucket. This is a silent mis-route: invisible and irreparable after the fact.

The fix: make `bucket_count_actual` part of the request. Every bucket-routed request gains an `optional int32 bucket_count_actual` proto field, declaring the value the client used to compute `bucket_id`. The server compares it with the partition's actual bucket count; a mismatch returns `STALE_METADATA`, turning the silent mis-route into an explicit failure.

#### C.2 Write path

| Component | Role on the write path |
| --- | --- |
| `WriterClient` | Gets the target partition's bucket count from cluster metadata to assign buckets |
| `WriteBatch` | Anchors the bucket count at creation; uses the same value throughout the batch's lifecycle |
| `DynamicPartitionCreator` | Synchronously waits for the partition-level bucket count to become visible before proceeding |
| Request construction | Takes the anchored bucket count from the batch and fills the `bucket_count_actual` proto field |
| `Sender` | On `STALE_METADATA`, fails the batch and invalidates metadata; does not retry |

**Why `WriteBatch` must anchor the bucket count.** Cluster metadata is a volatile immutable snapshot, replaced wholesale on refresh. If the metadata is refreshed between computing `bucket_id` and filling `bucket_count_actual`, the batching window may straddle an ALTER: `bucket_id` is computed from the old layout, while `bucket_count_actual` carries the new layout's value, and the request declaration no longer matches the actual routing. Anchoring guarantees both come from the same snapshot.

**Why dynamic partition creation must wait synchronously.** Before this FIP, all partitions had the same bucket count as the table-level value, so dynamic partition creation could be asynchronous: the writer issues `createPartition` and immediately uses the table-level bucket count to assign buckets, without waiting for partition metadata to return. After an ALTER, a new partition's bucket count may differ from the table-level value, so the writer can no longer use the table-level count — it must wait until the partition-level bucket count is visible in the metadata before it can correctly assign buckets. Multiple writers targeting the same new partition share a single creation request and all wait for the same metadata refresh.

#### C.3 Read path

| Component | Role on the read path |
| --- | --- |
| `Lookup` resolution | Gets the target partition's bucket count from cluster metadata |
| `LookupBatch`, `PrefixLookupBatch` | Anchor the bucket count at construction; request builder no longer re-reads metadata |
| Request construction | Fills the `bucket_count_actual` proto field when the bucket count is positive |
| `Scanner` | Reads the bucket count live on each request; no anchoring |
| `ListOffsets`, `TableStats` | Also read the bucket count live |
| `LookupSender` | On `STALE_METADATA`, fails the batch and invalidates metadata; does not retry |

Lookup also batches, so the anchoring rationale is the same as `WriteBatch`. Scanners do not batch, so there is no window between reading the value and sending the request in which an ALTER could occur; live read suffices.

**Historical-partition lookup is resolved by the server.** A historical partition has already been dropped from Fluss; its data lives only in the lake, laid out with the bucket count in effect when it was tiered. After an ALTER, neither the table-level count nor any partition-level `bucket.num.actual` describes this value — only the lake metadata still knows it. The server therefore resolves the tiered bucket count from the original partition's lake snapshot, locates the lake bucket the key belongs to under that count, and reads it. The client only supplies the key and the original partition name; it does not compute `bucket_id`, and `bucket.num.actual` validation does not apply to historical-partition lookup requests.

#### C.4 Bucket enumeration becomes partition-level

Every place that enumerated `[0, the table-level bucket.num)` for a partitioned table now enumerates `[0, the partition-level bucket.num.actual)`:

| Layer | Component | Enumeration scenario |
| --- | --- | --- |
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

### D. Routing validation

#### D.1 Validation approach

When the server receives a bucket-routed request, it needs to determine whether the client's `bucket_count_actual` is stale. Validation operates at two levels:

First, when the request carries `bucket_count_actual`: the server compares it with the actual bucket count it holds for that partition; a mismatch returns `STALE_METADATA`. This directly prevents a `bucket_id` computed with a stale bucket count from being silently accepted.

Second, when the request does not carry `bucket_count_actual` (old clients): the server needs to distinguish whether the table has been ALTERed. If it has never been ALTERed, all partitions' bucket counts equal the table-level value, and omitting `bucket_count_actual` is safe and can be accepted. If it has been ALTERed, the table-level value no longer represents all partitions' actual bucket counts, and requests without `bucket_count_actual` must be rejected with `STALE_METADATA`.

`bucketLayoutEpoch` is this distinguishing flag: 0 means never ALTERed, greater than 0 means ALTERed. It is advanced only on bucket-count changes, and orders only bucket-layout changes — it is not a general metadata version: two metadata updates with the same `bucketLayoutEpoch` may carry different schema, partition, or replica metadata. When the TabletServer receives a lower `bucketLayoutEpoch`, it discards that table's update, preventing an older bucket layout from replacing the one committed by a later ALTER.

The server only judges a mismatch when it actually knows the target bucket count. When the snapshot lacks the partition's or table's bucket count, the request is allowed through, to avoid spurious rejections from incomplete snapshots during metadata propagation; if the client can fill in a partition-level bucket count, the value must have come from the server's metadata, so allowing it does not introduce a mis-route. The same applies when a TabletServer has just started and has not yet received metadata: the snapshot is empty, no mismatch judgment is made, and requests pass through.

#### D.2 Why validation is needed

If the server cannot distinguish "the table has never been ALTERed" from "the table has been ALTERed", there are only two choices:

* Always accept requests without `bucket_count_actual`: after an ALTER, old clients route old partitions using the table-level value — silent mis-route

* Always reject requests without `bucket_count_actual`: even tables that have never been ALTERed reject old clients — not backward-compatible


Both are unacceptable. `bucketLayoutEpoch` of 0 allows omitting `bucket_count_actual` (the table-level value is correct for all partitions), while greater than 0 requires rejection (the table-level value is no longer guaranteed to equal old partitions' bucket counts).

The key point is "not guaranteed", not "necessarily different". The table-level bucket count has an ABA form: after changing from 4 to 8 and back to 4, the 4-generation partitions' bucket count happens to equal the current table-level value, the 8-generation does not, and both generations coexist in the same table. Equality is coincidental; the table-level value cannot tell which generation a partition belongs to. Therefore, once `bucketLayoutEpoch > 0`, fallback to the table-level value is never done; only the partition's own `bucket.num.actual` is used for routing.

#### D.3 Example

**Without validation.** The table's `bucket.num` was originally 4, ALTER changes it to 6. An old client has cached the table-level value 4; when writing to a partition created after the ALTER (actual 6 buckets), it computes `bucket_id` using `hash(key) % 4`, resulting in a value in the 0-3 range. Under the 6-bucket layout, `bucket_id` values 0-3 all exist, so the server cannot detect the problem via `bucket_id` existence — the record lands in the wrong bucket, causing a silent mis-route.

**With validation.** After the ALTER, `bucketLayoutEpoch` becomes greater than 0. The old client's request does not carry `bucket_count_actual`; the server sees `bucketLayoutEpoch > 0` and the request does not carry `bucket_count_actual`, so it returns `STALE_METADATA`. The client refreshes metadata, obtains the partition-level bucket count 6, recomputes `bucket_id` using `hash(key) % 6`, and routes correctly.

## Migration Plan and Compatibility

### Upgrade contract

**1. `bucket.num` cannot be changed during the rolling-upgrade window.** During a rolling upgrade, old and new TabletServers coexist, and old TabletServers do not have `bucket_count_actual` validation capability. If an ALTER is executed at this time, old clients using the new table-level bucket count to route to old partitions cannot be rejected by old TabletServers, potentially causing silent mis-route. Therefore ALTER must not be executed until all TabletServers have been upgraded. This is an operational constraint that users must follow: do not execute `ALTER TABLE ... SET ('bucket.num' = N)` during a rolling upgrade. Server-side enforcement (coordinator checking the minimum version of all live TabletServers before executing ALTER) is left as a goal for a future FIP.

**2. It is recommended to upgrade clients first, then the cluster.** If the cluster (server) is upgraded before the clients:

* Old clients can continue to operate while the table has not been ALTERed (`bucketLayoutEpoch`=0) — the server accepts requests without `bucket_count_actual`

* Once the table is ALTERed (`bucketLayoutEpoch`>0), old clients' requests are explicitly rejected by the server with `STALE_METADATA` — old clients cannot read or write the table until upgraded


If clients are upgraded first:

* New clients are fully compatible with old clusters (new proto fields are optional, old servers ignore them)

* After cluster upgrade, seamless transition with no additional steps


**3. New clients seamlessly support both old-only and new-only clusters.**

* New client → old cluster: new proto fields are ignored by old servers; responses omit `bucketLayoutEpoch` / `bucket_count_actual`, the client reads `bucketLayoutEpoch`=0 and falls back to the table-level bucket count. Equivalent to old-client behavior.

* New client → new cluster: fully compatible, all features available.


### Compatibility matrix

Broken down by server version, client version, and table `bucketLayoutEpoch`:

| Server | Client | `bucketLayoutEpoch` | Behavior |
| --- | --- | --- | --- |
| New | New | 0 | Compatible. At `bucketLayoutEpoch`=0 all partition bucket counts equal the table-level value; the client passes validation with or without `bucket_count_actual`. |
| New | New | `>0` | Compatible. New client carries `bucket_count_actual`; server validates and passes. If the client's metadata is stale, `STALE_METADATA` is returned; client refreshes and recovers. |
| New | Old | 0 | Compatible. Old client omits `bucket_count_actual`; `bucketLayoutEpoch`=0 allows omission. Table-level bucket count is correct for all partitions. |
| New | Old | `>0` | Incompatible. Old client omits `bucket_count_actual`; `bucketLayoutEpoch`>0 rejects with `STALE_METADATA`. Old client cannot read or write the table until upgraded. |
| Old | New | N/A | Compatible. New proto fields are ignored by old servers; client reads `bucketLayoutEpoch`=0, falls back to table-level. |
| Old | Old | N/A | Status quo, unchanged. |

**Core rule:** whether `bucket_count_actual` may be omitted is determined by `bucketLayoutEpoch`. At `bucketLayoutEpoch`=0, omission is safe (the table-level bucket count is correct for all partitions); at `bucketLayoutEpoch`>0, omission is rejected (the table-level bucket count no longer equals old partitions' bucket counts). Client newness only indirectly affects "whether `bucket_count_actual` is filled"; the server does not infer client versions, it only looks at the request content and `bucketLayoutEpoch`.

### Upgrade procedure

1. **Upgrade all clients / connectors.** New clients are compatible with old clusters and immediately usable after upgrade.

2. **Upgrade servers** (CoordinatorServer + TabletServers). Do not execute `ALTER TABLE ... SET ('bucket.num' = N)` during the rolling upgrade.

3. **After all servers are upgraded**, `ALTER TABLE ... SET ('bucket.num' = N)` may be used.


### Known limitations

* **`bucket.num` cannot be changed during the rolling-upgrade window.** Old TabletServers do not have `bucket_count_actual` validation capability and cannot reject old-client requests using stale bucket counts, potentially causing silent mis-route. Users must ensure that ALTER is not executed until all servers are upgraded. Server-side enforcement is left as a goal for a future FIP.

* **Downgrade after an ALTER is unsafe.** `TableRegistration` and `PartitionRegistration` deserializers do not validate the version and ignore unknown fields, so an older server reading the new znode would drop `bucketLayoutEpoch` and `bucket_count_actual` and then route every partition by the table-level count. Downgrading a cluster that has ALTERed any table is therefore not supported.

* Rescaling existing partitions is not yet supported (to be addressed by a future FIP).

## Test Plan

ALTER semantics and atomicity tests verify: ALTER on a non-partitioned table is rejected; `bucket.num` less than 1 or greater than `max.bucket.num` is rejected; backfill idempotency, where only partitions lacking `bucket.num.actual` are processed and those with a value are skipped; the entire ALTER is aborted when a partition's registration is unreadable during backfill; ALTER and partition creation are mutually exclusive, so concurrent partition creation does not cause ALTER to miss a new partition; the table-level update and backfill are committed in the same ZK transaction, and a CAS failure on any znode causes a full rollback; the coordinator crash recovery path before and after the ZK transaction.

End-to-end read/write tests verify: after ALTER, newly created partitions are written at the new bucket count and existing partitions are read and written at the old count; both log tables and primary-key tables are covered; dynamically created partitions automatically adopt the post-ALTER bucket count; a client with stale metadata receives `STALE_METADATA` when writing to a newly created partition and recovers after refreshing; the scale-down direction is also covered.

Union-read tests verify: after ALTER, the bucket counts of Fluss and lake-side partitions are each correct, and union-read enumeration uses the partition-level bucket count rather than the table-level value; when lake bucket ids fall outside the enumerated range, a fail-loud exception is thrown rather than silently dropping data; Flink batch and streaming, and Spark are all covered, including a lake-only expired partition after ALTER.

Metadata and serialization tests verify: `PartitionRegistration` serialization v2 new fields read and write correctly; v1 legacy data without `bucket_count_actual` reads as `null`; `TableRegistration` serialization v2 new fields read and write correctly; v1 legacy data without `bucket_layout_epoch` reads as 0; `TabletServerMetadataCache` `bucketLayoutEpoch` ordering, where a lower `bucketLayoutEpoch` `UpdateMetadata` is discarded; validation behavior before the first `UpdateMetadata`.

Client error handling tests verify: `Sender` fails the batch on `STALE_METADATA` without re-enqueuing, invalidating metadata and `BucketAssigner`; `LookupSender` fails the batch on `STALE_METADATA` without entering the retry loop; the retry path for `TABLET_METADATA_NOT_READY` is correctly distinguished from the fast-fail path for `STALE_METADATA`.

Tiering tests verify: per-partition bucket count stamping is correct across multiple tiering rounds; the lake writer receives the correct partition-level bucket count at initialization; an alert is triggered when Fluss's `bucket.num` and the Paimon schema's `CoreOptions.BUCKET` are inconsistent.


## Rejected Alternatives

**1. Keep the existing table-level `bucket.num` and rescale by rewriting all data.**

Method: keep the bucket count as a table-level fixed property; create a new table at the new bucket count and rewrite all data.

Rejection reasons:

* Rewrites historical partitions that nobody wanted to change, including data already tiered to the lake that should never be rewritten.

* Breaks the lake-side bucket layout of already-committed data.

* For large tables, this is a long, high-risk migration.

* The core observation of this FIP is that a partition's bucket layout only needs to be internally self-consistent; different partitions of the same table can use different bucket counts. Rewriting cannot target only new partitions — old partitions are forced to be rewritten along with them, even though they do not need to change.


**2. Use client version or a boolean flag to replace `bucketLayoutEpoch`.**

Method: reject old clients' requests by client protocol version; or persist a boolean flag recording whether an ALTER has occurred.

Rejection reasons:

* Rejecting by client version would immediately reject all old clients after a server upgrade, even if the table has never been ALTERed.

* A boolean flag can record whether an ALTER has occurred, but two UpdateMetadata messages produced by two ALTERs both carry `true`, and the TabletServer cannot tell which is older. If a message carrying a stale table-level value arrives late, the boolean cannot have it ignored. A monotonically increasing `bucketLayoutEpoch` solves this via numeric comparison.