# FIP-XXX: Support Per-Partition Bucket Count

|            |                                                                  |
|------------|------------------------------------------------------------------|
| **当前状态**   | 讨论中（Under Discussion）                                            |
| **讨论线程**   | TBD（`[DISCUSS] FIP-XXX Support Per-Partition Bucket Count`）      |
| **Issue**  | [apache/fluss#3907](https://github.com/apache/fluss/issues/3907) |
| **参考实现**   | [apache/fluss#3908](https://github.com/apache/fluss/pull/3908)   |
| **发布版本**   | \<Fluss Version\>                                                |


## Motivation

目前，Fluss 表及其所有分区共用一个在建表时确定的 bucket 数。`bucket.num` 是表的结构化属性，分区本身不持有任何独立的 bucket 数属性：一个分区的 bucket 数仅仅是生成分区 assignment 时的表级 `bucket.num`，而元数据里并没有记录它。

这导致两个问题。

**1. 没有分区级灵活性。** 在典型的时间分区表里，每个分区的数据量随时间增长。昨天的分区可能 4 个 bucket就够了，今天可能需要 32 个。由于 bucket 数是表级的，用户必须从建表第一天就按未来峰值来设定，导致每个历史分区都过度配置；或者接受近期分区 bucket 不足、成为读写瓶颈。

**2. 改变 bucket 数代价高昂。** 在本 FIP 之前，Fluss 不支持改变 bucket 数，唯一办法是按新 bucket 数新建一张表，然后把数据迁移到新表——这是最原始、效率最低的方法：要重分布**所有**数据，包括已经 tiered 到湖里、本不该被重写的历史分区。对大表来说这是一次高风险的迁移，还会破坏已提交数据的湖侧 bucket 布局。

在一个真实 Fluss 场景，我们需要解决的问题：

> **应用场景。** Fluss 表在运行过程中需要调整 bucket 数量以适配数据量变化，但历史分区数据已按原 bucket 数写入，不宜变更。
>
> **需求描述。** 作为 Fluss 用户，希望能动态修改表的 bucket 配置，使新分区按新配置写入而历史分区保持不变，从而实现灵活的存储与性能调优，无需重写旧数据。

本 FIP 直接回应了这一需求：rescale 现在可以**只作用于变更后新建的分区**，已有分区原样保留。这在逻辑上是成立的，因为一个分区的 bucket 布局只需要自身自洽——没有任何东西要求同一张表的两个分区必须采用相同的 bucket 布局，只要每个读写路径都按*分区自己的* bucket 数来路由即可。

本 FIP 将 bucket 数变为**分区级**属性：

`ALTER TABLE ... SET ('bucket.num' = N)` 后，只影响之后创建的分区；已有分区保持其 bucket 数不变、旧数据不被触碰。这条规则本质上是分区维度的，非分区表没有分区维度可供它作用。

### Goals

- 分区表可以拥有不同 bucket 数的分区。
- `ALTER TABLE ... SET ('bucket.num' = N)` 是一个在线的、无数据移动的元数据操作，双向都支持——扩容以将负载分散到更多 bucket，缩容以释放多余 bucket 占用的副本和 RocksDB 实例。
- 用户操作面**只有表级**：`ALTER TABLE ... SET ('bucket.num' = N)`。不引入分区级 `ALTER` 语法。
- 每条读、写、lookup、tiering 和 union-read 路径都按目标分区的实际 bucket 数路由。
- 持有过期元数据的客户端绝不会静默地将记录写入错误的 bucket；它必须显式失败。

### Non-Goals

- 本 FIP 不考虑旧数据重刷。
- 非 **Paimon** 的湖格式。本 FIP 只考虑 Paimon；其他格式后续考虑。


## New or Changed Public Interfaces

### RPC / Proto 变更（`FlussApi.proto`）

所有新字段都是 `optional`，新旧 peer 保持 wire 兼容。

表级 epoch：

| 消息                     | 字段                                       |
|------------------------|------------------------------------------|
| `PbTableMetadata`      | `optional int64 bucket_layout_epoch = 9` |
| `GetTableInfoResponse` | `optional int64 bucket_layout_epoch = 7` |

分区级 bucket count：

| 消息                    | 字段                                |
|-----------------------|-----------------------------------|
| `PbPartitionMetadata` | `optional int32 bucket_count = 5` |
| `PbPartitionInfo`     | `optional int32 bucket_count = 4` |

桶路由请求（客户端计算 `bucket_id` 时使用的值）：

| 消息                           | 字段                                |
|------------------------------|-----------------------------------|
| `PbProduceLogReqForBucket`   | `optional int32 bucket_count = 4` |
| `PbPutKvReqForBucket`        | `optional int32 bucket_count = 4` |
| `PbFetchLogReqForBucket`     | `optional int32 bucket_count = 5` |
| `PbLookupReqForBucket`       | `optional int32 bucket_count = 5` |
| `PbPrefixLookupReqForBucket` | `optional int32 bucket_count = 4` |
| `PbScanReqForBucket`         | `optional int32 bucket_count = 5` |
| `PbTableStatsReqForBucket`   | `optional int32 bucket_count = 3` |
| `LimitScanRequest`           | `optional int32 bucket_count = 6` |
| `ListOffsetsRequest`         | `optional int32 bucket_count = 7` |

### 错误码

| 码  | 名称               | 异常                       | 含义                                                        |
|----|------------------|--------------------------|-----------------------------------------------------------|
| 74 | `STALE_METADATA` | `StaleMetadataException` | 请求中的 bucket count 和服务端的实际 bucket count 不匹配。客户端必须刷新元数据后重试。 |


### ZooKeeper 元数据格式

| Znode                                             | 变更                                                                                                             |
|---------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `PartitionRegistration`（`/.../partitions/<name>`） | 新增 optional `bucket_count`；serde `VERSION` 从 **1 → 2**。v1 数据中无此字段 → 读为 `null`，通过 `getBucketCountOrDefault` 解析。 |
| `TableRegistration`（`/.../tables/<table>`）        | 新增 `bucket_layout_epoch`；serde `VERSION` 从 **1 → 2**。v1 数据中无此字段 → 读为 `0`，即该表从未被 ALTER 过。                       |

### 配置和 DDL

不引入新的配置选项。两个已有面变化含义：

- **`bucket.num` 语义。** 之前是"Fluss 表的 bucket 数"。现在是"目标 bucket 数；对分区表，它适用于新建分区，已有分区保留原来的 bucket count。"
- **`bucket.num` 变为可 alter。** 从 Flink connector 的 `ALTER_DISALLOW_OPTIONS` 中移除，启用：

  ```sql
  ALTER TABLE my_partitioned_table SET ('bucket.num' = '8');
  ```


## Proposed Change

### A. 引入分区级别的 `bucket.num.actual` 的语义

本 FIP 将表级别和分区级别的 bucket 数拆为两个属性，两者结构上对称、仅可写性和查询方式不同：

|      | `bucket.num`                                   | `bucket.num.actual`                               |
|------|------------------------------------------------|---------------------------------------------------|
| 作用域  | 表                                              | 分区（对非分区表，同时拥有 `bucket.num` 和 `bucket.num.actual`） |
| 含义   | **目标值**：预期的终态 bucket 数，作为之后新建分区的模板             | **实际值**：某个分区当前真正使用的 bucket 数                      |
| 用户可写 | 是，通过 DDL（`CREATE TABLE`、`ALTER TABLE ... SET`） | 否，服务端在分区创建时派生                                     |
| 持久化为 | `TableRegistration.bucketCount`                | `PartitionRegistration.bucketCount`               |
| 用户可读 | 是（`DESCRIBE`、connector options）                | 通过存储过程查询分区或非分区表的 `bucket.num.actual`              |

两者都是 `int` 结构化字段，都序列化为 znode JSON 的 `bucket_count`，都不在 property map 里。`bucket.num` 额外作为 Flink connector option key（`FlinkConnectorOptions.BUCKET_NUMBER`）暴露给用户通过 `WITH ('bucket.num' = N)` 设置；`bucket.num.actual` 没有 connector 侧的 key，用户不能直接设置。两者的区别仅在于可写性和查询方式。

#### A.1 一致性规则

`ALTER` 后，新建分区的 `bucket.num.actual == bucket.num`，因为新建分区在创建时从表级`bucket.num` 拷贝，而此时表级值已被 ALTER 更新为目标值 N。

已有分区的 `bucket.num.actual != bucket.num`，因为这些分区在 ALTER 之前就已经创建，它们的 `bucket.num.actual` 拷贝的是 ALTER 之前的表级值，而表级值现在已经变了。这种分叉状态持续到该分区的数据被重刷（由后续 FIP 完成）后才重新一致。

非分区表同时拥有两个属性。`ALTER` 后表的 `bucket.num` 和 `bucket.num.actual` 以同样的方式分叉：`bucket.num` 变成 N，`bucket.num.actual` 保持旧值，只有数据重刷（由后续 FIP 完成）才能让它们重新一致。

这个一致性规则是整个设计的基石：它确保了"哪些分区还是旧布局、哪些是新布局"在任何时刻都可以通过对比 `bucket.num` 和 `bucket.num.actual` 来判断，这也是存储过程查询`bucket.num.actual` 的意义，用户可以据此判断哪些分区需要做数据重分布。

#### A.2 继承 / 快照语义

分区在**创建瞬间**从表级 `bucket.num` **拷贝一次**并持久化为自己的 `bucket.num.actual`。此后该分区的 `bucket.num.actual` 不可变，直到其数据被重写。

这意味着：表级 `bucket.num` 的后续变更**不会**影响已存在分区的 `bucket.num.actual`，每个分区"冻结"了它创建那一刻的表级值。这正是"新分区用新桶数、旧分区保持旧桶数"得以实现的核心机制：不需要为旧分区做任何迁移，它们自然保持不变。

典型时间线：

1. 以 `bucket.num=4` 建表 → 该期间创建的每个分区都带 `bucket.num.actual=4`
2. 用户执行 `ALTER TABLE ... SET ('bucket.num' = '8')` → 表级 `bucket.num` 变为 8
3. 之后创建的每个分区都带 `bucket.num.actual=8`，从新表级值拷贝
4. 4 桶和 8 桶两代分区在同一张表里长期共存，读写路径按各自分区的 `bucket.num.actual` 路由

### B. ALTER 操作：修改表级 bucket num 属性

整个 ALTER 分为两个阶段：**湖优先传播**和 **Fluss 侧 ZK 事务**。两者跨系统非原子，但设计保证最终一致：湖传播幂等可安全重跑，Fluss 侧旧分区回填 + 表级更新在同一个 ZK事务里原子提交。

#### B.1 湖优先传播

如果表是开湖的（`table.datalake.enabled = true`）且 bucket key 非空，coordinator **先**向湖侧传播新 bucket 数，**再**修改 Fluss 侧。这沿用了已有的 `alterTableSchema` "湖优先"顺序：湖失败会中止 ALTER 且 Fluss 侧不变，而因为传播是幂等的，Fluss 侧失败后重跑同一 ALTER能让两侧收敛。

以 Paimon 为例，coordinator 调用 `PaimonLakeCatalog.alterTable`，将 Paimon 表 schema 的`CoreOptions.BUCKET` 改为目标值 N。这使 Paimon 之后新建分区时采用新桶数覆盖 `totalBuckets`。

传播是**幂等**的：将同一个 N 值重复传播不产生副作用，Paimon 的 `BUCKET` 选项被设为同一个值，重复设置不会改变已有分区的布局。

传播失败（湖不可达、湖 catalog 异常、表不存在）时，**ALTER 中止，Fluss 侧不变**。用户重跑同一 ALTER，湖传播从头开始，因为幂等，不会产生脏状态。

Iceberg / Hudi / Lance 不支持此传播，`alterTable` 直接抛 `UnsupportedOperationException`，ALTER 被拒绝。

非开湖表或 bucket key 为空的表跳过此阶段，不需要湖传播。

#### B.2 旧分区回填

ALTER 之前已经存在的分区，即"旧分区"，可能没有持久化自己的 `bucketCount`，特别是那些由本 FIP 之前的版本创建的分区，它们的 `PartitionRegistration.bucketCount` 是 `null`。回填的目的是为这些分区补上 `bucket.num.actual`，取值为它们**当前实际的** bucket 数，从 bucket assignment size 派生，而不是新表级值 N。

这保证了旧分区在 ALTER 后仍然按其原有布局运行，它们的 `bucket.num.actual` 被"冻结"在ALTER 之前的值，而不是跟着表级值一起变。

回填是**幂等**的：已有 `bucketCount` 的分区跳过。因此如果 ALTER 被重试，已经回填过的分区不会被重复处理。

回填的计算流程：对每个没有持久化 `bucketCount` 的已有分区，连同其 znode version 一起读取注册信息，从 bucket assignment size 派生桶数。如果某个分区被列出但其注册信息或assignment 不可读，**中止整个 ALTER**，部分回填会让该分区被新表级值路由，这正是本设计要防止的损坏。

回填的结果，即每个分区的 `PartitionRegistration` 和对应的 znode version，不会单独提交，而是和 Fluss 的表级更新合并在同一个 ZK 事务里，保证原子性。

#### B.3 Fluss 侧原子提交

回填计算完成后，所有分区 backfill 加上新 `TableRegistration`，含新桶数和 `bucketLayoutEpoch + 1`，在一个 ZooKeeper multi-op 事务中写入。

每个 `setData` 被读取时捕获的 znode version 做 CAS 保护，如果某个 znode 在读取后被其他操作修改，CAS 会失败，整个事务回滚。整个事务还被 coordinator-epoch znode 围栏，如果coordinator 已经被废黜，epoch 不匹配，事务也会失败。

后果：**没有分区能在没有自己的 `bucket.num.actual` 的情况下观察到新表级桶数。**回填和表级更新要么整体生效要么整体不生效，不存在"表级已变但旧分区还没有 `bucket.num.actual`"的中间态。

提交成功后，如果表启用了 auto-partitioning，`AutoPartitionManager` 的缓存 `TableInfo` 被刷新，使新建的自动分区从 ZK 现读新桶数并 stamp 到自己的注册信息里。

#### B.4 并发控制

一个 **striped 公平 `ReentrantReadWriteLock`** 数组保护 bucket 布局。固定 1024 个锁，`TablePath.hashCode() % 1024` 映射到固定 stripe。ALTER 取写锁；以下操作取读锁：`createPartition` 手动或动态建分区、`dropPartition` 手动删分区、`auto-partition` 预创建、`auto-partition` 历史分区创建、`auto-partition` 过期分区清理、以及历史分区 `enable/disable`时的创建或删除。

**为什么用 striped 而非 per-table map：** per-table `ConcurrentHashMap<TablePath, ReadWriteLock>`在 table drop 时需要 `remove` 避免锁对象泄漏。但 drop 后如果同名表重建，新锁和旧锁是不同实例，互斥可能丢失。Striped locks 没有 lifecycle 管理，同一个 `TablePath` 永远映射到同一个锁（通过 hashCode），即使 drop/recreate 后也如此，互斥不会丢失。1024 个 stripe 保持碰撞概率可忽略，内存只几 KB，一次性分配。

**公平模式**（`ReentrantReadWriteLock(true)`）防止 ALTER 在持续建分区流下饥饿。并发建分区之间仍然并行——读锁不互斥，只有写锁互斥。

建分区时在**读锁内从 ZooKeeper 现读**表级 bucket count（`metadataManager.getTableRegistration(tablePath).bucketCount`），而不是用可能过期的缓存 `TableInfo`，并将其 stamp 到新分区的注册信息里。这保证了 ALTER 正在执行时如果有建分区请求进来，它读到的是 ALTER 之前的值，因为ALTER 还没提交，这是自洽的。Auto-partition 创建同样在读锁内从 ZK 现读。

#### B.5 failover 与重试

**一致性保证：** ALTER 分为湖传播和 Fluss 侧 ZK 事务两个阶段，跨系统非原子。设计通过**幂等 + 有限重试 +最终一致**保证正确性。

- **湖传播失败：整个 ALTER 不起作用。** 湖传播在 Fluss ZK 事务之前执行，如果湖不可达或湖 catalog 异常，ALTER 直接中止，Fluss 侧 ZK 事务不执行。湖侧没改，Fluss 侧也没改，全部回滚。用户重跑同一 ALTER 即可。

- **湖传播成功但 Fluss ZK 失败：两侧不一致，报错提醒用户重跑。** 湖侧已改为 BUCKET = N，但 Fluss 侧未改，bucket.num = 旧值，epoch不变。此时**自动重试** ZK 事务，最多 3 次。超过 3 次后抛 `FlussRuntimeException`报错提醒用户。**用户手动重跑**同一 ALTER 时，重传湖，但湖传播幂等不产生副作用，重复设同一个 N 值。Fluss 侧 ZK重试提交。如果用户不重跑，虽然不影响正确性（tiering writer 用 Fluss 侧分区级桶数覆盖 Paimon BUCKET），但两侧状态不一致。

- tiering service 在读写时同时接触两侧，它读Fluss 元数据（`tableInfo.getNumBuckets()`）和 Paimon schema（`CoreOptions.BUCKET`），如果两者不一致，报警提醒用户重跑 ALTER，使两侧收敛。

**幂等性保证：** 三个部分各自幂等，因此同一条 ALTER 可以安全重跑，两侧最终收敛：
- **湖传播幂等：** 重复设置 Paimon `CoreOptions.BUCKET` 为同一个 N 值不产生副作用。
- **backfill 幂等：** `computePartitionBucketCountBackfill` 遍历分区时，已有 `bucketCount`的分区跳过。第一次没跑完的 backfill，重试时继续补上。
- **Fluss 侧 ZK 事务幂等：** 表级更新写绝对目标值 N（不是增量操作），CAS 保护。如果上次已提交，重跑时读到新状态，`bucketCount` 值不变（还是 N），CAS 成功提交。虽然 epoch 会多 +1，但 epoch 最重要的作用是"是否 ALTER 过"的标志，在这里多推一次不影响正确性。

**原子性保证：** 回填 + 表级更新在同一个 ZK multi-op 事务里提交，要么整体生效要么整体不生效。没有分区能观察到"表级已变但自己还没有 `bucket.num.actual`"的中间态。

**coordinator crash 恢复：**

| 场景                               | 恢复策略                                                                                                                                                                                                |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ZK 事务发送后、coordinator 收到响应前 crash | coordinator 重启后**从 ZK 读当前状态**判断是否已提交：读 `TableRegistration`，如果 `bucketCount` = 新值 N 且 `bucketLayoutEpoch` = 旧值+1，则已提交，ALTER 已完成；如果 `bucketCount` = 旧值且 `bucketLayoutEpoch` = 旧值，则未提交，ALTER 未生效，用户重跑。 |
| ZK 事务成功后 coordinator crash       | ZK 事务已提交（`bucketCount` = N，`epoch` = 旧值+1，所有分区 backfill 完成）。AutoPartitionManager 在下次事件处理时刷新。无需特殊处理。                                                                                                 |

### C. 读写路径的 bucket count 路由契约

#### C.1 为什么读写请求要带 bucket_count

客户端计算 `bucket_id` 用的是 `hash(key) % bucketCount`。如果客户端用了过期的 `bucketCount`，算出的 `bucket_id` 仍然落在 `[0, actualCount)` 之内，仍然是一个**合法的** bucket id，服务端会照常接受，记录却落进了错误的 bucket。这是一次**静默错路由**，既不可见，也无法在事后修复。

修复方法：把 `bucket_count` 变成请求的一部分。每个桶路由请求新增 `optional int32 bucket_count`proto 字段，声明客户端计算 `bucket_id` 时使用的值。服务端把它和该分区的实际桶数比对，不匹配就返回 `STALE_METADATA`，静默错路由由此变成显式失败。

#### C.2 写路径

| 组件                                 | 在写路径上做什么                                                                                                                      |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `WriterClient#doSend`              | 分配 bucket 前先用 `cluster.getBucketCount(tablePartition)` 取目标分区的桶数，取不到才回退到 `tableInfo.getNumBuckets()`，再用这个值初始化 `BucketAssigner` |
| `WriteBatch`                       | 创建时就把桶数存进自己的 `final int bucketCount` 字段，之后不再重读 `Cluster`                                                                      |
| `DynamicPartitionCreator`          | 同步等待，直到 partitionId 和 bucketCount 都在客户端集群元数据中可见才继续；上界为 `client.request-timeout`，默认 30s，轮询指数退避，100ms 起、倍增、上限 1s，每轮附带一次强制元数据刷新  |
| `ClientRpcMessageUtils`            | 构建 `PbProduceLogReqForBucket` 和 `PbPutKvReqForBucket` 时，从 batch 上取出锚定的桶数填入 `bucketCount` 字段                                   |
| `Sender#handleWriteBatchException` | 在可重试错误分支**之前**显式拦截 `Errors.STALE_METADATA`：失败该 batch 且不重新入队，标记该表的元数据失效，并失效该 bucket 对应的缓存 `BucketAssigner`                     |

**为什么 `WriteBatch` 要锚定桶数。** `Cluster` 是 volatile 不可变快照，刷新时整体替换。如果在算`bucket_id` 和填 `bucket_count` 之间分两次读 `Cluster`，攒批窗口就可能跨越一次 ALTER：`bucket_id` 按旧布局算出，`bucket_count` 填的却是新布局的值，请求声明和实际路由不再一致。锚定之后两者必然来自同一个快照。

**为什么动态建分区要同步等。** 以前是异步的，writer 发出 `createPartition` 就继续，bucket assignment 用表级桶数。ALTER 之后表级桶数不再代表新分区的桶数，必须等分区级桶数可见才能分配 bucket。针对同一新分区的并发 writer 共享一个 in-flight 创建，并都等待同一个元数据条件。

**为什么 STALE_METADATA 要显式拦截。** `StaleMetadataException` 继承 `InvalidMetadataException`与 `RetriableException`，通用机制本会重试它，而 batch 的 `bucket_id` 已经固定，重试只会再次落进错误的 bucket。失败 batch 并失效缓存的 `BucketAssigner`，才能让下次写从刷新后的 `Cluster`重新解析桶数。

#### C.3 读路径

| 组件                                                            | 在读路径上做什么                                                                                                                 |
|---------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `AbstractLookuper#resolvePartitionBucketCount`                | 按和写路径相同的方式解析目标分区的桶数：先查 `Cluster`，缺失时回退到表级值                                                                               |
| `LookupBatch`、`PrefixLookupBatch`                             | 构造时锚定自己的 `final int bucketCount`，请求构建函数不再接收 `Cluster` 参数                                                                 |
| `ClientRpcMessageUtils`                                       | batch 的桶数为正时，设置 `PbLookupReqForBucket` 和 `PbPrefixLookupReqForBucket` 的 `bucketCount`                                    |
| `TableScan`、`LimitBatchScanner`、`KvBatchScanner`、`LogFetcher` | 每次发请求时从 `Cluster` **现读**桶数，不做锚定                                                                                          |
| `ListOffsets`、`TableStats` 请求                                 | 同样从 `Cluster` 现读桶数，取到值才设置 proto 字段                                                                                       |
| `LookupSender#handleLookupError`                              | 在可重试错误分支**之前**显式拦截 `Errors.STALE_METADATA`：先按 `InvalidMetadataException` 分支失效该表或该分区的元数据，然后异常完成该 batch 的所有 lookup，不进入重试循环 |

lookup 同样攒批，锚定的理由和 `WriteBatch` 一致。Scanner 则不攒批，从取值到发出请求之间没有跨越 ALTER 的窗口，所以现读即可。

**历史分区 lookup 由服务端解析湖桶数。** 历史分区已经从 Fluss 删除，数据只留在湖里，按它被tier 时的桶数布局。ALTER 之后表级桶数和任何分区级 `bucket.num.actual` 都不再描述这个值，只有湖元数据还知道它，Paimon 把它暴露为 `DataSplit#totalBuckets`。因此这条路径的桶定位由服务端完成：服务端从原始分区的湖快照解析出该桶数，据此定位 key 所属的湖 bucket 再读取。客户端只提供key 和原始分区名，不参与计算 `bucket_id`，`bucket_count` 校验也不适用于历史分区 lookup请求。

#### C.4 bucket 枚举改为分区级

所有对分区表枚举 `[0, tableInfo.getNumBuckets())` 的地方，改为枚举`[0, partition.getBucketCount())`：

| 层     | 组件                                                      | 枚举场景                         |
|-------|---------------------------------------------------------|------------------------------|
| 客户端   | `TableScan`                                             | 批量扫描时枚举分区 bucket             |
| 客户端   | `LimitBatchScanner`、`KvBatchScanner`                    | limit 扫描、KV 快照扫描             |
| 客户端   | `LogFetcher`                                            | 枚举要 fetch 的 bucket           |
| Flink | `FlinkSourceEnumerator`                                 | 枚举分区 bucket 生成 split         |
| Flink | `FlussOnlyBatchSplitGenerator`                          | log split 生成                 |
| Flink | `LakeSplitGenerator`                                    | union-read split 生成          |
| Flink | `TieringSplitGenerator`                                 | tiering split 生成             |
| Flink | `TieringSplitReader`                                    | 快照分区桶数供 lake writer 使用       |
| Flink | `RecoveryOffsetManager`                                 | undo recovery 枚举 bucket 重建偏移 |
| Flink | `PushdownUtils`                                         | `count(*)` 下推枚举              |
| Flink | `OrphanCleanUtils`                                      | 孤儿数据清理枚举                     |
| Spark | `SplitPlanner`、`FlussMicroBatchStream`                  | 批和微批的 split 生成               |
| 服务端   | `RpcServiceBase`、`CoordinatorService#resolveNumBuckets` | 从分区 assignment 而非表级值解析分区桶数   |

#### C.5 客户端 Cluster 新增 bucket count map

`Cluster` 新增两个 bucket count map：`bucketCountByPartition` 按 `TablePartition(tableId,partitionId)` 键控，用于分区表；`bucketCountByTable` 按 `tableId` 键控，用于非分区表。

原来 `Cluster` 没有显式的 bucket count map，桶数隐含在 bucket locations 的 size 里，因为同一张表的所有分区桶数相同。ALTER 后不同分区有不同桶数，size 不再能推导出目标分区的桶数，所以需要显式存储分区级 bucket count。

选择 `TablePartition` 而不是路径作为 key，是因为 `partitionId` 是分区的稳定身份标识，全局唯一不可复用。分区删除后重建时路径不变而 `partitionId` 是新的，用路径键控无法区分新旧两套布局，用 `TablePartition` 键控则天然隔离。

### D. `bucketLayoutEpoch`：桶布局版本与路由校验

#### D.1 是什么

`bucketLayoutEpoch` 是 `TableRegistration` 上的一个 `long` 字段，表级单调递增计数器。新表默认为`0`，不含此字段的旧 JSON 也读为 `0`。它只在 `TableRegistration#withBucketCount` 中被推进，每次ALTER `bucket.num` 时 +1。新桶数和 epoch + 1 绑定在同一个原子操作里，没有代码路径能在不推进epoch 的情况下发布新 bucket 布局。

`bucketLayoutEpoch` 通过 `PbTableMetadata` 和 `GetTableInfoResponse` 在 proto 层传播。

#### D.2 为什么需要

核心问题：服务端收到一个桶路由请求时，怎么判断客户端用的 `bucket_count` 是否过期？

如果不能区分表从未被 ALTER 过和表已被 ALTER 过，就只有两种选择：
- 总是接受：ALTER 后旧分区被用新表级值路由，**静默错路由**
- 总是拒绝：表从未 ALTER 过时也拒绝旧客户端，**不向后兼容**

`bucketLayoutEpoch` 就是那个区分标志：
- `epoch == 0`：表从未被 ALTER，所有分区桶数都等于表级值，回退到表级是**可证明安全的**
- `epoch > 0`：表已被 ALTER，分区桶数不再**保证**等于表级值，回退不可证明安全，所以不回退

这里的关键是不保证，而不是一定不等。表级桶数存在 ABA 形态：从 4 改到 8、再改回 4 之后，4 那一代分区的桶数与当前表级值恰好相等，8 那一代不相等，而两代分区在同一张表里共存。相等只是巧合，从表级值无法反推某个分区属于哪一代，回退到表级值对一部分分区碰对、对其余分区静默错路由。因此 `epoch > 0` 之后一律不做回退，只拿分区自己的 `bucket.num.actual` 路由。

#### D.3 怎么起作用

`bucketLayoutEpoch` 在三个环节起作用：

**环节 1：服务端校验路由请求。** `TabletService` 在处理每个桶路由请求前，调用`TabletServerMetadataCache#validateBucketCount` 校验请求中的 `bucket_count`。桶路由请求包括ProduceLog、PutKv、FetchLog、Lookup、PrefixLookup、Scan、LimitScan、ListOffsets、TableStats，校验逻辑如下：

```
请求未带 bucket_count，即 optional 字段未设置、读为 0：
  epoch == 0 → NONE，允许省略，回退表级安全
  epoch > 0  → STALE_METADATA，表已被 ALTER，不允许省略

请求带了 bucket_count：
  分区请求：快照里有该分区的桶数且不相等 → STALE_METADATA
  非分区表请求：快照里该表的 bucket metadata 非空且 size 不相等 → STALE_METADATA
  其余情况 → NONE，包括相等，以及快照里没有该分区或该表的桶数
```

服务端只在自己确实知道目标桶数时才判定不匹配。快照里缺该分区或该表的桶数时放行，避免元数据传播过程中的不完整快照造成误拒；客户端能填出分区级桶数，说明这个值本身就是从服务端元数据拿到的，此时放行不会引入错路由。

服务端**不做客户端版本推断**，只看请求内容和 epoch。一个旧客户端永远不带 `bucket_count`，在 epoch=0 时被允许、epoch>0 时被拒绝。一个新客户端在 epoch=0 时带的可能也是表级值，因为服务端没返回分区级桶数，客户端从 `Cluster` 回退到了表级；而表级值在 epoch=0 时等于实际值，校验通过。

TabletServer 在应用第一次 `UpdateMetadata` 之前不参与校验：此时快照是空的，根本无法判断桶数，`validateBucketCount` 抛可重试的 `LeaderNotAvailableException`，客户端刷新元数据后重试，而不会拿空快照做出匹配或不匹配的判定。

**环节 2：服务端返回分区元数据。** `RpcServiceBase#listPartitionInfos` 先读表注册信息拿到`bucketLayoutEpoch`，再读分区注册信息，对每个分区调用`PartitionRegistration#getBucketCountOrDefault` 填充 `bucket_count`：

```
分区有持久化 bucketCount → 用它
分区没有 bucketCount 且 epoch == 0 → 回退到表级值，此时表级值就是实际值
分区没有 bucketCount 且 epoch > 0 → 抛 StaleMetadataException，拒绝回退表级值
```

这意味着：epoch>0 时，服务端**必须返回分区实际桶数**，不可回退表级值。ALTER 的 backfill流程保证了 epoch>0 时所有分区都有持久化的 `bucketCount`，所以这个分支正常情况下不会触发，它是一个安全网，防止元数据不一致时静默返回错误值。

**环节 3：TabletServer 不接受更低的 epoch。** `TabletServerMetadataCache` 在收到 `UpdateMetadata` 消息时，比较消息中的 `bucketLayoutEpoch` 和自己已持有的值。如果消息的 epoch 低于已有值，**丢弃该表的这次更新**，同一条消息里其他表的更新照常生效。丢弃更低的 epoch，是为了不让较旧的桶布局替换较晚一次 ALTER `bucket.num` 提交的布局。epoch 只对表级桶布局的变更排序，它不是通用的元数据版本：同一个 epoch 下的两条更新可能携带不同的 schema、分区或副本元数据。


## Migration Plan and Compatibility

### 升级契约

**1. 禁止滚动升级期间 ALTER。** 滚动升级期间集群中新旧版本 TabletServer 混合，旧版本TabletServer 不理解 `bucket_layout_epoch` 和 `bucket_count`。如果此时执行 ALTER，旧 TabletServer会收到带新字段的 `UpdateMetadata` 但无法正确处理，导致静默错路由。因此 ALTER 必须在所有TabletServer 都升级完成后才能执行，并由服务端强制：coordinator 在执行 ALTER 前校验所有活跃TabletServer 都上报了支持分区级 bucket count 的最低版本，任一节点不满足就拒绝这次 ALTER。版本上报与校验沿用 KIP-584 的 feature version 协商思路。

**2. 推荐先升级客户端，再升级集群。** 如果先升级集群（服务端）再升级客户端：
- 老客户端在表未被 ALTER（epoch=0）之前可以继续用——服务端接受不带 `bucket_count` 的请求
- 但表被 ALTER（epoch>0）之后，老客户端的请求被服务端以 `STALE_METADATA` 显式拒绝——老客户端无法读写该表，直到升级

如果先升级客户端：
- 新客户端完全兼容老集群（新 proto 字段是 optional，老服务端忽略）
- 集群升级后无缝衔接，无需额外操作

**3. 新客户端无缝兼容纯老集群和纯新集群。**
- 新客户端 → 老集群：新 proto 字段被老服务端忽略；响应不含 `bucket_layout_epoch` / `bucket_count`，客户端读到 epoch=0，回退到表级桶数。等同于老客户端的行为。
- 新客户端 → 新集群：完全兼容，所有功能可用。

### 兼容性矩阵

按服务端版本、客户端版本、表 epoch 三个维度划分：

| 服务端 | 客户端 | epoch | 行为                                                                               |
|-----|-----|-------|----------------------------------------------------------------------------------|
| 新   | 新   | 0     | 完全兼容。新客户端可能不带 `bucket_count`（服务端未返回分区级时）；epoch=0 允许省略，回退表级安全。                    |
| 新   | 新   | `>0`  | 完全兼容。新客户端带 `bucket_count`，服务端校验通过。如果客户端元数据过期，返回 `STALE_METADATA`，客户端刷新后自动恢复。     |
| 新   | 旧   | 0     | 兼容。旧客户端不带 `bucket_count`；epoch=0 允许省略。表级桶数对所有分区都正确。                              |
| 新   | 旧   | `>0`  | **不兼容。** 旧客户端不带 `bucket_count`；epoch`>0` 拒绝，返回 `STALE_METADATA`。旧客户端无法读写该表，直到升级。 |
| 旧   | 新   | N/A   | 兼容。新 proto 字段被老服务端忽略；客户端读到 epoch=0，回退表级。等同于今天的行为。                                |
| 旧   | 旧   | N/A   | 现状，不变。                                                                           |

核心规则：**`bucket_count` 是否可以省略，由 epoch 决定**——epoch=0 时省略是安全的（表级桶数对所有分区都正确），epoch>0 时省略被拒绝（表级桶数不再等于旧分区桶数）。客户端新旧只间接影响“是否填 bucket_count”，服务端不做客户端版本推断，只看请求内容和 epoch。

### 升级流程

1. **升级所有客户端 / connector**（Flink、Spark、Kafka-compatible、Rust，以及任何嵌入使用`fluss-client` 的）。新客户端兼容老集群，升级后立即可用。
2. **升级服务端**（CoordinatorServer + TabletServer）。滚动升级期间禁止执行`ALTER TABLE ... SET ('bucket.num' = N)`。
3. **所有服务端升级完成后**，可以开始使用 `ALTER TABLE ... SET ('bucket.num' = N)`。

### 已知限制

- **滚动升级窗口内 `bucket.num` 不可变更。** 只要有活跃 TabletServer 没有上报支持分区级bucket count 的最低版本，coordinator 就拒绝 ALTER，用户需要等所有服务端升级完成。
- **ALTER 后降级不安全。** `TableRegistration` 和 `PartitionRegistration` 的 serde version 现在都是 v2，而两者的 deserializer 都不校验 version、也忽略未知字段，所以旧服务端读 v2 znode 时会丢掉 `bucket_layout_epoch` 和 `bucket_count`，然后按表级桶数路由所有分区。因此降级一个已ALTER 过表的集群是不支持的。
- 已有分区的 rescale 暂不支持（后续 FIP 支持）


## Rejected Alternatives

**1. 保持 bucket count 表级并通过重写所有数据来 rescale 。**

方法：保持 bucket 数为表级固定属性，按新 bucket 数新建表并重写所有数据。

拒绝理由：
- 重写了没人想改的历史分区，包括已经 tiered 到湖里、本不该被重写的数据。
- 破坏已提交数据的湖侧 bucket 布局。
- 对大表来说是长时间、高风险的迁移。
- 本 FIP 的核心观察是，一个分区的 bucket 布局只需要内部自洽，一个分区表的不同分区可以使用不同的 bucket 数，重写无法只针对新分区，旧分区被迫一起重写，而它们本不需要变。

**2. 保持客户端 `Cluster` 的 bucket count 按 `PhysicalTablePath` 键控。**

方法：客户端 `Cluster` 的分区级 bucket count map 按 `PhysicalTablePath`，即 `db.table$partition`字符串，做 key。

拒绝理由：
- bucket count 属于具体的 tableId 或 partitionId，不属于表名或分区名。对于分区表，`TablePartition(tableId, partitionId)` 的 bucket count 永远不变；对于非分区表，tableId 的bucket count 永远不变。ALTER bucket.num 只改变新创建分区使用的默认值，已有的 TablePartition保留自己的 `bucket.num.actual`。
- 表名或分区名不提供这个保证。未来的 INSERT OVERWRITE 可能用一个新 tableId 或 partitionId替换同名对象，并在相同名称下使用不同的 bucket count。路径不变而 `TablePartition` 不同，用路径做 key 会把新旧两套布局混在一起。

**3. 用 coordinatorEpoch 替代 bucketLayoutEpoch。**

方法：用协调器的 leader 任期 epoch 作为表级版本。

拒绝理由：
- coordinatorEpoch 代表协调器 leader 切换，不是单表的 bucket.num 变更。
- 一次 coordinator 切换可能不涉及任何 bucket.num 变更，一次 bucket.num 变更也不一定触发coordinator 切换，两者没有对应关系。

**4. 用客户端版本或布尔标志替代 bucketLayoutEpoch。**

方法：按客户端协议版本拒绝旧客户端的请求；或持久化一个布尔标志记录是否发生过 ALTER。

拒绝理由：
- 按客户端版本拒绝会在服务端升级后立即拒绝所有旧客户端，即使表从未执行过 ALTER。
- 布尔标志能记录是否 ALTER 过，但两次 ALTER 产生的 UpdateMetadata 都携带 true，TabletServer 无法判断哪条更旧。如果携带旧表级值的消息晚到，布尔值无法让它被忽略。单调递增的 epoch 通过数值比较解决这个问题，更符合当前实现。


