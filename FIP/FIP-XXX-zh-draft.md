# FIP-51: Support Per-Partition Bucket Count

| **当前状态** | 讨论中（Under Discussion） |
| --- | --- |
| **Discussion thread** | here (<- link to [https://lists.apache.org/list.html?dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org)) |
| **Vote thread** | here (<- link to [https://lists.apache.org/list.html?dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org)) |
| **Issue** | [apache/fluss#3907](https://github.com/apache/fluss/issues/3907) |
| **Release** |  |

## Motivation

目前，Fluss 表及其所有分区共用一个在建表时确定的 bucket 数。`bucket.num` 是表的结构化属性，分区本身不持有任何独立的 bucket 数属性：分区的 bucket 数取自创建分区时的表级 `bucket.num`，但分区元数据并不持久化这个值。

这导致两个问题。

**1. 没有分区级灵活性。** 在典型的时间分区表里，每个分区的数据量随时间增长。之前的分区可能 4 个 bucket就够了，现在可能需要 32 个。由于 `bucket.num`是表级的，用户必须从建表第一天就按未来峰值来设定，导致每个历史分区都过度配置；或者接受近期分区 bucket 不足、成为读写瓶颈。

**2. 改变 bucket 数代价高昂。** 在本 FIP 之前，Fluss 不支持改变 bucket 数，唯一办法是按新 bucket 数新建一张表，然后把数据迁移到新表，这是最原始、效率最低的方法：要重分布所有数据，包括已经 tiered 到湖里、本不该被重写的历史分区。对大表来说这是一次长时间、高风险的迁移，还会破坏已提交数据的湖侧 bucket 布局。

在一个真实 Fluss 场景，我们需要解决的问题：

> **应用场景。** Fluss 表在运行过程中需要调整 bucket 数量以适配数据量变化，但历史分区数据已按原 bucket 数写入，不宜变更。

> **需求描述。** 作为 Fluss 用户，希望能动态修改表的 bucket 配置，使新分区按新配置写入而历史分区保持不变，从而实现灵活的存储与性能调优，无需重写旧数据。

本 FIP 直接回应了这一需求：rescale 现在可以只作用于变更后新建的分区，已有分区原样保留。这在逻辑上是成立的，因为一个分区的 bucket 布局只需要自身自洽——没有任何东西要求同一张表的两个分区必须采用相同的 bucket 布局，只要每个读写路径都按分区自己的 bucket 数来路由即可。

本 FIP 将 bucket 数变为分区级属性：

`ALTER TABLE ... SET ('bucket.num' = N)` 后，只影响之后创建的分区；已有分区保持其 bucket 数不变、旧数据不被触碰。这条规则本质上是分区维度的，非分区表没有分区维度可供它作用。

### Goals

*   分区表可以拥有不同 bucket 数的分区。

*   `ALTER TABLE ... SET ('bucket.num' = N)` 是一个在线的、无数据移动的元数据操作，双向都支持：扩容以将负载分散到更多 bucket，缩容以释放多余 bucket 占用的副本和 RocksDB 实例。

*   用户操作面只有表级：`ALTER TABLE ... SET ('bucket.num' = N)`。

*   每条读、写、lookup、tiering 和 union-read 路径都按目标分区的实际 bucket 数路由。

*   持有过期元数据的客户端绝不会静默地将记录写入错误的 bucket；它必须显式失败。


### Non-Goals

*   本 FIP 不考虑旧数据重刷。

*   非 Paimon 的湖格式。本 FIP 只考虑 Paimon；其他格式后续考虑。


## New or Changed Public Interfaces

### RPC / Proto 变更

所有新字段都是 `optional`，新旧 peer 保持 wire 兼容。

表级 `bucket_layout_epoch`：

| 消息 | 字段 |
| --- | --- |
| `PbTableMetadata` | `optional int64 bucket_layout_epoch` |
| `GetTableInfoResponse` | `optional int64 bucket_layout_epoch` |

分区级 `bucket_count_actual`：

| 消息 | 字段 |
| --- | --- |
| `PbPartitionMetadata` | `optional int32 bucket_count_actual` |
| `PbPartitionInfo` | `optional int32 bucket_count_actual` |

桶路由请求（客户端计算 `bucket_id` 时使用的值）：

| 消息 | 字段 |
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

### 新增错误码

| 名称 | 异常 | 含义 |
| --- | --- | --- |
| `STALE_METADATA` | `StaleMetadataException` | 请求中的 `bucket_count_actual` 和服务端的实际 `bucket_count_actual` 不匹配。客户端必须刷新元数据后重试。 |
| `TABLET_METADATA_NOT_READY` | `TabletMetadataNotReadyException` | TabletServer 尚未收到该表或分区的元数据。客户端应重试同一请求，不需要刷新元数据或重建 bucket assigner，因为服务端只是还没追上，客户端的 `bucket_count_actual`可能是正确的。 |

`STALE_METADATA` 和 `TABLET_METADATA_NOT_READY` 语义不同：前者表示服务端已确认客户端的`bucket_count_actual` 是错的，客户端必须刷新元数据；后者表示服务端尚未收到元数据，无法判断，客户端应直接重试。

### ZooKeeper 元数据格式

| Znode | 变更 |
| --- | --- |
| `PartitionRegistration`（`/.../partitions/<name>`） | 新增 optional `bucket_count_actual`；serde `VERSION` 从 **1 → 2**。v1 数据中无此字段 → 读为 `null`。 |
| `TableRegistration`（`/.../tables/<table>`） | 新增 `bucket_layout_epoch`；serde `VERSION` 从 **1 → 2**。v1 数据中无此字段 → 读为 `0`，即该表从未被 ALTER 过。 |

### 配置和 DDL

不引入新的配置选项。两个已有面变化含义：

*   `bucket.num` 语义。 之前是"Fluss 表使用的 bucket 数"。现在是"Fluss表终态目标 bucket 数；对分区表，它适用于新建的分区，按新`bucket.num`建立分区；已有分区保留原来的 bucket 数。"

*   `bucket.num` 变为可 alter 。从 Flink connector 的 `ALTER_DISALLOW_OPTIONS` 中移除，启用：


```sql
ALTER TABLE my_partitioned_table SET ('bucket.num' = '8');
```

## Proposed Change

### A. 引入分区级别的 `bucket.num.actual` 的语义

本 FIP 将表级别和分区级别的 bucket 数拆为两个属性，两者结构上对称、仅可写性和查询方式不同：

|  | `bucket.num` | `bucket.num.actual` |
| --- | --- | --- |
| 作用域 | 表 | 分区 |
| 含义 | **目标值**：预期的终态 bucket 数，作为之后新建分区的模板 | **实际值**：某个分区当前真正使用的 bucket 数 |
| 用户可写 | 是，通过 DDL（`CREATE TABLE`、`ALTER TABLE ... SET`） | 否，服务端在分区创建时派生 |
| 持久化为 | 表 znode 的 `bucket_count` | 分区 znode 的 `bucket_count_actual` |
| 用户可读 | 是（`DESCRIBE`、connector options） | 通过存储过程查询分区的 `bucket.num.actual` |

两者都是 `int` 结构化字段，表级序列化为 znode JSON 的 `bucket_count`，分区级序列化为 `bucket_count_actual`，都不在 property map 里。`bucket.num` 额外作为 Flink connector option key `bucket.num` 暴露给用户通过 `WITH ('bucket.num' = N)` 设置；`bucket.num.actual` 没有 connector 侧的 key，用户不能直接设置。两者的区别仅在于可写性和查询方式。

#### A.1 一致性规则

`ALTER` 后，对于新建的分区，其`bucket.num.actual == bucket.num`，因为新建分区在创建时从表级`bucket.num` 拷贝，而此时表级值已被 ALTER 更新为目标值 N。

对于已有分区，`bucket.num.actual != bucket.num`，因为这些分区在 ALTER 之前就已经创建，它们的 `bucket.num.actual` 拷贝的是 ALTER 之前的表级值，而表级值现在已经变了。这种分叉状态持续到该分区的数据被重刷（由后续 FIP 完成）后才重新一致。

#### A.2 继承 / 快照语义

分区在创建瞬间从表级 `bucket.num` 拷贝一次并持久化为自己的 `bucket.num.actual`。此后该分区的 `bucket.num.actual` 不可变，直到其数据被重写。

这意味着：表级 `bucket.num` 的后续变更不会影响已存在分区的 `bucket.num.actual`，每个分区"冻结"了它创建那一刻的表级值。这正是"新分区用新桶数、旧分区保持旧桶数"得以实现的核心机制：不需要为旧分区做任何迁移，它们自然保持不变。

典型时间线：

1.  以 `bucket.num=4` 建表 → 该期间创建的每个分区都带 `bucket.num.actual=4`

2.  用户执行 `ALTER TABLE ... SET ('bucket.num' = '8')` → 表级 `bucket.num` 变为 8

3.  之后创建的每个分区都带 `bucket.num.actual=8`，从新表级值拷贝

4.  4 桶和 8 桶两代分区在同一张表里长期共存，读写路径按各自分区的 `bucket.num.actual` 路由


### B. ALTER 操作：修改表级 bucket num 属性

整个 ALTER 分为两个阶段：湖优先传播和 Fluss 侧 ZK 事务。两者跨系统非原子，但设计保证最终一致：湖传播幂等可安全重跑，Fluss 侧旧分区回填 + 表级更新在同一个 ZK事务里原子提交。

#### B.1 湖优先传播

如果表是开湖的（`table.datalake.enabled = true`）且 bucket key 非空，coordinator 先向湖侧传播新 bucket 数，再修改 Fluss 侧。这沿用了已有的湖优先 ALTER 流程：湖失败会中止 ALTER 且 Fluss 侧不变，而因为传播是幂等的，Fluss 侧失败后重跑同一 ALTER能让两侧收敛。

以 Paimon 为例，coordinator 调用湖 catalog 的 alterTable 接口，将 Paimon 表 schema 的`CoreOptions.BUCKET` 改为目标值 N。这使 Paimon 之后新建分区时采用新桶数。

传播是幂等的：将同一个 N 值重复传播不产生副作用，Paimon 的 `BUCKET` 选项被设为同一个值，重复设置不会改变已有分区的布局。

传播失败（湖不可达、湖 catalog 异常、表不存在）时，ALTER 中止，Fluss 侧不变。用户重跑同一 ALTER，湖传播从头开始，因为幂等，不会产生脏状态。

Iceberg / Hudi / Lance 暂不支持此传播，`alterTable` 直接抛 `UnsupportedOperationException`，ALTER 被拒绝。

非开湖表或 bucket key 为空的表跳过此阶段，不需要湖传播。

#### B.2 旧分区回填

ALTER 之前已经存在的分区，即"旧分区"，可能没有持久化自己的 `bucket.num.actual`，特别是那些由本 FIP 之前的版本创建的分区，它们的注册信息里没有持久化桶数。回填的目的是为这些分区补上 `bucket.num.actual`，取值为它们当前实际的 bucket 数，从该分区现有的 bucket assignment 推断，而不是新表级值 N。

这保证了旧分区在 ALTER 后仍然按其原有布局运行，它们的 `bucket.num.actual` 被"冻结"在ALTER 之前的值，而不是跟着表级值一起变。

回填是幂等的：已有 `bucket.num.actual` 的分区跳过。因此如果 ALTER 被重试，已经回填过的分区不会被重复处理。

回填的计算流程：对每个没有持久化 `bucket.num.actual` 的已有分区，连同其 znode version 一起读取注册信息，从该分区现有的 bucket assignment 推断桶数。如果某个分区被列出但其注册信息或 assignment 不可读，中止整个 ALTER，部分回填会让该分区被新表级值路由，这正是本设计要防止的损坏。

回填的结果，即每个分区的注册信息和对应的 znode version，不会单独提交，而是和 Fluss 的表级更新合并在同一个 ZK 事务里，保证原子性。

#### B.3 Fluss 侧原子提交

回填计算完成后，所有分区 backfill 加上新表注册信息，含新桶数和 `bucketLayoutEpoch + 1`，在一个 ZooKeeper multi-op 事务中写入。

每个 znode 的写入被读取时捕获的 znode version 做 CAS 保护，如果某个 znode 在读取后被其他操作修改，CAS 会失败，整个事务回滚。整个事务还被 coordinator-epoch znode 围栏，如果coordinator 已经被废黜，epoch 不匹配，事务也会失败。

后果：没有分区能在没有自己的 `bucket.num.actual` 的情况下观察到新表级桶数。回填和表级更新要么整体生效要么整体不生效，不存在"表级已变但旧分区还没有 `bucket.num.actual`"的中间态。

提交成功后，如果表启用了 auto-partitioning，`AutoPartitionManager` 的缓存 `TableInfo` 被刷新，使新建的自动分区从 ZK 现读新桶数并 stamp 到自己的注册信息里。

#### B.4 并发控制

ALTER `bucket.num` 在读取分区列表、计算回填、提交 ZK 事务的整个过程中，与分区创建和分区删除存在竞争。这两类操作修改同一批 znode，如果不加协调，会产生不一致。

考虑以下场景：ALTER 读取分区列表 `[P1, P2, P3]` 并开始计算回填；此时一个新的分区 `P4` 被创建并写入 ZK；ALTER 的事务提交时，`P4` 不在回填列表里，因此没有持久化的 `bucket.num.actual`。ALTER 提交后 `bucketLayoutEpoch` 变为大于 0，`P4` 走到「`bucketLayoutEpoch` > 0 且无 `bucket.num.actual`」的分支，抛 `StaleMetadataException`，该分区变得不可读写，直到被手动修复。反过来，如果 ALTER 提交后表级桶数已变为 N，而一个正在进行的建分区请求读到的还是旧值 4，新分区会被 stamp 成 4——这本身是自洽的，它是一个 pre-ALTER 分区，但是不允许这样的不可预测的行为发生。

因此 ALTER 与分区创建、分区删除互斥：ALTER 提交期间不允许建分区或删分区，反之亦然。不存在 ALTER 执行到一半时建分区读到一个中间态值的场景。分区创建之间仍然可以并发，因为它们互不影响彼此的 znode。建分区时从 ZK 现读表级 `bucket.num` 并 stamp 到新分区注册信息里，而不是用可能过期的缓存，保证读到的始终是当前已提交的值。

#### B.5 failover 与重试

**一致性保证：** ALTER 分为湖传播和 Fluss 侧 ZK 事务两个阶段，跨系统非原子。设计通过幂等 + 有限重试 +最终一致保证正确性。

*   湖传播失败：整个 ALTER 不起作用。 湖传播在 Fluss ZK 事务之前执行，如果湖不可达或湖 catalog 异常，ALTER 直接中止，Fluss 侧 ZK 事务不执行。湖侧没改，Fluss 侧也没改，全部回滚。用户重跑同一 ALTER 即可。

*   湖传播成功但 Fluss ZK 失败：两侧不一致，报错提醒用户重跑。 湖侧已改为 `bucket.num` = N，但 Fluss 侧未改，`bucket.num` = 旧值，`bucketLayoutEpoch`不变。此时自动重试 ZK 事务，最多 3 次。超过 3 次后抛 `FlussRuntimeException`报错提醒用户。用户手动重跑同一 ALTER 时，重传湖，但湖传播幂等不产生副作用，重复设同一个 N 值。Fluss 侧 ZK重试提交。如果用户不重跑，虽然不影响正确性（tiering writer 用 Fluss 侧分区级桶数覆盖 Paimon BUCKET），但两侧状态不一致。

*   tiering service 在读写时同时接触两侧，它读Fluss 元数据的表级桶数和 Paimon schema，如果两者不一致，报警提醒用户重跑 ALTER，使两侧收敛。


**幂等性保证：** 三个部分各自幂等，因此同一条 ALTER 可以安全重跑，两侧最终收敛：

*   湖传播幂等： 重复设置 Paimon `CoreOptions.BUCKET` 为同一个 N 值不产生副作用。

*   backfill 幂等： 回填遍历分区时，已有 `bucket.num.actual`的分区跳过。第一次没跑完的 backfill，重试时继续补上。

*   Fluss 侧 ZK 事务幂等： 表级更新写绝对目标值 N（不是增量操作），CAS 保护。如果上次已提交，重跑时读到新状态，`bucket.num` 值不变（还是 N），CAS 成功提交。虽然 `bucketLayoutEpoch` 会多 +1，但 `bucketLayoutEpoch` 最重要的作用是"是否 ALTER 过"的标志，在这里多推一次不影响正确性。


**原子性保证：** 回填 + 表级更新在同一个 ZK multi-op 事务里提交，要么整体生效要么整体不生效。没有分区能观察到"表级已变但自己还没有 `bucket.num.actual`"的中间态。

**coordinator crash 恢复：**

| 场景 | 恢复策略 |
| --- | --- |
| ZK 事务发送后、coordinator 收到响应前 crash | coordinator 重启后从 ZK 读当前状态判断是否已提交：读表注册信息，如果 `bucket.num` = 新值 N 且 `bucketLayoutEpoch` = 旧值+1，则已提交，ALTER 已完成；如果 `bucket.num` = 旧值且 `bucketLayoutEpoch` = 旧值，则未提交，ALTER 未生效，用户重跑。 |
| ZK 事务成功后 coordinator crash | ZK 事务已提交（`bucket.num` = N ，`bucketLayoutEpoch` = 旧值+1，所有分区 backfill 完成）。AutoPartitionManager 在下次事件处理时刷新。无需特殊处理。 |

### C. 读写路径的路由契约

#### C.1 为什么读写请求要带 bucket\_count\_actual

客户端计算 `bucket_id` 用的是 `hash(key) % bucket.num.actual`。如果客户端用了过期的 `bucket.num.actual`，算出的 `bucket_id` 仍然落在 `[0, 实际 bucket 数)` 之内，仍然是一个合法的 bucket\_id，服务端会照常接受，记录却落进了错误的 bucket。这是一次静默错路由，既不可见，也无法在事后修复。

修复方法：把 `bucket_count_actual` 变成请求的一部分。每个桶路由请求新增 `optional int32 bucket_count_actual`proto 字段，声明客户端计算 `bucket_id` 时使用的值。服务端把它和该分区的实际桶数比对，不匹配就返回 `STALE_METADATA`，静默错路由由此变成显式失败。

#### C.2 写路径

| 组件 | 在写路径上的角色 |
| --- | --- |
| `WriterClient` | 从集群元数据取目标分区的桶数来分配 bucket |
| `WriteBatch` | 创建时锚定桶数，整个攒批生命周期内使用同一个值 |
| `DynamicPartitionCreator` | 动态建分区时同步等待分区级桶数可见后再继续 |
| 请求构建 | 从 batch 上取出锚定的桶数填入 `bucket_count_actual` proto 字段 |
| `Sender` | 收到 `STALE_METADATA` 时失败该 batch 并失效元数据，不重试 |

**为什么** `WriteBatch` **要锚定桶数。** 集群元数据是 volatile 不可变快照，刷新时整体替换。如果在算 `bucket_id` 和填 `bucket_count_actual` 之间元数据发生刷新，攒批窗口就可能跨越一次 ALTER：`bucket_id` 按旧布局算出，`bucket_count_actual` 填的却是新布局的值，请求声明和实际路由不再一致。锡定之后两者必然来自同一个快照。

**为什么动态建分区要同步等。** 在本 FIP 之前，所有分区的桶数都等于表级值，动态建分区可以异步：writer 发出 `createPartition` 后直接用表级桶数分配 bucket，不等分区元数据返回。ALTER 之后，新建分区的桶数可能不同于表级值，writer 不能再用表级桶数，必须等到分区级桶数在元数据中可见后才能正确分配 bucket。多个 writer 同时写同一个新分区时，共享同一次创建请求，都等待同一轮元数据刷新。

#### C.3 读路径

| 组件 | 在读路径上的角色 |
| --- | --- |
| `Lookup` 解析 | 从集群元数据取目标分区的桶数 |
| `LookupBatch`、`PrefixLookupBatch` | 构造时锚定桶数，请求构建不再重读元数据 |
| 请求构建 | 桶数为正时填入 `bucket_count_actual` proto 字段 |
| `Scanner` | 每次发请求时现读桶数，不锚定 |
| `ListOffsets`、`TableStats` | 同样现读桶数 |
| `LookupSender` | 收到 `STALE_METADATA` 时失败该 batch 并失效元数据，不重试 |

lookup 同样攒批，锚定的理由和 `WriteBatch` 一致。Scanner 不攒批，从取值到发出请求之间没有跨越 ALTER 的窗口，现读即可。

**历史分区 lookup 由服务端解析湖桶数。** 历史分区已经从 Fluss 删除，数据只留在湖里，按它被 tier 时的桶数布局。ALTER 之后表级桶数和任何分区级 `bucket.num.actual` 都不再描述这个值，只有湖元数据还知道它。因此这条路径的桶定位由服务端完成：服务端从原始分区的湖快照解析出该桶数，据此定位 key 所属的湖 bucket 再读取。客户端只提供key 和原始分区名，不参与计算 `bucket_id`，`bucket.num.actual` 校验也不适用于历史分区 `lookup`请求。

#### C.4 bucket 枚举改为分区级

所有对分区表枚举 `[0, 表级的 bucket.num)` 的地方，改为枚举`[0, 分区级的 bucket.num.actual)`：

| 层 | 组件 | 枚举场景 |
| --- | --- | --- |
| 客户端 | `TableScan` | 批量扫描时枚举分区 bucket |
| 客户端 | `LimitBatchScanner`、`KvBatchScanner` | limit 扫描、KV 快照扫描 |
| 客户端 | `LogFetcher` | 枚举要 fetch 的 bucket |
| Flink | `FlinkSourceEnumerator` | 枚举分区 bucket 生成 split |
| Flink | `FlussOnlyBatchSplitGenerator` | log split 生成 |
| Flink | `LakeSplitGenerator` | union-read split 生成 |
| Flink | `TieringSplitGenerator` | tiering split 生成 |
| Flink | `TieringSplitReader` | 快照分区桶数供 lake writer 使用 |
| Flink | `RecoveryOffsetManager` | undo recovery 枚举 bucket 重建偏移 |
| Flink | `PushdownUtils` | `count(*)` 下推枚举 |
| Flink | `OrphanCleanUtils` | 孤儿数据清理枚举 |
| Spark | `SplitPlanner`、`FlussMicroBatchStream` | 批和微批的 split 生成 |
| 服务端 | `RpcServiceBase`、`CoordinatorService#resolveNumBuckets` | 从分区 assignment 而非表级值解析分区桶数 |

### D. 路由校验

#### D.1 校验思路

服务端收到桶路由请求时，需要判断客户端用的 `bucket_count_actual` 是否过期。校验分两个层面：

第一，请求带了 `bucket_count_actual` 的情况：服务端把它和自己掌握的该分区实际桶数比对，不匹配就返回 `STALE_METADATA`。这直接防止了客户端用过期桶数算出的 `bucket_id` 被静默接受。

第二，请求没带 `bucket_count_actual` 的情况（旧客户端）：服务端需要区分这张表是否被 ALTER 过。如果从未被 ALTER 过，所有分区的桶数都等于表级值，不带 `bucket_count_actual` 是安全的，可以接受。如果被 ALTER 过，表级值不再代表所有分区的实际桶数，不带 `bucket_count_actual` 的请求必须被拒绝，返回 `STALE_METADATA`。

`bucketLayoutEpoch` 就是这个区分标志：为 0 表示从未 ALTER，大于 0 表示已被 ALTER。它只在桶数变更时推进，只对桶布局变更排序，不是通用的元数据版本：同一个 `bucketLayoutEpoch` 下的两条元数据更新可能携带不同的 schema、分区或副本元数据。TabletServer 收到更低的 `bucketLayoutEpoch` 时丢弃该表的更新，不让较旧的桶布局替换较晚一次 ALTER 提交的布局。

服务端只在自己确实知道目标桶数时才判定不匹配。快照里缺该分区或该表的桶数时放行，避免元数据传播过程中的不完整快照造成误拒；客户端能填出分区级桶数，说明这个值本身就是从服务端元数据拿到的，此时放行不会引入错路由。TabletServer 刚启动、尚未收到元数据时同理：快照为空，不做不匹配判定，请求照常通过。

#### D.2 为什么需要校验

如果不能区分表从未被 ALTER 过和表已被 ALTER 过，就只有两种选择：

*   总是接受不带 `bucket_count_actual` 的请求：ALTER 后旧客户端用表级值路由旧分区，静默错路由

*   总是拒绝不带 `bucket_count_actual` 的请求：表从未 ALTER 过时也拒绝旧客户端，不向后兼容


两者都不可接受。`bucketLayoutEpoch` 为 0 时允许省略 `bucket_count_actual`（表级值对所有分区都正确），大于 0 时必须拒绝（表级值不再保证等于旧分区桶数）。

这里的关键是「不保证」而不是「一定不等」。表级桶数存在 ABA 形态：从 4 改到 8、再改回 4 之后，4 那一代分区的桶数与当前表级值恰好相等，8 那一代不相等，两代分区在同一张表里共存。相等只是巧后，从表级值无法反推某个分区属于哪一代。因此 `bucketLayoutEpoch > 0` 之后一律不回退到表级值，只拿分区自己的 `bucket.num.actual` 路由。

#### D.3 举例

**没有校验的情况。** 表的 `bucket.num` 原为 4，ALTER 改为 6。旧客户端缓存了表级值 4，写一个 ALTER 后新建的分区（实际 6 桶）时用 `hash(key) % 4` 算 `bucket_id`，结果在 0-3 范围内。6 桶布局下 0-3 的 `bucket_id` 都存在，服务端无法通过 `bucket_id` 是否存在发现问题，记录落进了错误的 bucket，导致静默错路由。

**有校验的情况。** ALTER 后 `bucketLayoutEpoch` 变为大于 0。旧客户端的请求不带 `bucket_count_actual`，服务端看到 `bucketLayoutEpoch > 0` 且请求未携带 `bucket_count_actual`，返回 `STALE_METADATA`。客户端刷新元数据后拿到分区级桶数 6，用 `hash(key) % 6` 重新算 `bucket_id`，路由正确。

## Migration Plan and Compatibility

### 升级契约

**1. 滚动升级窗口内** `bucket.num` **不可变更。** 滚动升级期间集群中新旧版本 TabletServer 混合，旧版本 TabletServer 不具备 `bucket_count_actual` 校验能力。如果此时执行 ALTER，旧客户端用新的表级桶数路由旧分区时，旧 TabletServer 无法拒绝，可能导致静默错路由。因此 ALTER 必须在所有TabletServer 都升级完成后才能执行。这是运维层面的约束，用户需要遵守：不要在滚动升级期间执行 `ALTER TABLE ... SET ('bucket.num' = N)`。服务端强制（coordinator 在执行 ALTER 前校验所有活跃 TabletServer 的最低版本）留作后续 FIP 的目标。

**2. 推荐先升级客户端，再升级集群。** 如果先升级集群（服务端）再升级客户端：

*   老客户端在表未被 ALTER（`bucketLayoutEpoch`\=0）之前可以继续用——服务端接受不带 `bucket_count_actual` 的请求

*   但表被 ALTER（`bucketLayoutEpoch`\>0）之后，老客户端的请求被服务端以 `STALE_METADATA` 显式拒绝——老客户端无法读写该表，直到升级


如果先升级客户端：

*   新客户端完全兼容老集群（新 proto 字段是 optional，老服务端忽略）

*   集群升级后无缝衔接，无需额外操作


**3. 新客户端无缝兼容纯老集群和纯新集群。**

*   新客户端 → 老集群：新 proto 字段被老服务端忽略；响应不含 `bucketLayoutEpoch` / `bucket_count_actual`，客户端读到 `bucketLayoutEpoch`\=0，回退到表级桶数。等同于老客户端的行为。

*   新客户端 → 新集群：完全兼容，所有功能可用。


### 兼容性矩阵

按服务端版本、客户端版本、表 `bucketLayoutEpoch` 三个维度划分：

| 服务端 | 客户端 | bucketLayoutEpoch | 行为 |
| --- | --- | --- | --- |
| 新 | 新 | 0 | 兼容。`bucketLayoutEpoch`\=0 时所有分区桶数等于表级值，客户端带不带 `bucket_count_actual` 都能通过校验。 |
| 新 | 新 | `>0` | 兼容。新客户端带 `bucket_count_actual`，服务端校验通过。如果客户端元数据过期，返回 `STALE_METADATA`，客户端刷新后自动恢复。 |
| 新 | 旧 | 0 | 兼容。旧客户端不带 `bucket_count_actual`；`bucketLayoutEpoch`\=0 允许省略。表级桶数对所有分区都正确。 |
| 新 | 旧 | `>0` | 不兼容。旧客户端不带 `bucket_count_actual`；`bucketLayoutEpoch`\>0 拒绝，返回 `STALE_METADATA`。旧客户端无法读写该表，直到升级。 |
| 旧 | 新 | N/A | 兼容。新 proto 字段被老服务端忽略；客户端读到 `bucketLayoutEpoch`\=0，回退表级。 |
| 旧 | 旧 | N/A | 现状，不变。 |

**核心规则**：`bucket_count_actual` 是否可以省略，由 `bucketLayoutEpoch` 决定。`bucketLayoutEpoch`\=0 时省略是安全的（表级桶数对所有分区都正确），`bucketLayoutEpoch`\>0 时省略被拒绝（表级桶数不再等于旧分区桶数）。客户端新旧只间接影响“是否填 bucket\_count\_actual”，服务端不做客户端版本推断，只看请求内容和 `bucketLayoutEpoch`。

### 升级流程

1.  升级所有客户端 / connector。新客户端兼容老集群，升级后立即可用。

2.  升级服务端（CoordinatorServer + TabletServer）。滚动升级期间禁止执行`ALTER TABLE ... SET ('bucket.num' = N)`。

3.  所有服务端升级完成后，可以开始使用 `ALTER TABLE ... SET ('bucket.num' = N)`。


### 已知限制

*   滚动升级窗口内 `bucket.num` 不可变更。 旧版本 TabletServer 不具备 `bucket_count_actual` 校验能力，无法拒绝使用过期桶数的旧客户端请求，可能导致静默错路由。用户需要确保在所有服务端升级完成后再执行 ALTER。服务端强制拒绝留作后续 FIP 的目标。

*   ALTER 后降级不安全。 `TableRegistration` 和 `PartitionRegistration` 的 deserializer 都不校验 version、也忽略未知字段，所以旧服务端读新 znode 时会丢掉 `bucketLayoutEpoch` 和 `bucket_count_actual`，然后按表级桶数路由所有分区。因此降级一个已ALTER 过表的集群是不支持的。

*   已有分区的 rescale 暂不支持（后续 FIP 支持）


## Test Plan

ALTER 语义与原子性测试验证：非分区表 ALTER 被拒绝；`bucket.num` 小于 1 或大于 `max.bucket.num` 被拒绝；回填幂等性（只有缺少 `bucket.num.actual` 的分区被处理，已有值的跳过）；回填过程中某个分区的注册信息不可读时整个 ALTER 中止；ALTER 与分区创建互斥（并发建分区时 ALTER 不会漏掉新分区）；表级更新与回填在同一个 ZK 事务里提交，任一 znode 的 CAS 失败导致整体回滚；coordinator 在 ZK 事务前后 crash 的恢复路径。

端到端读写测试验证：ALTER 后新建分区按新桶数写入、已有分区按旧桶数读写；日志表和主键表均覆盖；动态创建的分区自动采用 ALTER 后的桶数；持有过期元数据的客户端写入新创建分区时收到 `STALE_METADATA` 并在刷新后恢复；缩容方向同样覆盖。

Union-read 测试验证：ALTER 后 Fluss 与湖侧不同分区的桶数各自正确，union-read 枚举按分区级桶数而非表级值；lake 数据中的 bucket id 超出枚举范围时 fail-loud 抛出异常而非静默丢数据；Flink batch 和 streaming、Spark 均覆盖，含 ALTER 后 lake-only 的过期分区。

元数据与序列化测试验证：`PartitionRegistration` 序列化 v2 新增字段读写正确；v1 旧数据无 `bucket_count_actual` 时读为 `null`；`TableRegistration` 序列化 v2 新增字段读写正确；v1 旧数据无 `bucket_layout_epoch` 时读为 0；`TabletServerMetadataCache` 的 `bucketLayoutEpoch` 排序，低 `bucketLayoutEpoch` 的 `UpdateMetadata` 被丢弃；校验在第一次 `UpdateMetadata` 之前的行为。

客户端错误处理测试验证：`Sender` 收到 `STALE_METADATA` 后失败 batch 且不重新入队，失效元数据和 `BucketAssigner`；`LookupSender` 收到 `STALE_METADATA` 后失败 batch 且不进入重试循环；`TABLET_METADATA_NOT_READY` 的重试路径与 `STALE_METADATA` 的快速失败路径区分正确。

Tiering 测试验证：跨多轮 tiering 的每分区桶数 stamp 正确；lake writer 初始化时获得正确的分区级桶数；Fluss 的 `bucket.num` 与 Paimon schema 的 `CoreOptions.BUCKET` 不一致时触发报警。

## Rejected Alternatives

**1. 保持现有表级** `bucket.num` **并通过重写所有数据来 rescale 。**

方法：保持 bucket 数为表级固定属性，按新 bucket 数新建表并重写所有数据。

拒绝理由：

*   重写了没人想改的历史分区，包括已经 tiered 到湖里、本不该被重写的数据。

*   破坏已提交数据的湖侧 bucket 布局。

*   对大表来说是长时间、高风险的迁移。

*   本 FIP 的核心观察是，一个分区的 bucket 布局只需要内部自洽，一个分区表的不同分区可以使用不同的 bucket 数，重写无法只针对新分区，旧分区被迫一起重写，而它们本不需要变。


**2. 用客户端版本或布尔标志替代 bucketLayoutEpoch。**

方法：按客户端协议版本拒绝旧客户端的请求；或持久化一个布尔标志记录是否发生过 ALTER。

拒绝理由：

*   按客户端版本拒绝会在服务端升级后立即拒绝所有旧客户端，即使表从未执行过 ALTER。

*   布尔标志能记录是否 ALTER 过，但两次 ALTER 产生的 UpdateMetadata 都携带 true，TabletServer 无法判断哪条更旧。如果携带旧表级值的消息晚到，布尔值无法让它被忽略。单调递增的 `bucketLayoutEpoch` 通过数值比较解决这个问题。
