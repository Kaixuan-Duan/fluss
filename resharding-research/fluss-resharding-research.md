# Resharding 技术调研

> 调研范围：Apache Kafka（Partition 扩缩容与副本迁移）、Redis Cluster（Hash Slot 与 ASM）、Apache Paimon（Bucket 动态调整）、Apache Doris（临时分区）、Apache Flink（动态并行度调整与状态重分布）、Apache Hudi（Consistent Hashing）、Apache StarRocks（Tablet Split/Merge）

## 1. 概述：什么是 Resharding

**Resharding** 指在系统运行过程中改变分片数量或分片到节点的映射关系。

| 挑战 | 说明 |
|------|------|
| **数据重分布** | 改变分片数后 `hash(key) % N` 的结果变化，已有数据需要搬迁到新分片 |
| **消费位点连续性** | 下游消费者按 `(分片ID, offset)` 记录进度，分片变更后如何衔接 |
| **Key 有序性** | 同一 Key 的消息可能跨分片，保序性断裂 |
| **在线可用性** | 生产系统要求 resharding 期间服务不中断或仅短暂冻结 |
| **一致性** | 迁移过程中不丢数据、不重复数据 |

---

## 2. Apache Kafka — Partition 扩缩容

### 2.1 增加 Partition 数量

Kafka 允许随时通过 `kafka-topics.sh --alter` 增加 Topic 的 Partition 数量。这是一个**元数据级别的即时操作**，新 Partition 立即创建，但：

- **已有数据不会重分布**到新 Partition。Kafka 的存储是不可变的追加日志，搬迁已有记录会使 Consumer Offset 失效。
- Key 到 Partition 的路由发生变化：默认 Partitioner 使用 `murmur2(key) % number_of_partitions`，Partition 数改变后同一 Key 的新消息可能路由到不同的 Partition，**Key 保序性在扩容边界处断裂**。
- **Kafka 至今未解决此问题。** 如果业务依赖同一 Key 的消息到达同一 Partition，则不应改变 Partition 数量。

### 2.2 副本跨 Broker 迁移：追赶-切换机制

`kafka-reassign-partitions.sh` 在不改变 Partition 数量的前提下，将 Partition 副本在 Broker 之间迁移。虽然这不等同于 Resharding（Partition 数不变），但其**数据追赶 → 原子切换**的机制对在线 Resharding 有直接参考价值。

迁移基于 Kafka 已有的 ISR（In-Sync Replica）协议：

```
1. 新 Broker 作为 Follower 加入 → 从 Leader 拉取数据，逐步追赶 Log End Offset
2. 追赶完成 → 新 Follower 加入 ISR
3. 原子切换 → 旧副本下线并删除数据，必要时从新副本集中选举新 Leader
```

这个"追赶 → 追上 → 原子切换"的模式，与 Fluss Resharding 中 Migration Worker 从旧 Bucket Changelog 持续追赶、lag 归零后冻结切换的设计是同构的。副本迁移不影响 Consumer Offset，因为 Partition 身份不变。

### 2.3 小结：Kafka 的局限与权衡

| 维度 | 现状 |
|------|------|
| 增加 Partition | 支持，但已有数据不重分布，Key 保序性断裂 |
| 减少 Partition | **不支持** |
| 副本迁移 | 支持，基于 ISR 协议在线完成 |
| 数据重分布 | **不做**——这是 Kafka 的核心设计选择 |
| 核心权衡 | 以牺牲 Key 保序性为代价，换取扩容的即时性和零数据搬迁成本 |

---

## 3. Redis Cluster — Hash Slot 与 ASM

### 3.1 Hash Slot：固定中间层

Redis Cluster 将 key 空间划分为 **16384 个固定 hash slots**，每个 master 节点负责一个子集。Key 通过 `CRC16(key) % 16384` 映射到 slot。

Hash Slot 的核心价值是**高效定位需要搬迁的数据**。无中间层时扩缩容也只需搬迁部分数据，但必须扫描每个 key 逐一计算 `hash(key) % newN` 判断是否需要搬。有了 Hash Slot（或 VB 前缀），可以直接按 slot/VB 粒度定位和提取数据，无需逐 key rehash。

Flink Key Group 思路相同——通过预设一个远大于物理实例数的固定中间层，使扩缩容变成中间层到物理实例的重新映射，key 到中间层的映射永不改变：

```
Redis:   key → CRC16(key) % 16384         → hash slot  → node
Flink:   key → murmurHash(key) % maxPar   → key group  → subtask
```

### 3.2 Resharding 机制（ASM，Redis 8.4+）

Slot 在节点间迁移采用 Atomic Slot Migration（ASM），分三阶段：

1. **Snapshot**：源节点通过子进程异步创建迁移 slot 的 point-in-time 快照，发送给目标节点。父进程继续正常服务读写。
2. **Streaming**：快照期间产生的新写入，通过 filtered replication stream 持续发送给目标节点追增量。
3. **Finalization**：增量 lag 降到阈值以下时，源节点短暂暂停写入，目标节点处理完剩余变更后原子切换 slot 归属。切换完成后源节点删除已迁出的 slot 数据。

迁移过程中源节点保留完整数据持续服务（复制而非搬迁），只在最终切换时短暂暂停写入。

### 3.3 小结

1. **中间层抽象**：Hash Slot 固定中间层的思路与 Fluss Virtual Bucket 方案一致——在 key 和物理分片之间插入固定大小的中间层，高效定位需要搬迁的数据。
2. **迁移机制**：ASM 的 Snapshot → Streaming → Finalization 与 Fluss 在线重建方案的 KV Snapshot → Changelog 追赶 → 冻结切换同构。

---

## 4. Apache Paimon — Bucket 动态调整

Paimon 主键表提供三种 Bucket 模式：

| `bucket` 取值 | 模式 | 触发 Resharding 的方式 |
|---|---|---|
| `> 0` | **Fixed Bucket** | ALTER TABLE + INSERT OVERWRITE，或 `sys.rescale` 单步过程 |
| `-1` | **Dynamic Bucket** | 自动按 `target-row-num` 创建新 bucket，无需手动 |
| `-2` | **Postpone Bucket** | 写时不路由，落暂存目录；后台 compactor 决定 bucket 数 |

### 4.1 Fixed Bucket 模式：离线 Rescale

当 `bucket > 0` 时，记录按 `Math.abs(key_hashcode % numBuckets)` 分配。Rescale 是两步离线操作：

```sql
-- 第一步：仅更新元数据
ALTER TABLE my_table SET ('bucket' = '8');

-- 第二步：全量数据重组
INSERT OVERWRITE my_table SELECT * FROM my_table;
```

Paimon 也提供 `CALL sys.rescale(...)` 单步过程，内部仍是 INSERT OVERWRITE。

关键特性：
- **读不受影响**（snapshot 隔离），但 INSERT OVERWRITE 必须与其他写入互斥。
- **分区表支持不同 Bucket 数**，可逐分区 rescale。
- ALTER 后未执行 OVERWRITE 就写入新数据，Paimon 会报错。

### 4.2 Dynamic Bucket 模式：自动 Resharding

当 `bucket = -1` 时，Paimon 维护一个**索引**将每个主键映射到其 Bucket。Key 到 Bucket 的分配取决于**到达顺序**而非 hash 取模——Bucket 达到目标行数（`dynamic-bucket.target-row-num`）后，新 Key 自动分配到新建的 Bucket。

- **无跨分区更新**（主键包含所有分区字段）：使用**内存 HASH 索引**，每 1 亿条约 1 GB，非活跃分区不消耗内存。
- **跨分区更新**（主键不包含所有分区字段）：使用**磁盘 RocksDB** 维护全局 Key→(Partition, Bucket) 索引，启动时需扫描全表初始化，大表上非常缓慢。

局限：
- **单写入者限制**：多个并发写入 Job 写入同一分区会导致数据重复。
- **Bucket 耗尽问题**：Assigner 无法感知 Compaction 释放的空间，导致不必要地创建新 Bucket。

### 4.3 Postpone Bucket 模式

当 `bucket = -2` 时，写入路径不做 hash 路由，直接落暂存目录，数据**对 reader 不可见**直到后台 compactor 将其 hash 进真正的 bucket。牺牲 read-after-write 一致性，换取写路径完全无 bucket 决策。不适合实时点查场景。

### 4.4 小结：Paimon 的局限与权衡

| 维度 | Fixed Bucket | Dynamic Bucket | Postpone Bucket |
|------|-------------|----------------|-----------------|
| Resharding 方式 | ALTER + OVERWRITE 或 `sys.rescale`（离线） | 自动 | 写时延迟决策；扩容用 `sys.rescale` |
| 在线程度 | 离线（重写全量数据） | 在线 | 写在线，但 reader 延迟可见 |
| Key→Bucket 映射 | 确定性 hash 取模 | 到达顺序 + 索引 | compaction 时按 hash 取模 |
| 并发写入 | 支持（OVERWRITE 期间互斥） | **单写入者限制** | 多 writer 不冲突 |
| 核心权衡 | 以离线 OVERWRITE 为代价保持确定性映射 | 以索引内存开销和单写限制换取自动扩展 | 牺牲 read-after-write 换取写路径无 bucket 决策 |

---

## 5. Apache Doris — Tablet 不可变

Doris 采用两级结构：先按 partition（range/list）分区，再按 bucket（hash 或 random）分桶，每个 bucket 对应一个 tablet，是数据调度、副本管理、compaction 的最小物理单元。

### 5.1 核心现状：已有分区分片数不可变

Doris **不支持对已有 partition 变更 bucket 数**。`ALTER TABLE MODIFY DISTRIBUTION` 只能改变未来**新建 partition** 的默认 bucket 数，已有 partition 的 tablet 数一旦确定就不可变。

不过 Doris 提供了**临时分区（Temporary Partition）** 机制实现分区级原子 reshard：

```sql
-- 1. 创建临时分区（指定新 bucket 数）
ALTER TABLE t ADD TEMPORARY PARTITION tp1 VALUES [(...), (...))
    DISTRIBUTED BY HASH(col) BUCKETS 8;
-- 2. 将原分区数据导入临时分区
INSERT INTO t TEMPORARY PARTITION(tp1) SELECT * FROM t PARTITION(p1);
-- 3. 原子替换原分区
ALTER TABLE t REPLACE PARTITION(p1) WITH TEMPORARY PARTITION(tp1);
```

替换操作是原子的，不会丢数据。但替换期间原分区需要停写（非在线 reshard）。

Doris 支持的是 tablet replica 在 BE 节点间的自动 rebalance（基于磁盘使用率和副本数均衡调度），但这是**副本均衡**（副本在节点间移动），不是分片数变更。

### 5.2 对 Fluss 的启示

Doris 是 Fluss 试图克服的**反面案例**——建表时的分片决策一锤定音，流量变化后无法弹性调整。

但 Doris 的一个设计值得注意：**不同 partition 可以有不同的 bucket 数**（新分区用新默认值）。这对 Fluss 分区表有参考——resharding 可以**逐分区进行**，新旧分区用不同 bucket 数，降低单次 resharding 的影响范围。

### 5.3 小结

| 维度 | 现状 |
|------|------|
| 分片模型 | partition + bucket（hash/random）= tablet |
| Resharding 方式 | 已有分区不支持直接变更，可通过临时分区原子替换实现分区级 reshard |
| 在线程度 | 临时分区替换期间需停写 |
| 数据重分布 | 临时分区方案需全量重写该分区数据 |
| 核心权衡 | 简单确定 vs 缺乏在线弹性；临时分区提供分区粒度的离线 reshard |
| 对 Fluss | 反面案例；"新分区用新 bucket 数"对分区表逐分区 reshard 有参考 |

---

## 6. Apache Flink — 动态并行度调整与状态重分布

Flink 的 Rescaling 解决的是**有状态流处理算子**在并行度变化时如何重新分配状态的问题。

### 6.1 Key Group：核心抽象

Flink 的 Rescaling 建立在 **Key Group** 概念之上。Key Group 是状态重分布的**原子单位**，总数等于 `maxParallelism`（Job 启动时设定，**不可修改**）。

两级映射：

```
第一级：Key → Key Group
  keyGroupId = MathUtils.murmurHash(keyHash) % maxParallelism
  每个 Key 确定性地映射到一个 Key Group，此映射永不改变。

第二级：Key Group → Operator Subtask
  subtaskIndex = computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupId)
  每个 Subtask 获得一个连续的 KeyGroupRange [start, end]。
```

Rescaling 时 Key 到 Key Group 的映射不变，只改变 Key Group 到 Subtask 的分配：

```
Before (parallelism=4, maxParallelism=128):
  Subtask 0: KeyGroup [0,  31]
  Subtask 1: KeyGroup [32, 63]
  Subtask 2: KeyGroup [64, 95]
  Subtask 3: KeyGroup [96, 127]

After (parallelism=8, maxParallelism=128):
  Subtask 0: KeyGroup [0,  15]    ← 来自旧 Subtask 0 的前半
  Subtask 1: KeyGroup [16, 31]    ← 来自旧 Subtask 0 的后半
  Subtask 2: KeyGroup [32, 47]    ← 来自旧 Subtask 1 的前半
  ...
```

**关键优势**：Key Group 作为中间层，将逻辑分区（Key）与物理并行度解耦。Rescaling 只需按 Key Group 粒度搬迁状态，而非逐 Key 判断。

### 6.2 Rescaling 流程

Flink 的 Rescaling 本质是 **checkpoint-based global failover**，无论手动还是自动触发，都需要全图重启：

```
1. 触发 Checkpoint 或 Savepoint → 生成一致性分布式快照，持久化到外部存储（HDFS/S3）
2. 停止 Job
3. 以新并行度重新提交 Job
4. 启动时从 Checkpoint/Savepoint 恢复：
   - 按新并行度重新分配 Key Group 范围
   - 每个新 Subtask 从快照中读取属于自己的 Key Group 数据
5. 恢复处理
```

状态按 Key Group 组织存储，单个 Key Group 可独立提取。RocksDB 状态后端的 Key 序列化格式为 `[keyGroup, key, namespace]`，keyGroup 作为前缀使数据按 Key Group 有序排列，支持按前缀 range scan 提取——这与 Fluss Virtual Bucket 方案的设计思路同构。

### 6.3 小结：Flink 的局限与权衡

| 维度 | 现状 |
|----|------|
| 核心抽象 | Key Group 作为中间层，解耦逻辑分区与物理并行度 |
| Rescaling 方式 | Checkpoint-based global failover，需要全图重启 |
| maxParallelism | 不可变（决定了 Key Group 数量和 Rescaling 上限） |
| 核心权衡 | 以 Job 重启为代价，换取状态重分布的正确性和简洁性 |
| 关键 | 通过预先划分固定数量的 Key Group，使 Rescaling 变成 Key Group 的重新分配，而非逐 Key 的重新 hash |

---

## 7. Apache Hudi — Consistent Hashing Index

Hudi 的 bucket index 将每个 partition 内数据按 `Hash(recordKey) % N` 分配到 N 个 bucket，bucket 与 file group 一一对应。原始 bucket index（RFC-29）的 bucket 数固定，变更只能重写全表。

### 7.1 核心思路：一致性 Hash + 局部 split/merge

RFC-42 引入 **Consistent Hashing Index**，将"改 bucket 数"从全表重写变为**局部调整**：

```
1. 维护 hash range → bucket 的映射元数据（而非固定 hash % N）
2. split：某个 file group 过大 → 拆成两个，只动这一个 bucket
3. merge：相邻 file group 过小 → 合并，只动这两个 bucket
4. 绝大多数 bucket 不受影响
```

Resize 集成在 clustering service 中，基于文件大小自动触发。基于 MVCC 设计，resize 期间**新数据写入不受阻塞**（INSERT 到其他 file group），reader 读旧 file group，clustering 完成后切换到新 file group。但有两个重要限制：
- **对正在 clustering 的 file group 的 UPDATE 不被支持**，因为 resize 期间记录位置发生变化。
- Clustering pending 期间 writer 需要对新旧 bucket **双写（dual write）**，存在性能开销。

### 7.2 对 Fluss 的启示

一致性 hash 的价值是**局部 resize**——扩缩容只动受影响的分片，而非全表重分布。对 Fluss 尤其有意义的是 **2x 扩容场景**：4→8 时，每个旧 bucket 的数据只需按 `hash(key) % 8` 拆分到自身（bucket i）和一个新 bucket（bucket i+4），天然是局部操作，无需全局 shuffle。

### 7.3 小结

| 维度 | 现状 |
|------|------|
| 分片模型 | Bucket index，hash % N → file group |
| Resharding 方式 | Consistent Hashing，局部 split/merge |
| 在线程度 | 在线（MVCC，但对正在 resize 的 file group 的更新不被支持） |
| 数据重分布 | 局部，只动受影响的 bucket |
| 核心权衡 | 局部 resize vs 元数据复杂度 + 引擎支持不完整（Spark 全支持，Flink 仅 merge 且执行 resize plan 仍需 Spark 离线 job） |
| 对 Fluss | 2x 扩容天然局部，每个旧 bucket 只拆到自身 + 一个新 bucket |

---

## 8. Apache StarRocks — Tablet Split/Merge

StarRocks 同样是 partition + bucket 两级结构，每个 bucket 对应一个 tablet。v4.1+ 引入了 tablet split/merge 能力。

### 8.1 核心思路：按大小自动 split，物理布局持续调整

> **注意**：tablet split/merge 功能**仅限存算分离（shared-data）集群**，存算一体（shared-nothing/经典模式）集群不支持自动 tablet split/merge。

```
Split：tablet 超过 tablet_reshard_target_size（默认 10GB）→ 自动触发
      → 基于 sort key 的实际数据分布做范围切分（而非 hash 重分配）
      → 在线异步操作，由 FE scheduler 调度

Merge：相邻 tablet 过小 → 合并（默认关闭，需手动开启）
      除自动 merge 外，StarRocks 还支持手动 merge（ALTER TABLE ... MERGE TABLETS）
```

v4.1 还引入了 range-based distribution 语义（`enable_range_distribution` 默认 `false`，仅对新建表生效，已有 hash 分布的表不会自动升级）：tablet 可自动 split/merge，**物理布局从建表时的一次性决策变为系统持续调整的状态**。

### 8.2 对 Fluss 的启示

StarRocks 的 split 是**基于 sort key 的范围切分**，与 Fluss 的 hash 分桶语义不同，机制不能直接照搬。但其理念有参考价值：

- **在线异步 split**：resharding 不阻塞读写，后台异步完成
- **物理布局持续调整**：分片数不必在建表时精确预估，系统按实际负载动态调整

不过 StarRocks 的 merge 仍处于保守推出阶段（默认关闭），说明在线合并的复杂度高于分裂。

### 8.3 小结

| 维度 | 现状 |
|------|------|
| 分片模型 | partition + bucket = tablet |
| Resharding 方式 | tablet split/merge（仅 shared-data 集群） |
| 在线程度 | 在线异步（split），merge 默认关闭（也支持手动 DDL） |
| 数据重分布 | 按 sort key 范围切分 |
| 核心权衡 | 动态弹性 vs merge 保守 + 范围切分与 hash 语义不同 |
| 对 Fluss | "物理布局持续调整"理念可借鉴，但范围切分不适用 Fluss hash 分桶 |
