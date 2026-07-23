# Fluss 主键表动态 Rescale 方案设计

## 一、背景

### 1.1 根问题

Fluss 主键表的 `bucket.num` 建表后固定不可变。用户希望后续能够调整 bucket 数，诉求分为两层：未来数据不要继续使用过时的 bucket 数；历史数据在必要时也能按照新 bucket 数迁移。

### 1.2 核心约束

主键表使用 fixed hash 路由：`bucketId = hash(primaryKey) % bucket.num`。`bucket.num` 的变化会同时影响写入路由、lookup 路由、KV 状态分布、log 消费，因此不能把 rescale 简化成一次元数据修改，还必须伴随数据重分布。

### 1.3 本方案要解决的问题

本方案要在 Fluss 上落地 KV 表 rescaling，具体解决以下问题：

*   **在线扩容**：将 bucket 数从 M 提升到 N，历史数据重分布到新 bucket。

*   **在线缩容**：将 bucket 数从 N 降到 M，释放多余副本和 RocksDB 实例占用的资源。

*   **分区表可逐分区生效**：新分区按新 bucket 数创建，老分区异步 rescale，不同分区可处于不同 bucket 数状态。


---

## 二、调研

| 系统 | 分片模型 | Rescaling 方式 | 对 Fluss 的启示 |
| --- | --- | --- | --- |
| **Redis Cluster** | 16384 固定 Hash Slot → Node | ASM：Snapshot → Streaming → Finalization（在线，按 Slot 粒度搬迁，仅最终切换短暂暂停写入） | Hash Slot 中间层 = Virtual Bucket 思路；ASM 三阶段 = 在线重建 |
| **Paimon Fixed Bucket** | Partition → Bucket（hash % N） | ALTER metadata + INSERT OVERWRITE（离线，全量重写，OVERWRITE 与写入互斥） | 分区表不同分区可有不同 bucket 数 |
| **Paimon Dynamic Bucket** | Partition → Bucket（到达顺序 + 索引） | 自动按行数扩展（在线，无需重分布） | 索引维护成本高，单写限制，路由非确定性不适用 hash 取模点查 |
| **Doris** | Partition → Bucket = Tablet | 已有分区不可变；临时分区原子替换（替换期间停写，全量重写） | "新分区用新 bucket 数"逐分区 rescale 可借鉴 |
| **Flink** | Key Group（maxParallelism 固定）→ Subtask | Checkpoint → 全图重启 → 按新并行度重分配 Key Group（需停 Job） | Key Group 前缀 = VB 前缀思路同构；maxParallelism 不可变是上限约束 |

### 2.1 竞品观察

从相关产品系统中提炼与 Fluss fixed hash 模型最相关的启示：

**Flink Key Group / Redis Hash Slot**：依赖建表时预设的稳定中间分片层，rescaling 变成中间层到物理实例的重映射。启示——可作为迁移模型参考，但 Fluss 需要额外引入中间层 hash 映射，扩缩容只有在同余 hash 映射的 rescale 数的情况下才能享受局部搬迁的收益，否则容易退化为全量重建。1、中间 hash 层的引入比较麻烦，引入 VB 前缀会破坏 prefix lookup 的性能 2、实际上局部搬迁实现复杂，收益有限，实现性价比不高。

// todo

1、sst 裁剪

原地搬迁

[https://issues.apache.org/jira/browse/FLINK-31238](https://issues.apache.org/jira/browse/FLINK-31238)

2、一期，新分区生效 

**Paimon Fixed Bucket / Doris** ：ALTER TABLE 只改 metadata，新分区可以直接使用新 bucket 数。对于Doris来说，旧分区不支持直接修改 bucket 数，Paimon 支持通过 INESERT OVERWRITE 刷新旧表。"新数据"和"历史数据"是两个独立问题，应分开讨论。

### 2.2 结论

bucket.num rescale 本质上分为两层：新数据可以通过修改 metadata 让未来新分区直接按新 bucket 数生效；历史数据则必须通过异步刷新（全量或增量重写）才能让新 bucket 数真正生效。后续方案围绕"历史数据如何刷新"展开。

---

## 三、Fluss 问题拆解

### 3.1 两个子问题

从上述结论，Fluss 的 rescale 拆解为两个独立子问题：

**A. 新数据生效**：让未来新分区直接使用新的 `bucket.num`，无需数据搬迁。

**B. 历史数据生效**：让已存在的分区（或非分区表）中的数据按新 `bucket.num` 重分布。**对分区表**，本质是让 partition name 指向一组新的 bucket；**对非分区表**，本质是让 table path 指向一组新的 bucket。（新 partitionId / 新 tableId 下挂着新 bucket 数的物理对象）。

### 3.2 ALTER TABLE 的语义边界

`ALTER TABLE SET ('bucket.num' = 'N')` 的作用范围取决于表类型：

*   **分区表**：ALTER 后新建的分区按新 bucket 数创建；已有分区不受影响，仍按原 bucket 数运行，KV 数据与 Log 均不改变，需要额外的 rescale 操作完成历史数据重分布。

*   **非分区表**：不受影响，仍按原 bucket 数运行。ALTER 只改了配置值，数据仍路由到旧 bucket，需要额外的 rescale 操作完成历史数据重分布。


后续方案章节围绕"历史数据如何刷新"展开，即已有旧分区和非分区表的 rescale 实现路径。

---

## 四、历史数据刷新方案

### 4.1 两条路径

历史数据刷新有两条可选路径：

**方案一：原地迁移**。在原表/原分区内迁移，通过稳定的虚拟分片中间层定位需要搬迁的数据块，尽量减少搬迁量。需要建表时预设 VB 前缀。问题在于约束多（旧表不兼容、maxVB 不可变、扩缩容仅在整除倍数关系下能实现局部搬迁，如 4→8→16，非整除场景搬迁量大），相对于引入 VB 中间层的复杂度，收益不清晰。**结论**——不作为 fixed hash 主轨。

**方案二：Rebuild-and-Swap**。构建一组全新的物理 Bucket，数据搬迁完成后原子切换：分区表让 partition name 指向新的 partitionId，非分区表让 table path 指向新的 tableId。边界清楚，贴近 Paimon Fixed Bucket 设计。**结论**——作为主轨。

### 4.2 Rebuild-and-Swap 的执行方式

Rebuild-and-Swap 的核心原语是 **INSERT OVERWRITE**。基于"谁来驱动重写"，分为两条路径：

*   **用户手动路径**：用户通过上层引擎（Flink/Spark SQL）执行 INSERT OVERWRITE。

*   **系统全托管路径**：BulkLoad Service 在用户触发 rescale 后，自动完成数据重写 + ATOMIC REPLACE，无需用户手动编写 SQL。


### 4.3 数据重组

核心原则采用 BulkLoad 模式，通过 Flink Job 在线读取原表/原分区数据，直接输出 SST 文件或 RocksDB 实例，保存到 DFS，TS 端直接导入，不走在线 putKv 路径。

**源数据读取**：停写后等 HW 推进到 log end offset（KvPreWriteBuffer 自动清空），然后强制触发一次 KV snapshot（RocksDB native Checkpoint + 上传 DFS）。Flink Source 通过 KvSnapshotBatchScanner 从 DFS 下载 snapshot 到 TM 本地，打开只读 RocksDB 遍历，每 bucket 输出按 pk 字节序有序。

**方案一：Flink 侧多路归并写 SST**

**sink 不需要 RocksDB 排序，由 sink 节点一次输出全局有序的 SST 文件，**

KvSnapshotBatchScanner 保证每 sub channel（对应一个旧 bucket）内 pk 有序，keyBy shuffle 后每 sink subtask 收到 K=oldN 路有序流。

每个上游Bucket的输出流作为一个sub channel， 封装成无界迭代器，通过维护一个 PriorityQueue<Iterator> 实现多路归并，并通过SstFileWriter直接为目标Bucket生成sst文件。且生成的 SST 天然全局有序，无Key Overlap，可以直接作为RocksDB数据快照摄入，TS 端导入时，SST 直接落 bottom level 无需 compact。

**方案二：RocksDB 内部归并**

每 sink subtask 在本地磁盘开一个 RocksDB 实例，上游 K=oldN 路输入直接 put。写入结束后在 Flink 侧执行 compactRange 触发全量 compaction，再将整个 DB 目录上传到 DFS。TS 端下载后直接打开该 RocksDB 实例作为新 bucket 的 KV 存储，无需额外 compaction。

**对比：**

| 维度 | 方案一 Flink 侧多路归并写 SST | 方案二 RocksDB 内部归并 |
| --- | --- | --- |
| Flink 侧 compaction | 无需 | 需要 |
| Flink 侧 RocksDB 实例 | 无需 | 需要 |
| Flink 侧本地磁盘 | 无需 | 需要 |
| 复杂度 | 略高 | 简单 |

### 4.4 Swap 机制

BulkLoad 完成后，需要把逻辑指针从旧物理对象切到新物理对象。

**要求**：元数据和存储物理位置要解绑；新旧两套物理数据在切换前并存不冲突；元数据切换是原子操作。

**分区表**：分区表的运行时状态挂在 `/tabletservers/partitions/{partitionId}`，不同 partitionId 天然隔离。为新布局分配新 partitionId，新旧物理对象并存无冲突，swap 时更新 partitionId 指针即可。

**非分区表**：非分区表的运行时状态挂在 `/tabletservers/tables/{tableId}`，同一 tableId 下无法并存两套 bucket 布局。方案是为新布局分配一个全新的 tableId，新旧两套物理对象以不同 tableId 完全隔离并存。Coordinator 使用新 tableId 建立新物理对象（ZK 运行时 + Replica 分配 + DFS 目录），BulkLoad 填充数据后，swap 只需一次 ZK setData 将逻辑元数据的 tableId 从旧值更新为新值。

---

## 五、湖层协同

（湖表本章仅讨论 Paimon）

湖流一体表的 rescale 采用停写策略，保证 Fluss 与 Paimon 布局切换在同一停写窗口内完成，避免出现两侧 bucket 布局不一致的中间态。

### 5.1 湖流一体表的Rescale过程

核心时序（针对被 rescale 的分区或非分区表）：

第一步，**Fluss 侧停写**。触发 rescale 后，被 rescale 的分区/表进入停写状态，log end offset 定格。

第二步，等待 Tiering 追齐。等待 Tiering 完整消费到停写时刻的 log end offset，或由 Coordinator 触发 force flush 强制推进。此时旧 Paimon snapshot 包含 Fluss 旧对象的全量数据，是后续 OVERWRITE 的数据基线。

第三步，Fluss rebuild。触发 KV snapshot，BulkLoad Job 按新 bucket 数构建新物理对象。

第四步，Paimon INSERT OVERWRITE。对 Paimon 目标表/分区执行 ALTER + INSERT OVERWRITE，用新 bucket 布局全量重写。数据源是 Fluss 新对象的全量 KV 快照（BulkLoad 产出），因此 Paimon 新 snapshot 与 Fluss 新布局在数据内容上完全一致。

第五步 commit 顺序：Fluss rebuild 完成（对外以旧布局生效） → Paimon overwrite 完成（ Paimon commit）（**新 paimon + 旧 fluss**） → Fluss commit swap 记录（对外以新布局生效） 。

第六步，**Fluss 恢复写入**。新写入进入新对象的 Log，offset 从 0 开始增长。Tiering 从 Fluss 新 Log 的 offset=0 开始消费，与 Paimon 新 snapshot 无缝衔接。

**关键收益：**无需追旧 Fluss 表/分区的 CDC——旧 Log 已在停写前完整 tier 到旧 Paimon snapshot 中，新 Log 与新 Paimon snapshot 从 offset=0 起同步演进。

#### 5.1.1  rescale 事务性

事务原子（全成功 or 全失败）failover 怎么保证一致性。

Paimon INSERT OVERWRITE commit，此前所有中间产物（BulkLoad SST、Fluss 新对象、Fluss 内部 swap）对外不可见，失败可整体丢弃回滚；此后必须向前推进到 Fluss commit。

Coordinator 在 ZK 持久化一个阶段游标 /rescale/inflight/{tableName}，failover 后按游标恢复：

*   **Paimon commit 前 failover**：清理临时产物 + 回退 Fluss 内部指针到旧 tableId，rescale 标记失败，用户重试。

*   **Paimon commit 后、Fluss commit 前 failover**：Coordinator 重启后强制重试 Fluss commit（一次 ZK setData，幂等）直到成功。

*   **Fluss commit 后 failover**：rescale 已完成，旧物理对象的清理时机由第六章 lease 机制决定。


### 5.2 Union Read读问题

**核心原则**： 读取顺序永远是老的 paimon -> 老 fluss（所有bucket读完）-> 新fluss cdc

**并发 union read 与 rescale 阶段的关系：**

**case 1**：Tiering 追齐前——老 Paimon + 老 Fluss Log tail 补齐。旧布局。后续作业不停，追新 fluss log。

**case 2**：Tiering 追齐 →  commit 前（含 BulkLoad、swap、Paimon overwrite 全过程）——老 Paimon（完整），后续作业不停，追新 fluss log。

**case 3**：Paimon INSERT OVERWRITE 后 —— 读 Paimon 新 snapshot，然后追 fluss 新 log。（fluss 的 commit 要求一定做完，重试直到成功）

### 5.3 Split 切换机制

1、rescale commit 前启动的 job ，都是先消费旧表/旧分区的 split

2、rescale commit 后，消费完所有 bucket 的旧 split（依据 leo 来判断），再开始枚举新的 split。

**数据连续性保证**：

旧对象停写后 offset 不再增长，Reader 消费到 log end offset 后自然结束；新对象从 offset=0 记录 swap 后的第一条写入，source 无缝衔接新 Log。既不丢数据（旧 Log 完整消费到底、新 Log 从 0 开始），也不重复（旧对象停写后无新增，新对象从 0 起独立记录）。

---

## 六、旧数据清理

### 6.1 目标与原则

无缝切换：Job 读完旧对象所有 bucket 的 log end offset 后，从新对象 offset=0 读，语义不丢不重。

安全删除：旧物理对象（ZK 运行时状态、TS 本地 RocksDB/Log、DFS 上的 KV snapshot 与 log）只有在无任何消费者持有 lease 时才能被 Coordinator 清理。

消费者用 lease 声明"我还在读旧对象"，Coordinator 用"lease 全部释放"作为清理触发条件。

### 6.2 Swap 元数据

Fluss commit 时 Coordinator 在 ZK 持久化一条 rescale 记录：/rescale/swap/{tableName} 存 {oldTableId, swapTs}；其下 leases/ 子树供消费者注册续约。

该节点是清理流程的存储载体和重放依据：只要它还在，Coordinator 重启后可从此节点重放清理动作。

### 6.3 Lease 机制

注册时机：Flink Enumerator 感知到 tableId 变化后，先在 /rescale/swap/{tableName}/leases/{jobId} 创建 ZK 节点（写入 lastRenewTs），再去获取旧 bucket 的 log end offset。

续约：Job 每 30s setData 更新 lastRenewTs。

释放：所有旧 split 消费完毕后，Enumerator 主动删除该 lease 节点。

超时判定：Coordinator 周期扫描 leases/，now - lastRenewTs > leaseTimeout 视为进程已死，强制删除该 lease 节点。lease timeout 检测的是"进程是否存活"，不是猜测消费耗时——活着的 Job 持续续约，永远不会被误删。

兜底：若 swap 节点存在、leases/ 下从未注册过任何子节点、且 now - swapTs > timeout，也触发清理，覆盖不支持 lease 的旧版本消费者。

### 6.4 Coordinator 清理流程

触发条件：leases/ 下无子节点，且 now - swapTs > MinPeriod（保护期）才触发清理

清理顺序（先物理后元数据，每步幂等，允许重启重放）：

向所有持有旧 tableId bucket 副本的 TS 发送 StopReplica(delete=true)。TS 端处理 in-flight 请求后关闭 Replica，删除本地 RocksDB 与 Log segment。

所有 TS ack 后，删除旧 tableId 的 ZK 运行时状态节点 /tabletservers/tables/{oldTableId}（分区表为 /tabletservers/partitions/{oldPartitionId}）。

异步删除 DFS 上的旧 KV snapshot 与 remote log 目录。

最后删除 swap 元数据节点 /rescale/swap/{tableName}，标志本次清理完成。

swap 节点最后删——只要它还在，Coordinator 重启后即可从头重放（每步幂等，重放不会造成脏状态）。

---

## 七、推荐路线

**M1：新数据生效**。ALTER bucket.num 只影响后续新分区，已有分区和非分区表不受影响。这是最小可用版本——用户至少能让未来数据按正确 bucket 数写入。

**M2：历史数据托管刷新**。BulkLoad Service 在用户触发 rescale 后，自动完成数据重写和 ATOMIC REPLACE，无需用户手动执行。

**M3：开湖表协同**。落地开湖表 rescale 的完整流程：Fluss rebuild，Paimon 侧执行 ALTER + INSERT OVERWRITE 刷新数据，Fluss 切换 tableId（或 partitionId）。Tiering 从新 Log 的起始 offset 恢复增量消费。