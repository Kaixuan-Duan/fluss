# Fluss 主键表动态 Rescale 待解决问题

## 湖流一体适配

### 背景

Rescale 改变 Fluss bucket 数后，Paimon 侧仍是旧 bucket 布局。若不同步处理，Union Read 与 Tiering 都失效。本章仅讨论 Paimon。

### 核心原则

Paimon 只做一次全量数据重组。

### 关键约束

rescale 前后 bucket 布局不同，旧布局产出的增量 Log 无法路由到新布局的 Paimon bucket。因此旧布局的 Tiering 增量要么在 rescale 之前彻底做完，要么整体放弃、走全量重组。

### 由核心原则推导出的两条路径

**路径一：Paimon 侧走新表/新分区 Tiering + swap**

Paimon 不做 INSERT OVERWRITE。rescale 时 Fluss 切到新 partitionId，Paimon 侧对应创建新的临时表/临时分区，由 Tiering 从 Fluss 新布局的 Log offset=0 起把新数据全量灌入这个临时对象。追齐后原子 swap 到正式表名/分区名，旧 Paimon 对象随之清理。 旧布局的 Tiering 尾巴无需处理——旧 partitionId 的 Log 随 stop-write 自然截止，是否 tier 完不影响新对象正确性。

不支持的理由：
1. 表级：有 renameTable 但没有原子的 drop+rename，中间态会丢表
2. 分区级：连 renamePartition 都没有，根本做不了

**路径二：Paimon 侧原地 INSERT OVERWRITE**

rescale 时对 Paimon 目标表/分区直接执行 ALTER + INSERT OVERWRITE，一次性把新布局全量写入原对象。

前提：原表原分区的 Tiering 必须完整消费到 stop-write 之后的 log end offset，否则 OVERWRITE 落地的 snapshot 会丢失未 tier 的尾部数据。手段是等待 Tiering 自然追平，或 Coordinator 触发 force flush 让 Tiering 强制推进到 log end。

对 Paimon 能力要求低，只依赖 ALTER + INSERT OVERWRITE。

// todo

Union Read（Paimon snapshot + Fluss 实时尾巴）

Union Read 在此期间会读到"新 Fluss + 旧 Paimon"的组合。阻塞 Union Read ？

在 fluss swap 后， fluss commit 到 paimon insert overwrite 期间

1、在读老 paimon （如果log衔接不了，重试）

2、纯 Fluss log 消费 job：不受影响，走 lease 切换。 在读老 fluss 的 流式 job（读完 老fluss cdc，在读新的，语义是正确的）

---

## BulkLoad 实现

### 背景

BulkLoad 指 Flink Job 直接输出 SST 文件 / RocksDB 实例 到 DFS，TS 端直接导入 SST 文件 / RocksDB 实例，不走在线 putKv 路径。

源数据读取用 KvSnapshotBatchScanner：停写后等 HW 推进到 log end offset（KvPreWriteBuffer 自动 drain），然后强制触发一次 KV snapshot（RocksDB native Checkpoint + 上传 DFS）。Flink Source 通过 KvSnapshotBatchScanner 从 DFS 下载 snapshot SST 文件到 TM 本地，打开只读 RocksDB 遍历，每 bucket 输出按 pk 字节序有序。checkpoint 完成后 TS 即释放，scan 全程不占用 TS CPU/内存/网络资源，不影响在线流量。
### 核心决策点：排序位置

KvSnapshotBatchScanner 保证每 source channel（对应一个旧 bucket）内 pk 有序，keyBy shuffle 后每 sink subtask 收到 K=oldN 路有序流。SstFileWriter 要求 put 严格单调递增，才能写 SST 文件。

**方案一：Flink 侧多路归并写 SST** 

每 sink subtask 对上游 K=oldN 路有序输入自维护一个 PriorityQueue，弹出当前最小 key 依次写入 SstFileWriter。每 sink 输出的多个 SST 不重叠，属于同一个新 bucket，TS 端 `IngestExternalFile` 时 SST 直接落 bottom level 无需 compact。 sink 维护一个全量迭代器，结束需要 EOF，必须知道下一个value。

**方案二：RocksDB 内部归并**

每 sink subtask 在本地磁盘开一个 RocksDB 实例，上游 K=oldN 路输入直接 put。写入结束后在 Flink 侧执行 compactRange 触发全量 compaction，再将整个 DB 目录上传到 DFS。TS 端下载后直接打开该 RocksDB 实例作为新 bucket 的 KV 存储，无需额外 compaction。

对比：

| 维度 | 方案一 Flink 侧多路归并写 SST | 方案二 RocksDB 内部归并 |
|---|---|---|
| Flink 侧 compaction | 无需 | 需要（compactRange） |
| Flink 侧 RocksDB 实例 | 无需 | 需要（每 sink 一个） |
| Flink 侧本地磁盘 | 无需 | 需要（容纳单 bucket 数据量） |
| 排序实现 | 手写 K 路归并 + PriorityQueue + operator state 覆盖 checkpoint/failover | 委托 RocksDB 内部 memtable + compaction |
| 复杂度 | 略高 | 简单 |


---

## Swap 原语

### 背景

Rescale 完成后需要把分区或非分区表的物理布局从 oldN bucket 换成 newN bucket。每个读写请求看到的布局是完整一致的。

这要求两个前提：新旧两套物理数据在切换前并存于 DFS 不冲突；元数据切换是原子操作。

### 分区表

**现有布局：**

```
ZK 逻辑元数据：
/metadata/databases/{db}/tables/{tableName}/partitions/{partitionName}
  └── { tableId: 5, partitionId: 100, bucketCount: 4, remoteDataDir: ... }

ZK 运行时状态：
/tabletservers/partitions/100
  ├── data: { tableId: 5, assignments: {0→[ts1,ts2,ts3], 1→[...], 2→[...], 3→[...]} }
  └── buckets/{bucketId}/
        ├── leader_isr
        └── snapshots/{snapshotId}

DFS：
{remote.data.dir}/kv/{db}/{tableName}-{tableId}/{partitionName}-p{partitionId}/{bucket}/
```

**新布局：**

ZK 节点结构不变。DFS 路径不变。

### 非分区表

非分区表的运行时状态挂在 `/tabletservers/tables/{tableId}`——同一 tableId 下无法并存两套 bucket 布局。方案是为新布局分配一个全新的 tableId，新旧两套物理对象以不同 tableId 完全隔离并存，swap 时只更新逻辑元数据的 tableId 指针。

**现有布局：**

```
ZK 逻辑元数据：
/metadata/databases/{db}/tables/{tableName}
  └── { tableId: 5, bucketCount: 4, partitionKeys: [], ... }

ZK 运行时状态：
/tabletservers/tables/5
  ├── data: { assignments: {0→[ts1,ts2,ts3], 1→[...], 2→[...], 3→[...]} }
  └── buckets/{bucketId}/
        ├── leader_isr
        └── snapshots/{snapshotId}

DFS：
{remote.data.dir}/kv/{db}/{tableName}-{tableId}/{bucket}/
```

**Rescale 期间（新旧并存）：**

Coordinator 从 `table_seqid` 分配新 tableId=6，按现有 createTable 内部子流程建立新物理对象（ZK 运行时 + Replica 分配 + DFS 目录），BulkLoad 填充新数据。

**Swap 后的布局：**

```
ZK 逻辑元数据（已更新）：
/metadata/databases/{db}/tables/{tableName}
  └── { tableId: 6, bucketCount: 8, partitionKeys: [], ... }

ZK 运行时状态（新）：
/tabletservers/tables/6
  ├── data: { assignments: {0→[ts1,ts2,ts3], ..., 7→[...]} }
  └── buckets/{bucketId}/
        ├── leader_isr
        └── snapshots/{snapshotId}

DFS（新）：
{remote.data.dir}/kv/{db}/{tableName}-6/{bucket}/
```

### 旧数据清理

#### 目标

无缝切换 + 安全删除。下游 Flink Job 读完旧对象所有 bucket 的 log end offset 后，直接从新对象 log offset=0 继续消费，语义上不丢不重。旧物理对象（ZK 运行时状态、TS 本地 RocksDB/Log、DFS remote KV snapshot 与 remote log）只有在无任何消费者持有 lease 时才能删除。

#### Swap 元数据

Swap Commit 时 Coordinator 在 ZK 持久化一条 rescale 记录：

```
/rescale/swap/{tableName}
  ├── data: { oldTableId: 5, swapTs: <ms> }
  └── leases/
        ├── {jobId-1}: { lastRenewTs: <ms> }
        └── {jobId-2}: { lastRenewTs: <ms> }
```

分区表每个分区独立 swap，各自一条记录。

#### Flink 消费切换

**Enumerator 枚举新旧 split：**

Flink Source Enumerator 周期性检查 table metadata，发现 tableId 变化时触发切换：

```
1. Enumerator 探测到 tableId: 5→6（metadata 刷新后发现 tableId 不等于当前 split 的 tableId）
2. 向 Coordinator 注册 lease(oldTableId=5, jobId)
3. 通过 ListOffsets(tableId=5, 每个 bucket, LATEST) 获取旧每个 bucket 的 log end offset
4. 给当前所有旧 split 标记 bounded（endOffset = 该 bucket 的 log end offset）
5. 为新 tableId=6 创建 newBucketCount=8 个新 split（startOffset=0），加入待分配队列
```

**Reader 无缝切换：**

```
6. Reader 继续消费旧 split → 读到 endOffset → 报告 split finished
7. Enumerator 收到所有旧 split finished → 释放 lease(oldTableId=5, jobId)
8. Enumerator 将新 split 分配给 Reader → Reader 从 offset=0 开始消费新对象
```

数据连续性保证：旧对象停写后 log end offset 不再增长，新对象从 offset=0 记录 swap 后的写入。中间无用户变更（BulkLoad 直写 SST 不产生 log record），不丢不重。

#### Lease 机制

Flink Enumerator 感知到 rescale 后，在 `/rescale/swap/{tableName}/leases/{jobId}` 创建一个 ZK 节点，value 记录 `lastRenewTs`，表示该 Job 仍在消费旧对象，Coordinator 不得清理。Job 每 30s setData 更新 `lastRenewTs` 续约；所有旧 split 消费完毕后主动删除该节点释放 lease。

Coordinator 周期扫描 leases/ 下所有子节点，若 `now - lastRenewTs > leaseTimeout`（默认 5min，= 10× 续约周期），判定进程已死，强制删除该 lease 节点。lease timeout 检测的是"进程是否存活"，而非猜测消费耗时——活着的 Job 持续续约，永远不会被误删。

兜底：若 swap 节点存在、leases/ 下从未注册过任何子节点、且 `now - swapTs > rescale.retain.timeout`（默认 10min），也触发清理，覆盖无 lease 能力的旧版本消费者。

#### Coordinator 清理

leases/ 下无子节点时触发清理。Coordinator 向所有持有旧 tableId bucket 副本的 TS 发送 StopReplica(delete=true)，TS 端 drain in-flight 请求后关闭 Replica 并删除本地 RocksDB 与 Log segment。所有 TS ack 后删除旧 tableId 的 ZK 运行时状态，再异步删除 DFS 上的旧数据。最后删除 swap 元数据节点，标志清理完成。整个顺序先物理后元数据，swap 节点最后删——只要它还在，Coordinator 重启后即可重放（每步幂等）。

---


//

1、临时表：完全继承现有表授权，不可见，list 不可见
需要知道自己是临时表

分区自动继承

2、原表 alter schema， 对临时表的写入问题


ZK 逻辑元数据



insert overwrite 切换，先确定正确语义

1、类似 postgres ，truncated 事件

回溯、重新消费

2、等 log 消费完成 （仅在 rescale 场景正确，现在考虑全部通用 ow 场景）

湖表，数据已经在 湖侧，不需要 +I
非湖表，也不需要入湖，不需要 +I




