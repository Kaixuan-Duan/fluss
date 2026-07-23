# Fluss Rescale 一期方案

## 0. 背景

### 0.1 当前 Fluss 的 bucket 数是全表统一的

*   **现状**：一张表连同它的所有分区，共用同一个在建表时确定的 bucket 数。分区不持有任何独立的 bucket 数属性。

*   **限制**：没有分区级属性，无法只给某一批分区单独设更大的桶数。


### 0.2 需要分区级别 bucket num

*   **动机**：分区表的数据量随时间增长，晚期分区的数据规模大于早期分区，需要更多的 bucket 来分担读写压力。

*   **全量重刷数据的代价**：改全表 bucket 数，并且必须 `ALTER TABLE` + `INSERT OVERWRITE` 全量重刷数据。这个代价很高，且会触碰、重写历史分区的数据。

*   **一期工作期望**：改桶数只对"生效点之后新建的分区"生效，**老分区维持旧 bucket、不做数据重刷**。


### 0.3 语义统一

上述讨论的一个核心做法：**每个分区可以各自持有自己的桶数，老分区保持不变、新分区采用新 bucket 数**。

但这个做法立刻带来一个语义问题：原本全表唯一的 `bucket.num` 不再能代表这张表的 bucket 数了。同一张表里，老分区可能是 4、新分区可能是 8，`bucket.num` 到底指哪一个值？

要消除这个歧义，考虑把桶数的语义拆成两层，用两个属性分别承载"目标桶数"与"实际桶数"：

*   `bucket.num`：表级属性（现有，语义调整）。从"整表实际桶数"重新定义为**父表预期的终态 bucket 数**，只作为新分区创建时的模板。

*   `bucket.num.actual`：新增属性。语义为**当前实际使用的 bucket 数**。对非分区表是表级属性；对分区表是分区级属性。


**一致性规则**：

*   alter 后的新分区，`bucket.num.actual` 和 `bucket.num` 一致。

*   旧分区 / 非分区表 alter 后，`bucket.num.actual` 和 `bucket.num` 不一致。（或 bulkload 工作做完、旧分区重刷后才一致。）


**回填规则**（保证新老分区属性一致）：

*   对旧表/旧分区：第一次 alter 时，需要为旧分区增加属性 `bucket.num.actual`（补 bucket.num 旧值）。

*   对新表/新分区：建表/建分区时就写入 `bucket.num.actual`。


---

## 1. 实现目标

### 1.1 核心目标

*   **分区级 bucket 数**：不再强制全表统一。

*   **逐分区生效**：修改只影响生效点之后创建的分区，不触碰已存在分区。

*   **新分区用新桶数**：ALTER 表级默认或显式指定后，新建分区按新的 bucket 数创建。

*   **老分区保持现状不变**：已存在分区继续按其原有 bucket 数提供读写服务，**不做任何数据迁移**。


### 1.2 用户交互口径

*   **用户操作面只有表级 ALTER**：用户修改 bucket 数使用 `ALTER TABLE ... SET ('bucket.num' = N)`，这是对表的操作。用户不需要、也不提供任何分区级 ALTER 语法。


### 1.3 属性继承与快照语义

*   **分区新增独立桶数属性，创建时从表级快照**：为分区引入 `bucket.num.actual` 属性，代表该分区实际使用的桶数。分区创建瞬间从当时表级 `bucket.num` 拷贝一次值并落库，此后该分区的 `bucket.num.actual` 在数据重写前不可变。

*   **典型时间线**：建表时 `bucket.num=4` → 期间新建的所有分区都拿 `bucket.num.actual=4`；用户 `ALTER TABLE bucket.num=8` → 期间新建的分区都拿 `bucket.num.actual=8`；两批分区在同一张表下长期共存。


### 1.4 查询语义分层

*   查询表的 bucket.num → 返回当前表级配置（ALTER 前返回 4，ALTER 后返回 8）。它是"父表预期的终态 bucket 数"，不代表任何已存在分区的实际桶数。

*   查询分区 / 非分区表的 bucket.num.actual → 返回当前实际使用的 bucket 数（通过存储过程）。


### 1.5 一期范围界定

一期只交付以下三块，其余均留二期：

1.  **非湖分区表（PK 表 + Log 表）**：`table.datalake.enabled=false` 的分区表，服务端 ALTER/回填、元数据传播、客户端读写/lookup 的分区级桶数路由。

2.  **开湖分区表（PK 表 + Log 表），湖侧只做 Paimon**：`table.datalake.enabled=true` 且 lake format 为 Paimon 的分区表。一期边界为「新分区按新桶数写入 + 老分区维持原桶数不变、Tiering 到 Paimon 对齐」，**不做任何旧数据重刷/重组**。Iceberg、Hudi、Lance 的湖侧实现**不在一期范围**（见 §2.2 / §2.3 / §2.4，仅分析，留二期）。

3.  **fluss-rust 客户端**：Rust 客户端需实现与 Java 客户端**完全一致**的能力（连接、读写、lookup 等），核心是同一套「分区级优先、回退表级」的桶数路由。服务端逻辑两端共用，Rust 侧只做客户端路由适配（见 §7）。

**明确排除（二期）**：任何湖侧旧分区数据重刷/重组（含 Paimon `INSERT OVERWRITE` 重刷）、Iceberg/Hudi/Lance 的湖侧变桶实现、竞态二（INSERT OVERWRITE 与 ALTER 并发）。


---

## 2. 湖流一体问题

### 2.1 Paimon

> **一期范围（唯一纳入实现的湖格式）**：一期开湖只做 Paimon，边界为「新分区按新桶数写入 + 老分区维持原桶数不变、Tiering 到 Paimon 对齐」，不做旧数据重刷/重组。

**Paimon 对 per-partition 桶数的真实支持（源码核实）**：

*   **持久化**：每个数据文件的 manifest 记录 `FileEntry.totalBuckets()`，即该分区写入时的桶数。因此**每个分区的实际桶数在 Paimon 元数据里是存着、可读回的**，同一张表允许"旧分区 4 桶、新分区 8 桶"共存。

*   **读侧 total-aware**：按分区各自桶数规划，跨异构桶数分区的读天然正确。

*   **写侧校验**：`AbstractFileStoreWrite` 的 `numBuckets` 取自表 schema 的 `CoreOptions.bucket()`（单一表级值）。写"已有分区"时会读出该分区历史 `totalBuckets` 与 writer 的 `numBuckets` 比对，不一致则抛异常；写"全新分区"（无历史文件）则按 writer 的 `numBuckets` 盖桶数。旧分区的新文件会继承其历史桶数（`firstNonNull(历史 totalBuckets, numBuckets)`）。

也就是说：Paimon Fixed Bucket 本身支持每分区不同桶数；它只在"writer 桶数与分区历史桶数不一致地追加"时报错。**最终实现不关闭该校验**（早期方案曾用 `withIgnoreNumBucketCheck(true)` 绕过，已废弃）：Tiering writer 创建时用该分区的 `bucket.num.actual` 覆盖 writer 桶数（bucketOverride，经 `table.copy(schema.copy(BUCKET=N))` 的内存表视图），使 writer 桶数与分区历史/实际布局恒等，校验天然通过并保留为湖侧原生防线——若 Fluss 路由出 bug 或外部直写污染布局，tiering 会响亮失败而非静默错写。

对照 Fluss 四种表/分区组合，`ALTER bucket.num` 之后的读写行为（修正版）：

| 组合 | ALTER 后写"已有分区/表" | ALTER 后写"全新分区" |
| --- | --- | --- |
| 非分区 pk 表 | 不在一期范围（Fluss 直接拒绝 ALTER bucket.num） | N/A |
| 非分区 log 表 | 同上 | N/A |
| 分区 pk 表 | **可继续写**，旧分区维持原桶数（tiering 靠"分区历史 `totalBuckets` + writer 桶数按分区实际值覆盖（bucketOverride）"） | 可写入，按新 bucket 数 |
| 分区 log 表（有 `bucket.key`，Fixed Bucket） | 同上 | 可写入，按新 bucket 数 |
| 分区 log 表（无 `bucket.key`，Unaware Bucket） | 桶数无关（全写 bucket-0），天然可写 | 天然可写 |

> 湖流一体的 log 表需要讨论是否存在`bucket.key`。其 Paimon 侧分桶策略由是否存在`bucket.key`决定：存在 → Fixed Bucket；不存在 → Unaware Bucket。三种 Fluss 分桶策略映射如下：

| Fluss log 分桶策略 | 触发条件 | 对应 Paimon 分桶策略 | Paimon 如何 rescale bucket |
| --- | --- | --- | --- |
| Hash | 指定 `bucket.key` | **Fixed Bucket**：`BUCKET=numBuckets`、`BUCKET_KEY=bucketKeys` | 每分区独立桶数由 `FileEntry.totalBuckets` 持久化、读侧 total-aware。旧分区继续按其历史桶数写（tiering writer 桶数被覆盖为分区实际值，与历史布局恒等，原生校验保持开启并通过），新分区按新桶数写；无需 `INSERT OVERWRITE` 重刷。读不受影响。 |
| Sticky（默认） | 无 `bucket.key` | **Unaware Bucket**：`BUCKET=-1` | 无 bucket 数概念、无需 rescale；旧分区 tiering 不受 ALTER 影响。数据全部物理写进 bucket-0，不按桶分文件，扩容靠写并行度和读侧 split。逻辑桶身份靠 \_\_bucket 列携带 |
| RoundRobin | 无 `bucket.key` | 同 Sticky | 同 Sticky。 |

**结论：能否只改 Fluss 源码实现（Fixed Bucket，开湖分区表）**

场景：ALTER `bucket.num`（4→8）后，新/旧作业持续往 Fluss 表写，Tiering 再把数据写入 Paimon 的新/旧分区。分两条腿：

*   **写入 Fluss**：`WriterClient` 已实现分区级路由（`cluster.getBucketCount(分区).orElse(表级)`）。旧分区落 0–3、新分区落 0–7，**新作业、旧作业都正确**——与作业新旧无关。

*   **Tiering→Paimon**：Tiering 是独立作业，只读 Fluss 当前元数据，与"谁写的 Fluss"无关。旧分区(4) 与新分区(8) 均可正确写入 Paimon：旧分区新文件继承其历史 `totalBuckets=4`、新分区按 schema 取 8。

四象限（作业新旧 × 分区新旧）逐一成立：

| | 写 Paimon 旧分区(4) | 写 Paimon 新分区(8) |
| --- | --- | --- |
| 旧作业 | ✅ 正确（继承历史 4 桶） | ✅ 正确（按新 8 桶） |
| 新作业 | ✅ 正确（继承历史 4 桶） | ✅ 正确（按新 8 桶） |

**可只改 Fluss 源码实现，无需改 Paimon、无需重刷**，共三处改造：

1.  放开开湖表的 `ALTER bucket.num`，并把新桶数**传播进 Paimon 表 schema 的 `BUCKET`**（使新分区按新桶数盖 `totalBuckets`）。传播走统一的 `LakeCatalog.alterTable` 通道，以 Fluss 属性 `bucket.num` 的 `SetOption` 表达——与被禁止的湖原生 `bucket` 选项按 key 天然分流（用户直改湖侧 BUCKET 仍被拒绝，防两侧手动改散）。**失败语义（Lake-First, fail-loud）**：传播发生在 Fluss ZK 提交**之前**，带有界重试（3 次 + 线性退避），耗尽后**抛异常中止整个 ALTER**——Fluss 侧元数据一字未动，湖可达后重跑同一条 ALTER 即可（传播幂等故重跑安全）。若传播成功后 ZK 提交失败，重跑会再次幂等传播后收敛两侧。中间态期间 Fluss 体系内不受影响（tiering 盖章走分区级 bucketOverride，不读 schema）。（早期曾实现"先 ZK 后传播 + pending 集合懒对账"的方案，因中间态暴露与自动收敛的自身竞态问题而废弃，改为 Lake-First。）

2.  Tiering writer 创建时按该分区的 `bucket.num.actual` **覆盖 writer 桶数（bucketOverride）**：经 `TieringSplitReader` 的分区桶数快照 → `WriterInitContext.bucketCount()`（非 null、构造时解析，分区缺值 fail-fast） → `PaimonLakeWriter` 以 `table.copy(schema.copy(BUCKET=N))` 重建内存表视图。旧分区续写与其历史布局恒等、"ALTER 前未入湖的老分区"首批文件也盖正确桶数；Paimon 原生桶数校验**保持开启**作为湖侧防线（早期的 `withIgnoreNumBucketCheck(true)` 绕过方案已废弃）。仅对 Fixed Bucket 生效（Unaware Bucket 的 BUCKET=-1 编码分桶模式，不可覆盖）。

3.  `TieringSplitGenerator` 的桶枚举由表级 `tableInfo.getNumBuckets()` 改为**分区级实际桶数**，否则旧分区仍会枚举越界 bucket。


### 2.2 Iceberg

> **不在一期范围（留二期）**：以下 Iceberg 分析仅用于方案完整性，一期不实现 Iceberg 湖侧变桶。

Iceberg 支持在不重写历史数据的前提下演进分区规范，多个 spec 版本在同一张表内共存；每个数据文件记录自己的 `spec-id`；读取时按各文件所属 spec 分别做分区裁剪与规划。新分区落在新 spec（新桶数），旧分区仍归属旧 spec（旧桶数），互不干扰。

需要手动执行ALTER TABLE ADD PARTITION FIELD bucket(8, pk\_column) 加新 spec。

**四种表 + bucket.key → Iceberg PartitionSpec 映射**：

| Fluss 表类型 | bucket.key | Iceberg PartitionSpec | 能否 per-partition 变桶 |
| --- | --- | --- | --- |
| 非分区 pk 表 | 有 | `bucket(key, N)` | 无分区概念 |
| 非分区 log 表（有 bucket.key） | 有 | `bucket(key, N)` | 无分区概念 |
| 非分区 log 表（无 bucket.key） | 无 | `identity(__bucket)` | 无分区概念 |
| 分区 pk 表 | 有 | `identity(分区键...) + bucket(key, N)` | 桶数在 spec 里；Iceberg DDL 修改桶数 |
| 分区 log 表（有 bucket.key） | 有 | `identity(分区键...) + bucket(key, N)` | 同上 |
| 分区 log 表（无 bucket.key） | 无 | `identity(分区键...) + identity(__bucket)` | 支持 |

限制：

1.  只支持一个 bucket key；因此接入 Iceberg tiering 的表只能配置单个 bucket key（Fluss 本身允许多 key）。

2.  Iceberg 修改 bucket 数后，旧分区的新写入会套用最新的 spec 版本（即新 bucket.num），导致旧分区内混入新旧两种桶数的文件；因此考虑让旧分区不可再接收新写入。


**结论**：有 bucket.key 时，旧分区维持旧 bucket，新分区使用新 bucket 数；无 bucket.key 时，桶数在 Iceberg 侧不可见，新旧分区天然无冲突。一期范围内，Iceberg 的处理策略与 Paimon 完全相同。

> Iceberg 原生数据重刷/重组机制：（二期）

| > 机制 | > 作用 | > 输出使用的 spec | > 能否把旧分区按新桶数重刷 |
| --- | --- | --- | --- |
| > `ADD PARTITION FIELD`（一期需要） | > 演进出新分区规范，如 `bucket(key,3)`→`bucket(key,6)` | > 生成新 spec 供后续写入 | > 只影响之后的新写入，老文件保留旧 spec，桶数不变 |
| > `rewrite_data_files` + `output-spec-id` | > 注册新桶 spec 后，按 spec-id 点名该 spec 重分桶写回 | > 按 spec-id | > 旧分区旧桶数按 `hash(key)%N` 重刷成新桶数 |
| > `INSERT OVERWRITE` | > 注册新桶 spec 并置为最新，使用最新 spec 覆盖写 | > 跟随最新 spec | > 旧分区整体覆盖重写 |

### 2.3 Hudi

> **不在一期范围（留二期）**：以下 Hudi 分析仅用于方案完整性，一期不实现 Hudi 湖侧变桶。

**Hudi 分桶机制总览**：Hudi 的 bucket index 有三种 engine：

| 机制 | 一表内不同分区能否不同桶 | 改桶数方式/代价 | 适用表 |
| --- | --- | --- | --- |
| Simple Bucket Index（表级） | 否（全表一个 N） | 全量重写数据。如果不重写，读旧分区按照新bucket num，出错 | COW/MOR |
| Partition-Level（基于 Simple） | 是（正则按分区配 N） | 新分区按新 bucket.num 创建；旧分区改需存储过程重写 | COW/MOR |
| Consistent Hashing（Tiering 不支持此 Hudi 表属性） | 否 | 桶自动增缩，分裂/合并搬动部分数据 | 仅 MOR |

**Hudi 支持"旧分区旧桶数、新分区新桶数"**，Hudi 提供 **Partition-Level Bucket Index** 按分区正则为不同分区配置不同的固定桶数，未匹配的回落表级默认值。给已存在分区改桶数要走 Spark 存储过程，重写受影响分区的数据。

**四种表 × bucket.key → Hudi 实现**：

| Fluss 表类型 | bucket.key | 能否 per-partition 变桶 |
| --- | --- | --- |
| 非分区 pk 表 | 有（PK 强制） | 无分区概念 |
| 非分区 log 表（有 bucket.key） | 有 | 无分区概念 |
| 非分区 log 表（无 bucket.key） | 无 | 无分区概念 |
| 分区 pk 表 | 有（PK 强制） | 支持（Partition-Level） |
| 分区 log 表（有 bucket.key） | 有 | 支持（Partition-Level） |
| 分区 log 表（无 bucket.key） | 无 | 支持（Partition-Level） |

**结论**：Hudi 格式**支持** per-partition 桶数，机制是 Partition-Level，新分区按新桶数创建、旧分区改桶需存储过程重写。

**Fluss 与 Hudi 的湖流一体示例**

初始状态：Hudi 表分区 a、b 已存在，bucket=4（普通 Simple Bucket Index，无 partition-level 表达式）

**step 1:** 升级为 Partition-Level Bucket Index，同时为新分区 c 设置 bucket=6。**需要停止所有写入 job**

```plaintext
-- 升级到 partition-level，并声明 c 分区用 6 桶  
-- bucket_number=4 是原来的默认桶数（必须指定，否则报错）  
call partitionBucketIndexManager(  
  table      => 'my_table',  
  overwrite  => 'c,6',   -- 正则匹配分区路径中含 "c" 的分区  
  bucket_number => 4,    -- 原默认桶数，a/b 分区继续用 4  
  dry_run    => false  
)
```

此时 a、b 分区桶数不变（仍为 4），c 分区尚不存在，无需 rescale。

重启写入 job，写入分区 c 时自动使用 6 桶。

**step 2:** 将分区 b 的 bucket 改为 10，其余分区不变。

> Hudi 支持使用 Spark 存储过程单独修改某个分区的 bucket 数（二期）

```plaintext
call partitionBucketIndexManager(  
  table    => 'my_table',  
  add      => 'b,10',  
  dry_run  => false  
)  
-- 可以做到：b,10;a,4;c,6  
-- 触发分区 b 的 insert overwrite（4桶 → 10桶）
-- 必须整张表停写，不能只停分区 b
-- replace-commit 是原子操作，读取基于 snapshot 隔离：
-- rescale 进行中：读取看到的是旧版本数据（旧 bucket 数的文件）
-- replace-commit 完成后：读取切换到新版本数据（新 bucket 数的文件）
-- 不存在中间状态，读取不受影响，无需停读。
```

### 2.4 Lance

> **不在一期范围（留二期）**：以下 Lance 分析仅用于方案完整性，一期不实现 Lance 湖侧变桶。

| 维度 | Lance 集成现状 |
| --- | --- |
| 支持的表类型 | 仅 log 表；PK 表建表即拒绝 |
| 分桶 | 无 bucket 概念，桶数/桶键被忽略 |
| 分区 | 无分区概念，分区列作为普通数据列写入 |
| `__bucket` 系统列 | 不注入 |
| alter | 抛 `UnsupportedOperationException`，不接受任何变更 |

**结论**：Lance 集成既无分桶也无分区、且不支持 PK 表与 alter。照常湖流一体，桶数变了对 Lance 没有任何影响。

---

## 3. 关键问题分析

### 3.1 竞态问题

**竞态一：建分区与 ALTER** `bucket.num` **并发**

表初始 `bucket.num=4`；建分区请求先读到表级值 4；随后一个 ALTER 把表级值改成 8；建分区最终用手里的 4 落库，新分区带 `bucket.num.actual=4`。这个分区在 ALTER 发起前就已读值，用旧值 4 创建是自洽的；并发下落到旧值还是新值，两者都算合法。

建分区有三条来源，都能与 ALTER 并发：

| 建分区来源 | 触发场景 | 读 `bucket.num` 来源 |
| --- | --- | --- |
| 手动 DDL（ADD PARTITION） | 用户显式建分区 | ZK 现读 |
| 动态建分区 | 写入命中不存在的分区，写入侧自动补建 | ZK 现读 |
| 自动建分区 | 时间分区按策略周期预建 | 内存 |

Coordinator 的单线程事件队列串行处理的只是各类元数据/成员/副本/快照变更的**写后通知**；而读 bucket 数 + 写 ZooKeeper，这个动作不进队列，由 RPC 线程或 `AutoPartitionManager` 线程直接执行。

解决：按表使用 `ReadWriteLock` —— ALTER bucket.num 持写锁，create/drop partition（含自动分区）持读锁，保证"读表级值 → 注册分区"不与 ALTER 交错。（早期考虑过纳入 coordinator 单线程事件队列，已否决，见 §6.2.3。）

> **竞态二：INSERT OVERWRITE 与 ALTER** `bucket.num` **并发** （二期）

> 表初始 `bucket.num=4`，某次 ALTER 已将其改为 8；此后发起一次 INSERT OVERWRITE，它按当时读到的表级值 8 组织数据；执行期间又触发一次 `ALTER bucket.num=16`。INSERT OVERWRITE 用的仍是它启动时读到的 8，覆盖写按其启动时刻的表级值进行，结果自洽合法。

*   取消 INSERT OVERWRITE，按最新的表级值重来。这在语义上更符合直觉，但技术上不好实现： OVERWRITE 由外部引擎执行，Coordinator 改的是 ZK 上的表级值，没有现成机制让 Flink 作业感知 ALTER 事件，Coordinator 在什么时机、以什么通道通知作业，作业收到后如何安全中止并重启。

*   不打断 INSERT OVERWRITE，按启动时读到的值跑完。这样这个分区先被重刷成 8，之后用户若仍想要 16，再发起一次覆盖写即可推到 16。


### 3.2 ALTER 的幂等与原子性保证

一次 ALTER 做两件事：把旧分区回填上各自的 `bucket.num.actual`，再把表级 `bucket.num` 更新为目标值。

第一，ALTER 操作具有幂等性。

*   回填旧分区幂等：遍历老分区时，已有 `bucket.num.actual` 的直接跳过，补写值取自该分区自己的 assignment 大小。

*   表级更新则是把 `bucket.num` 写成一个绝对目标值。因此同一条 ALTER 安全重试。


第二，把对旧分区的回填也作为原子操作。回填与表级值更新合并为一次原子提交。提交要么整体生效、要么整体不生效。

## 4. ALTER 后的读写语义

**根因**：读端和写端原本都用表级 bucket 数来操作。ALTER 后表级值代表的是"父表终态桶数"、凡是拿表级值路由/枚举的地方都会出错

*   写端可能把数据路由到目标分区不存在的高位 bucket

*   读端会对老分区枚举出根本不存在的高位 bucket（多出空 split 或读取报错）。


**修正**：读端和写端都改为**优先按分区取实际桶数** `bucket.num.actual`**，取不到再回退表级值**。

**具体链路**：

*   流读：在 `FlinkSourceEnumerator` 按 bucket 逐个生成 split。需改为分区实际 bucket 数。

*   批读：`TableScan` 的批量 scanner 为分区表按 bucket 枚举。需改为分区实际 bucket 数。

*   点查：主键点查用 `hash(key) % N` 定位到 bucket。需改为分区实际 bucket 数。

*   putkv 与 produce log：KV upsert 写入与 append 日志写入这两条写链路，共同经 `WriterClient` 的 bucket assigner 计算目标 bucket（`hash(bucketKey) % N`）。需改为分区实际 bucket 数。


新旧 split 的切换：本特性不需要 bucket 级切换。新分区是全新分区、老分区桶数创建后永不改写，不存在"同一分区桶数在运行中被改"的情况。

---

## 5. 调研：当前 Fluss 的 bucket 设计现状

> 本章是理解全篇的基础，按"表类型 × 分区维度"四象限梳理，并给出代码证据。

### 5.1 bucket 数的来源与默认值
- 集群默认：`ConfigOptions.DEFAULT_BUCKET_NUMBER`，key `default.bucket.number`，默认 **1**（`ConfigOptions.java:74-82`）。
- 上限：`ConfigOptions.MAX_BUCKET_NUM`，key `max.bucket.num`，默认 **4096**，语义为"非分区表 / 分区表每个分区"的上限（`ConfigOptions.java:301-310`）。
- 数据模型：`TableDescriptor.TableDistribution` 持 `@Nullable Integer bucketCount` + `List<String> bucketKeys`（`TableDescriptor.java:457-497`）；null 表示用集群默认。
- 服务端填默认：`CoordinatorService.applySystemDefaults` 在缺失时 `withBucketCount(defaultBucketNumber)`（`CoordinatorService.java:603-614`）。
- 到 `TableInfo` 后 `numBuckets` 已被确定为原始 int（`TableInfo.java:424-431`、`getNumBuckets()` 331-333）。

### 5.2 bucket 的两种 assignment 落点（表级 vs 分区级）
- **非分区表**：建表时即生成 `TableAssignment`，写 ZK `/tabletservers/tables/{tableId}`（`CoordinatorService.java:490-497` 生成、`MetadataManager.java:417` 写入、`ZkData.java:462-475`）。
- **分区表**：建表时**不生成** assignment；每次创建分区时生成 `PartitionAssignment`，写 ZK `/tabletservers/partitions/{partitionId}`（`MetadataManager.createPartition:811-883`、`ZooKeeperClient.registerPartitionAssignmentAndMetadata:1065-1122`）。
- assignment 的 `assignments` Map（bucketId → 副本列表）的 **size 即该表/分区的实际 bucket 数**（`TableAssignment.java:36-48`）。
- `PartitionAssignment` 结构：继承 `TableAssignment`，自身只多一个 `tableId`（`PartitionAssignment.java:36-41`）；bucket 信息全在父类的 `Map<Integer, BucketAssignment> assignments`（`TableAssignment.java:36`），key=bucketId、value=该 bucket 的副本 server 列表（首个为 leader，如 `{0:[0,1,3]}`）。**桶数不是独立字段，而是 `assignments.size()` 隐式得出**——这是"当前分区没有显式桶数属性"的直接体现，也是本设计要新增字段的动因。

### 5.3 日志表（Log Table） vs 主键表（PK Table）的差异
- 相同点：bucket 数来源、assignment 生成、ZK 落点均一致（bucket 数机制与表类型无关）。
- 差异仅在**路由算法**，且真正的判定标准是 **bucket key 是否为空**（`WriterClient.java:353` 的 `!bucketKeys.isEmpty()`），与"日志表/主键表"没有直接关系。三种情况：
    - 主键表（未单独指定 bucket key 时默认取主键去掉分区键作 bucket key）→ bucketKeys 非空 → `HashBucketAssigner`，`hash(bucketKey) % numBuckets`（`WriterClient.java:357`、`HashBucketAssigner.java:26-42`、`FlussBucketingFunction.java:30-42`）。
    - 无主键的日志表**但显式指定了 `bucket.key`** → bucketKeys 非空 → 同样走 `HashBucketAssigner`（日志表也能 hash 分桶）。
    - 无主键的日志表**且未指定 bucket key** → bucketKeys 为空 → `RoundRobinBucketAssigner` 或 `StickyBucketAssigner`（随机/轮询摊分），由 `CLIENT_WRITER_BUCKET_NO_KEY_ASSIGNER` 决定（`WriterClient.java:359-364`；对应 `bucket.key` 描述 "distributed to each bucket randomly"）。
- 关键：无论哪种 assigner，`numBuckets` 都统一取自 `TableInfo.getNumBuckets()`（`WriterClient.java:351`）——**这是当前路由绑定表级桶数的根因**。

### 5.4 分区表 bucket 数当前恒等于表级（核心现状结论）
- 手动建分区：用 `table.bucketCount`（`CoordinatorService.java:741`）。
- 自动分区：`AutoPartitionManager` 用 `tableInfo.getNumBuckets()`（`AutoPartitionManager.java:416-418`）。
- 两条路径都不存在 per-partition 独立桶数来源；分区 assignment 的 size 恒等于表级 bucketCount。
- createPartition 的桶数校验已是 per-partition（`MetadataManager.java:849-855`，超限抛 `TooManyBucketsException("...per partition")`）。

### 5.5 ZK 元数据结构速览
- 逻辑元数据 `/metadata/databases/{db}/tables/{table}` → `TableRegistration`（含 bucketCount/bucketKeys）；其下 `/partitions/{partitionName}` → `PartitionRegistration`（**不含 bucket 信息**）。
- 运行时 assignment `/tabletservers/tables/{tableId}` → `TableAssignment`；`/tabletservers/partitions/{partitionId}` → `PartitionAssignment`。
- 分区元数据 + 分区 assignment 在**同一 ZK 事务**里写入（`ZooKeeperClient.java:1065-1122`）。

### 5.6 bucket 相关属性现状清单
- **表级（已持久化）**：`TableRegistration.bucketCount`（`public final int`，`TableRegistration.java:52`，注册时强制存在）、`TableRegistration.bucketKeys`（`TableRegistration.java:51`）；对应 `TableDistribution.bucketCount/bucketKeys`、`TableInfo.numBuckets/bucketKeys`。
- **分区级（当前无独立桶数字段）**：`PartitionRegistration` 仅 tableId/partitionId/remoteDataDir 三字段、**无 bucketCount**（`PartitionRegistration.java:35-52`）；`PartitionInfo` **无 getBucketCount**（`PartitionInfo.java:33-101`，仅 partitionId/partitionSpec/remoteDataDir）；分区桶数只能从 `PartitionAssignment.assignments.size()` 反推。
- **配置项**：`DEFAULT_BUCKET_NUMBER`、`MAX_BUCKET_NUM`（表/分区通用上限）；Flink 侧 `bucket.num`、`bucket.key` 均在 `ALTER_DISALLOW_OPTIONS`（`FlinkConnectorOptions.java:233-238`）。

---

## 6. 非湖流一体表的 per-partition bucket 数实现

> 本章范围：`table.datalake.enabled=false` 的分区表。开湖（Paimon）分区表的一期实现见 §7，Rust 客户端适配见 §8。
> 结论前置：四种分桶模式共享 `WriterClient.java:351` 的 `bucketNumber` 输入口，per-partition 桶数改造只需下沉这一个入口即可全覆盖；PK 表和 Log 表在服务端 assignment/元数据链路上的改造完全一致，差异只在客户端路由行为的语义边界。

### 6.1 分桶策略全景（非湖流一体分区表）

| 表类型 | 分桶策略 | 触发条件 | Assigner 实现 | 桶号来源 |
|---|---|---|---|---|
| **PK 表**（1 种） | Hash | `bucketKeys` 恒非空（`TableDescriptor.normalizeDistribution:397-417` 缺省=PK 去分区键） | `HashBucketAssigner`（`WriterClient.java:357`） | `hash(bucketKey) % numBuckets`（`HashBucketAssigner.java:40-41`） |
| **Log 表**（3 种） | Hash | 显式指定 `bucket.key`，`bucketKeys` 非空 | `HashBucketAssigner`（`WriterClient.java:357`） | `hash(bucketKey) % numBuckets` |
| | Sticky | 无 `bucket.key`，`client.writer.bucket.no-key-assigner=STICKY`（默认） | `StickyBucketAssigner`（`WriterClient.java:363-364`） | 从 `Cluster.getAvailableBucketsForPhysicalTablePath` 挑一个，粘住直到批切换（`StickyBucketAssigner.java:67-100`） |
| | RoundRobin | 无 `bucket.key`，`client.writer.bucket.no-key-assigner=ROUND_ROBIN` | `RoundRobinBucketAssigner`（`WriterClient.java:361-362`） | 计数器轮循分区物理桶列表（`RoundRobinBucketAssigner.java:43-53`） |

共同入口（`WriterClient.java:349-370`）：
- `bucketNumber = tableInfo.getNumBuckets()`（第 351 行）—— **当前一律取表级桶数，是路由绑表级的根因**。
- 判定 `!bucketKeys.isEmpty()`（第 353 行）→ Hash 与非 Hash 二分。

### 6.2 非湖表 Log 表的 per-partition bucket 数实现设计

> 本节范围：`table.datalake.enabled=false` 且无主键的日志表（分区表）。
> PK 表的服务端元数据/assignment 链路与 Log 表完全一致，不需要额外改造；PK 表独有的客户端路由语义边界（lookup 定位）另见后续 6.3 节。

#### 6.2.1 服务端元数据层改造

**a. `PartitionRegistration` 新增 `bucketCount` 字段**

当前 `PartitionRegistration`（`PartitionRegistration.java:35-52`）仅有 `tableId`/`partitionId`/`remoteDataDir`，无桶数。改造：
- 新增 `public final int bucketCount` 字段；
- ZK 序列化路径 `/metadata/databases/{db}/tables/{table}/partitions/{partitionName}` 的数据结构同步增加此字段；
- 反序列化时兼容旧数据：若 ZK 中无此字段，回退取 `TableRegistration.bucketCount`（等同旧行为）。

**b. `PartitionInfo` 新增 `getBucketCount()` 方法**

`PartitionInfo`（`PartitionInfo.java:33-101`）当前无桶数接口。改造：
- 新增 `private final int bucketCount` 字段及 `public int getBucketCount()` 方法；
- 该值即 `bucket.num.actual` 在代码层的体现。

#### 6.2.2 建分区路径改造

**现状**：
- 手动建分区：`CoordinatorService.java:741` 用 `table.bucketCount`（表级）；
- 自动分区：`AutoPartitionManager.java:416-418` 用 `tableInfo.getNumBuckets()`（表级）。

**改造**：两条路径均改为"取当前表级 `bucket.num` 值作为新分区的 `bucketCount`"并写入 `PartitionRegistration`。逻辑不变（仍取表级值），但多做一步：**将该值显式持久化到分区元数据**，使后续可按分区独立查询。

代码路径：
1. `MetadataManager.createPartition`（811-883）：构造 `PartitionRegistration` 时传入 `bucketCount`；
2. `ZooKeeperClient.registerPartitionAssignmentAndMetadata`（1065-1122）：在同一 ZK 事务中写入含 `bucketCount` 的分区元数据。

**自动分区表的缓存刷新**：手动 DDL 与写入侧动态建分区都在 RPC 线程内现读 ZK 的表级值，天然拿到 ALTER 后的最新桶数；但 `AutoPartitionManager` 用的是内存里缓存的 `TableInfo`，不会自动感知 ALTER。因此当一条 ALTER 只改了 `bucket.num`（没有触发自动分区策略变化）时，`CoordinatorEventProcessor` 处理表变更通知须额外调用 `updateAutoPartitionTables(newTableInfo)` 刷新该缓存，使其后自动预建的分区以更新后的表级桶数作为自身 `bucket.num.actual`，与手动/动态建分区口径一致。

#### 6.2.3 ALTER TABLE 路径改造

**现状**：`bucket.num` 在 `FlinkConnectorOptions.ALTER_DISALLOW_OPTIONS`（`FlinkConnectorOptions.java:236`），ALTER 直接被拒绝。

**改造**：

1. **解除禁用**：从 `ALTER_DISALLOW_OPTIONS` 中移除 `BUCKET_NUMBER.key()`，允许 `ALTER TABLE ... SET ('bucket.num' = N)`。
2. **表级属性更新**：ALTER 将新值写入 `TableRegistration.bucketCount`（即 `bucket.num`）——该路径已存在，只需解禁。
3. **旧分区回填**（首次 ALTER 时，一次性操作）：
    - 遍历该表所有已存在分区的 `PartitionRegistration`；
    - 对 `bucketCount` 字段为空/未设的旧分区，回填 `bucketCount = PartitionAssignment.assignments.size()`（即当前实际桶数）；
    - 此回填**必须整体先于表级 `bucket.num` 更新完成**——否则旧分区回退取 `TableRegistration.bucketCount` 时会拿到新值，导致路由错误。回填与表级切换的原子性与失败恢复语义见下文第 5 点。
4. **校验**：解除禁用后，ALTER 路径需自行校验合法性——湖表（Paimon）**放开**，经 Lake-First 先传播新桶数到湖 schema 再提交 Fluss（见 §2.1）；未支持 rescale 的湖格式（Iceberg 等）由 `LakeCatalog.alterTable` 抛 `UnsupportedOperationException` 中止 ALTER；非分区表拒绝（需重建 assignment 并在 TabletServer 初始化 LogTablet，本期不支持）、新值须落在 `[1, MAX_BUCKET_NUM]`（下界 1、上界默认 4096），越界分别抛 `InvalidAlterTableException` 与 `TooManyBucketsException`。

**5. 原子性与失败恢复**

一次 ALTER 要落两类 ZooKeeper 写：对每个旧分区各写一次 `PartitionRegistration`（回填 `bucket.num.actual`），以及一次 `TableRegistration`（更新表级 `bucket.num`）。这两类写必须整体原子——只要"部分旧分区已回填、表级值未改"或"表级已改、部分旧分区未回填"的中间态能被读写路径观察到，老分区就可能回退到错误的桶数。

**设计目标：合并为单一 ZooKeeper 事务提交。** 将所有旧分区的回填 op 与表级更新 op 用 Curator 的 `transactionOp().setData()` 逐个收集进一个 op 列表，一次 `zkClient.transaction().forOperations(ops)` 提交。ZooKeeper 的 `multi` 原生保证整个事务要么全部生效、要么全部不生效：提交成功则所有旧分区已带上各自的 `bucket.num.actual`、表级值同时切到新值；提交失败则 ZooKeeper 上一个字节都没变，全表停在改前的自洽状态。因此不存在任何读写路径能观察到的中间态，原子性由存储层直接保证，无需依赖应用层的补偿或不变量推导。

失败恢复因而很简单：事务失败即回到改前状态，直接重试同一条 ALTER 即可；回填取值是各分区 assignment 大小、表级更新是写绝对目标值，均幂等，重试安全。

**事务体积约束**：单个 ZooKeeper 事务受 op 数与 `jute.maxbuffer`（默认约 1MB）上限限制。既有的 `batchRegisterLeaderAndIsrForTablePartition` 以 `MAX_BATCH_SIZE=1024` 分批提交，即为规避该上限。一期分区规模在单事务容量内，直接单事务提交；若未来出现分区数极多、单事务装不下的大表，需另行设计（分批会牺牲全局原子性，届时再评估），不在本期范围。

**当前实现状态**：HEAD 已按单一事务落地——回填改为"只算不写"（`computePartitionBucketCountBackfill` 遍历旧分区、收集待回填的 `PartitionRegistration` 但不提交），再由 `updateTableWithPartitionBucketCountBackfill` 把所有旧分区回填 op 与表级更新 op 放进同一个 `zkClient.transaction().forOperations(ops)` 一次提交。原分步多次 `setData` 的写法（`backfillPartitionBucketCount` 循环内各调一次 `updatePartitionRegistration` 再单独 `updateTable`）已废弃。该改造在 HEAD 完成，待合入 master。

**6. 与并发建分区的安全性（竞态一）**

回填与表级切换运行在处理 ALTER 请求的线程上，而建分区（手动 DDL、写入侧动态补建走 RPC 线程，自动分区走 `AutoPartitionManager` 线程）不经同一串行队列，可与 ALTER 并发（§3.1 竞态一）。

该竞态即便不加锁也无害，原因有三。第一，任何在 ALTER 窗口内被创建的分区，都会把它当时读到的表级 `bucket.num` 原子写入自身 `PartitionRegistration`——读到旧值 4 就定死 4、读到新值 8 就定死 8，两者都是合法且此后不可变的分区终态。第二，回填事务只覆盖它构建 op 列表时读到的那批旧分区（且只改 `bucket.num.actual` 为空者）；在事务构建与提交之间新建的分区不在该批次内，回填对它无影响，而它落库时已自带桶数，无需回填。二者写的是不同 znode（既有分区 vs 新分区），互不冲突。第三，读写路径统一按"分区级实际桶数优先、取不到再回退表级"取数，新分区无论定死 4 还是 8，其自身路由都自洽。

修正决策：虽无害，但为彻底关闭这一理论窗口，最终仍采用**按表的读写锁**（`MetadataManager.getBucketRescaleLock`，per-table `ReentrantReadWriteLock`）主动串行化 ALTER 与建分区。ALTER bucket.num 持**写锁**（排他）；建分区三条来源（手动 DDL、写入侧动态、自动分区）与 `dropPartition` 持**读锁**（彼此并行，只与写锁互斥）。于是任一建分区要么完全在 ALTER 之前、要么完全在之后，不会交错；建分区之间仍并行、无额外串行开销。

为何不用"纳入 Coordinator 单线程事件队列"：那会把全部建分区/ALTER 串行化，代价过大；读写锁只在改 bucket.num 时短暂排他，且不阻塞建分区之间的并发，代价更小。注意这把读写锁是进程内的，只解决"ALTER 与建分区写不同 znode 的读时序竞态"——版本比对（第 8 点）覆盖不了它，因为二者不争抢同一 znode、版本互不触发；而跨协调器的过期提交由第 8 点的版本比对与纪元栅栏兜底。两者互补：锁管进程内读时序，CAS+栅栏管过期/失主写。

**7. 回填完整性：分区元数据不完整时整条 ALTER 失败**

问题：回填遍历时若某个"已在分区列表中列出"的分区，其 `PartitionRegistration` 或 `PartitionAssignment` 读不到（并发 drop 的中间态、ZK 手动改动、脑裂、znode 损坏等），早期实现是静默 `continue` 跳过该分区。后果：这个分区不进回填集合，事务提交后它在 ZK 上仍缺 `bucket.num.actual`，而表级值已切到新值——之后读写按"分区级优先、取不到回退表级"取数时，该分区会回退到**新**表级桶数，用错桶路由。

修正：这两处不再跳过，改为抛 `InvalidAlterTableException` 让**整条 ALTER 失败**。回填在事务提交前计算，抛出时 ZK 一个字节都没改，ALTER 安全可重试；运维处理完元数据不一致后重跑即可。原则：宁可整体失败可重试，也不提交一个不完整的回填。已有 `bucket.num.actual` 的分区仍按幂等跳过。

配套：`CoordinatorService.dropPartition` 也持该表读锁（见第 6 点），使**合法**的并发 drop 不会与 ALTER 的回填枚举重叠、造成"分区在 `getPartitions()` 与 `getPartition()` 之间消失"的假失败；上面的抛错因此只在真实元数据不一致时触发。

**8. 防止提交过期快照：版本比对（CAS）与协调器纪元栅栏**

问题：第 5 点的单一事务只保证"要么全写、要么全不写"（不半写），但**不保证写的是最新快照**。两种过期提交它挡不住：其一，另一条不改 `bucket.num` 的普通属性 ALTER 先读到旧 `TableRegistration`、期间 bucket.num ALTER 提交、它再用手里的旧副本经 `updateTable` 无条件写回，把新桶数覆盖掉，破坏路由契约（过期读改写）；其二，已失去 leadership 的旧协调器发来的在途请求仍会被 ZK 接受。

修正：给 ALTER 的所有 ZK 写加乐观并发控制。读取时用 `storingStatIn` 同时拿到 znode 数据与版本（`getTableWithVersion`/`getPartitionWithVersion`）；写入时每个分区 op、表级 op 都带 `.withVersion(读到的版本)`（compare-and-set），并对整个事务追加一个对 `/coordinators/epoch` 的版本 check（`wrapRequestsWithEpochCheck`，复用其它 fenced 协调器写的同一机制）。任一版本对不上（过期读改写）或 epoch 变了（已失主）→ 整个 `multi` 以 `BadVersionException` 失败，ZK 零改动。普通属性 ALTER 的 `updateTable` 也走同一套 CAS+栅栏，不能再覆盖并发的 bucket.num 变更。

冲突处理：`alterTableProperties` 内建重试（上限 `MAX_ALTER_TABLE_RETRIES`）。只有"纯 bucket.num 回填"这条无外部副作用、幂等的路径，遇 `BadVersionException` 才重读最新元数据重算重提；走 `preAlterTableProperties`（可能创建/变更外部湖表，非幂等）的路径遇冲突转为可重试的失败，不自动重试，避免重复副作用。协调器纪元版本经 `CoordinatorContext.getCoordinatorZkVersion()` → `CoordinatorEventProcessor` → `CoordinatorService` 供应器传入 `alterTableProperties`。

小结：第 5 点的原子性防"半写"，第 8 点的版本比对与纪元栅栏防"写旧"，二者叠加才完整——原子性单独并不能阻止提交一个已过期的快照。

**当前实现状态**：第 6、7、8 点均在 HEAD 完成，待合入 master。

#### 6.2.4 客户端写路由入口下沉

**当前状态**（`WriterClient.java:349-370`）：
```java
int bucketNumber = tableInfo.getNumBuckets();  // 第 351 行，一律取表级
```

**改造目标**：将 `bucketNumber` 从"表级 `getNumBuckets()`"下沉为"分区级 `getBucketCount()`"。

**三种 Log 策略的影响分析**：

| 策略 | 现有对 `bucketNumber` 的依赖 | 改造点 |
|---|---|---|
| Hash | `hash(bucketKey) % bucketNumber`（`HashBucketAssigner.java:40-41`）。**强依赖桶数做模运算**。 | 改为按目标分区的 `bucket.num.actual` 取模。写入哪个分区由分区键决定（WriterClient 在第 351 行之前已确定 `physicalTablePath`），从中可查分区级桶数。 |
| Sticky | `bucketNumber` 仅在 fallback（`availableBuckets.isEmpty()` 时 `random % bucketNumber`，`StickyBucketAssigner.java:78`）使用。主路径用 `Cluster.getAvailableBucketsForPhysicalTablePath`——**已天然是分区维度**。 | fallback 中的 `bucketNumber` 改为该分区的 `bucket.num.actual` 即可；主路径无需改动。 |
| RoundRobin | 轮循 `Cluster.getAvailableBucketsForPhysicalTablePath` 的列表（`RoundRobinBucketAssigner.java:43-53`）。`bucketNumber` 也仅用于 fallback。 | 同 Sticky：fallback 中 `bucketNumber` 改为分区级值；主路径无需改动。 |

**实现方式**：
- `WriterClient.java:351` 处改为：通过 `physicalTablePath` 查 `Cluster` 获取该分区的 `bucketCount`；
- 若为非分区表或分区未查到（旧数据兼容），回退取 `tableInfo.getNumBuckets()`。

#### 6.2.5 客户端元数据同步

客户端 `Cluster` 对象需持有 per-partition 的 bucket count 信息，以支持 6.2.4 的查询。

**现状**：`Cluster` 已通过 metadata RPC 拿到分区列表及各分区的 `PartitionAssignment`（含 `assignments` map），但未显式存储 per-partition bucket count。

**改造**：
- 方案 A（轻量）：`Cluster` 内部对每个分区缓存 `bucketCount`，来源为 `PartitionAssignment.assignments.size()`。这不需要额外 RPC 字段——assignment 的 size 就是桶数。但这依赖 assignment 完整加载。
- 方案 B（显式）：metadata RPC 的分区元数据响应中增加 `bucketCount` 字段（从 `PartitionRegistration.bucketCount` 透传），`Cluster` 直接存储。更显式、更解耦。

建议采用**方案 B**：与 6.2.1 新增的持久化字段一致，且不依赖 assignment 是否完整加载，对后续 PK 表的 lookup 路径也适用。

#### 6.2.6 用户查询语义分层

与 §1 实现目标中的查询语义分层对应：
- `DESCRIBE TABLE` / `SHOW CREATE TABLE` → 返回表级 `bucket.num`（`TableInfo.getNumBuckets()`），不变。
- 新增分区级查询入口（如 `DESCRIBE PARTITION` 或 `SHOW PARTITIONS` 增强）→ 返回 `PartitionInfo.getBucketCount()`（即 `bucket.num.actual`）。

#### 6.2.7 客户端读路由（Flink source split 枚举）下沉

> 对应 §3.2 读端语义。写端下沉已在 §6.2.4 完成，本节补齐读端。

**现状（改造前）**：`FlinkSourceEnumerator` 生成 log split 时按表级桶数枚举 bucket：

```java
for (int bucketId = 0; bucketId < tableInfo.getNumBuckets(); bucketId++) { ... }
```

`tableInfo` 在 `start()` 时一次性取得、全程复用。ALTER 把表级值改大后，对所有分区（含老分区）都按新桶数枚举，老分区会多枚举出不存在的高位 bucket（空 split 或读取报错）。

**改造目标**：读端枚举 bucket 时改用目标分区自己的 `bucket.num.actual`，回落表级值。

**前提条件（已满足）**：`ClientRpcMessageUtils.toPartitionInfos(response, defaultBucketCount)` 已将 `PbPartitionInfo.bucket_count` 解析进 `PartitionInfo.getBucketCount()`（原生 `int`；旧 server 不下发该字段时由 `FlussAdmin.listPartitionInfos` 额外拉取表级值填充），`PartitionInfo` 因此总是携带已解析的可靠桶数。

**改造细节**：

1. **枚举器内部 `Partition` 携带桶数**：`FlinkSourceEnumerator.Partition` 新增 `final int bucketCount` 字段（来自 `PartitionInfo.getBucketCount()`，已解析）；仅用于 diff 比较/移除处理的实例以 `NO_BUCKET_COUNT` 哨兵标记，误用于生成 split 时 `checkState` fail-fast。
2. **`getLogSplit` 增加分区桶数入参**：4 参重载接收已解析的 `int bucketCount`；分区路径由调用方传 `partition.getBucketCount()`，非分区路径传 `tableInfo.getNumBuckets()`，方法内无 null 回退。
3. **非分区表**：2 参重载显式传 `tableInfo.getNumBuckets()`（非分区表桶数不可变，表级值定义正确）。

**改造后效果**：老分区只枚举其真实存在的 bucket、新分区按新桶数枚举，读取不重不漏。

> 湖表读端已同步改造：`LakeSplitGenerator` 按 `flussPartition.getBucketCount()`（分区级实际桶数）枚举 Fluss log 侧 bucket，并对 pk 表越界湖桶加防丢数据 guard（详见 §7）。

#### 6.2.8 §1 实现目标验证对照

| §1 验收口径 | 本设计如何满足 |
|---|---|
| 分区级 bucket 数：不再强制全表统一 | 6.2.1 为分区引入独立 `bucketCount`，6.2.2 建分区时从表级快照 |
| 逐分区生效 | 6.2.2 只影响新建分区；6.2.3 旧分区回填不改实际桶数 |
| 新分区用新桶数 | 6.2.2 取当时表级 `bucket.num` |
| 老分区保持现状不变 | 6.2.3 回填的是旧值 `assignments.size()`；6.2.4 路由按分区级桶数 |
| 读端不重不漏 | 6.2.7 读端 split 枚举按分区级 `bucket.num.actual`，老分区只枚举真实存在的 bucket |
| 用户操作面只有表级 ALTER | 6.2.3 只改表级 `bucket.num`，无分区级 ALTER 入口 |
| bucket.num.actual 永久不可变 | 6.2.1/6.2.2 写入后无修改路径（一期内） |

### 6.3 非湖表 PK 表的 per-partition bucket 数实现设计

> 本节范围：`table.datalake.enabled=false` 且有主键的分区表。
> 服务端元数据层（6.2.1）、建分区路径（6.2.2）、ALTER TABLE 路径（6.2.3）、客户端元数据同步（6.2.5）的改造与 Log 表完全一致，已在 §6.2 实现，本节不再重复。
> PK 表的增量改造集中在**客户端 lookup 路由**——这是 PK 表独有的读路径，Log 表不存在。

#### 6.3.1 问题分析：lookup 路由绑定表级桶数

PK 表支持点查（lookup）语义：客户端根据主键定位到具体的 bucket，向对应 TabletServer 发起查询。定位公式为 `hash(bucketKey) % numBuckets`。

**当前代码现状**：

- `PrimaryKeyLookuper`（`PrimaryKeyLookuper.java:71`）：
  ```java
  this.numBuckets = tableInfo.getNumBuckets();  // 构造时取表级桶数
  ```
  在 `lookup()` 方法（line 122）中：
  ```java
  int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);  // 始终用表级桶数
  ```

- `PrefixKeyLookuper`（`PrefixKeyLookuper.java:74`）完全相同的模式：
  ```java
  this.numBuckets = tableInfo.getNumBuckets();  // 构造时取表级桶数
  int bucketId = bucketingFunction.bucketing(bucketKeyBytes, numBuckets);  // line 168
  ```

**问题**：ALTER TABLE 将表级桶数从 4 改为 8 后，所有 lookup 都按 `hash % 8` 计算 bucket ID。但旧分区实际只有 bucket 0-3（桶数为 4），lookup 若路由到 bucket 4-7 则找不到数据或报错。

**对比写路径**：写端在 §6.2.4 需改为 `cluster.getBucketCount(physicalTablePath).orElse(tableInfo.getNumBuckets())`，按分区级桶数路由。lookup 路径需做相同改造。

#### 6.3.2 改造方案：lookup 路由下沉到分区级桶数

**改造目标**：`PrimaryKeyLookuper` 和 `PrefixKeyLookuper` 的 bucket ID 计算从"表级 `numBuckets`"下沉为"分区级 `bucketCount`"。

**前提条件（由 §6.2 改造提供）**：
- `Cluster.getBucketCount(PhysicalTablePath)` 由 §6.2.5 的改造提供，供分区级桶数查询；
- `AbstractLookuper` 已持有 `metadataUpdater` 字段，可访问 `Cluster`；
- 两个 Lookuper 在 `lookup()` 中已通过 `PartitionGetter` 获取分区名，可构造 `PhysicalTablePath`。

**改造细节**：

**a. `PrimaryKeyLookuper.lookup()` 改造**

当前代码顺序：encode keys → resolve partitionId → compute bucketId（用表级桶数）。

改造后：
```java
@Override
public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
    byte[] pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
    byte[] bkBytes = bucketKeyEncoder == primaryKeyEncoder
            ? pkBytes : bucketKeyEncoder.encodeKey(lookupKey);

    Long partitionId = null;
    int effectiveNumBuckets = numBuckets;  // fallback: table-level
    if (partitionGetter != null) {
        try {
            partitionId = getPartitionId(
                    lookupKey, partitionGetter, tableInfo.getTablePath(), metadataUpdater);
            // Resolve per-partition bucket count
            String partitionName = partitionGetter.getPartition(lookupKey);
            PhysicalTablePath physicalTablePath =
                    PhysicalTablePath.of(tableInfo.getTablePath(), partitionName);
            effectiveNumBuckets = metadataUpdater.getCluster()
                    .getBucketCount(physicalTablePath)
                    .orElse(numBuckets);
        } catch (PartitionNotExistException e) {
            return CompletableFuture.completedFuture(
                    new LookupResult(Collections.emptyList()));
        }
    }

    int bucketId = bucketingFunction.bucketing(bkBytes, effectiveNumBuckets);
    TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
    // ... send lookup request (unchanged)
}
```

关键点：
- `numBuckets` 字段保留为 fallback（非分区表、或旧元数据无 bucketCount 时使用）；
- 分区名从 `partitionGetter.getPartition(lookupKey)` 获取——注意 `getPartitionId()` 内部也调用了此方法，可提取为局部变量避免重复计算；
- `effectiveNumBuckets` 在 partition 解析**之后**、bucket 计算**之前**确定。

**b. `PrefixKeyLookuper.lookup()` 改造**

当前代码顺序有问题：compute bucketId（line 168）在 resolve partitionId（line 170-179）**之前**。改造时需要调换顺序：

```java
@Override
public CompletableFuture<LookupResult> lookup(InternalRow prefixKey) {
    byte[] prefixKeyBytes = prefixKeyEncoder.encodeKey(prefixKey);
    byte[] bucketKeyBytes = prefixKeyEncoder == bucketKeyEncoder
            ? prefixKeyBytes : bucketKeyEncoder.encodeKey(prefixKey);

    Long partitionId = null;
    int effectiveNumBuckets = numBuckets;  // fallback: table-level
    if (partitionGetter != null) {
        try {
            partitionId = getPartitionId(
                    prefixKey, partitionGetter, tableInfo.getTablePath(), metadataUpdater);
            String partitionName = partitionGetter.getPartition(prefixKey);
            PhysicalTablePath physicalTablePath =
                    PhysicalTablePath.of(tableInfo.getTablePath(), partitionName);
            effectiveNumBuckets = metadataUpdater.getCluster()
                    .getBucketCount(physicalTablePath)
                    .orElse(numBuckets);
        } catch (PartitionNotExistException e) {
            return CompletableFuture.completedFuture(
                    new LookupResult(Collections.emptyList()));
        }
    }

    // Moved AFTER partition resolution — needs effectiveNumBuckets
    int bucketId = bucketingFunction.bucketing(bucketKeyBytes, effectiveNumBuckets);
    TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
    // ... send prefix lookup request (unchanged)
}
```

关键点：
- **必须调换顺序**：先解析分区 → 获取分区级桶数 → 再算 bucketId。当前代码先算 bucketId 再解析分区，这在桶数统一时无影响，但 per-partition 模式下顺序错了；
- 对非分区表（`partitionGetter == null`），`effectiveNumBuckets` 保持表级值，行为不变。

#### 6.3.3 PK 表流读 split 枚举（读端）

> 对应 §3.2 读端语义中 PK 表部分。§6.2.7 讲的是 Log 表读端；PK 表的流读 split 枚举与 Log 表略有不同，单列说明。

PK 表在 Flink source 侧有两条读路径，均无需按表级桶数枚举，天然安全：

1. **快照 + 增量读（`getSnapshotAndLogSplits`）**：按服务端返回的 `KvSnapshots.getBucketIds()` 枚举 bucket，而不是 `tableInfo.getNumBuckets()`。该 bucket 列表由服务端根据该分区实际的 snapshot/assignment 生成，本就是分区维度，ALTER 表级值不影响老分区返回的 bucket 集合。
2. **纯 log 增量读（复用 §6.2.7 的 `getLogSplit`）**：PK 表在非 snapshot 模式下与 Log 表共用 `getLogSplit`，已按分区级 `bucket.num.actual` 枚举。

因此 PK 表读端**不需要额外改造**：快照读路径天然按分区返回的 bucket 列表枚举，log 读路径复用 §6.2.7 的分区级枚举。点查（lookup）路径是 PK 独有、且与流读 split 枚举无关，其分区级下沉见 §6.3.1/§6.3.2。

#### 6.3.4 改造影响范围

| 组件 | 是否需要改动 | 说明 |
|---|---|---|
| `PrimaryKeyLookuper.lookup()` | ✅ | bucket ID 计算改用分区级桶数 |
| `PrefixKeyLookuper.lookup()` | ✅ | 同上 + 调换 bucketId 计算与 partition 解析的顺序 |
| `PrimaryKeyLookuper` 构造函数 | ❌ | `numBuckets` 字段保留作为 fallback |
| `PrefixKeyLookuper` 构造函数 | ❌ | 同上 |
| `AbstractLookuper` | ❌ | `metadataUpdater` 已是 protected 字段 |
| `LookupClient` / `LookupSender` | ❌ | 只接收 `TableBucket`，不涉及桶数计算 |
| RPC 层 | ❌ | lookup RPC 按 `(tableId, partitionId, bucketId)` 定位，无需改动 |
| 服务端元数据/ALTER 路径 | ❌ | 已在 §6.2 中完成，PK 表和 Log 表共享 |

#### 6.3.5 PK 表不需要额外的 ALTER 拦截

Fluss 当前没有"PK 表禁止 ALTER bucket.num"这类按表类型设置的拦截；§6.2.3 第 4 点的 ALTER 校验只按"是否湖表""是否分区表""新值是否越界"三个维度判定，与主键无关。因此 §6.3.2 的 lookup 路由下沉完成后，PK 分区表天然沿用 §6.2.3 的同一套校验与回填逻辑执行 ALTER bucket.num，无需为 PK 表新增或移除任何拦截分支。这也是 §6.2 强调"PK 表与 Log 表共享服务端 ALTER 路径"的直接体现。

#### 6.3.6 写路由确认

PK 表的写入路由使用 `HashBucketAssigner`（`WriterClient.java:357`），桶号 = `hash(bucketKey) % numBuckets`。

写路径中的 `numBuckets` 在 §6.2.4 下沉为分区级（`cluster.getBucketCount(physicalTablePath).orElse(tableInfo.getNumBuckets())`），PK 表与 Log 表共享同一入口，无需额外改造。

#### 6.3.7 语义约束

- **同一分区内数据一致性**：同一分区的写入和 lookup 使用相同的 `bucketCount`（来自同一个 `Cluster.getBucketCount()` 缓存），保证 `hash(pk) % N` 的 N 值一致，写到哪个 bucket 就能从哪个 bucket 查到。
- **跨分区 lookup**：不同分区可能有不同的 `bucketCount`，同一个 pk 在不同分区被路由到不同 bucketId，这是预期行为（分区间数据独立）。
- **旧分区兼容**：如果旧分区的 `bucketCount` 为 null（历史存量），`Cluster.getBucketCount()` 返回 empty，fallback 到表级值。在 ALTER 之前这与旧行为一致；ALTER 之后由 backfill 确保旧分区有正确的 bucketCount，不会 fallback 到新的表级值。

#### 6.3.8 §1 实现目标验证对照

| §1 验收口径 | 本设计如何满足 |
|---|---|
| 分区级 bucket 数：不再强制全表统一 | 复用 §6.2 的元数据层，lookup 路由按分区级桶数 |
| 逐分区生效 | ALTER 只影响新建分区的桶数；旧分区 lookup 仍用旧桶数 |
| 新分区用新桶数 | 复用 §6.2.2 建分区路径 |
| 老分区保持现状不变 | lookup 取 `Cluster.getBucketCount(分区)` = 旧值，不受 ALTER 影响 |
| 写入与查询一致 | 写路由（§6.2.4）与 lookup 路由（§6.3.2）使用相同的分区级桶数来源 |

### 6.4 测试覆盖

改造横跨服务端 ALTER/回填、元数据端到端传播、客户端读写路由三层，测试须分层覆盖，并补一条打通全链路的客户端端到端用例。

**服务端 ALTER 与回填**（`AlterBucketNumTest`）：覆盖 Paimon 湖表 ALTER 经 Lake-First 传播成功/失败中止、未支持湖格式（UOE）被拒、非分区表 ALTER 被拒、PK 分区表 ALTER 成功（旧分区保留原桶数、表级值更新为新值、ALTER 后新建分区带新桶数）、Log 分区表 ALTER 成功、回填只影响 `bucket.num.actual` 为空的旧分区（已有桶数的分区跳过，验证幂等）。

**自动分区表缓存刷新**（`AutoPartitionManagerTest`）：验证仅改 `bucket.num` 的 ALTER 触发缓存刷新后，自动预建的新分区采用更新后的表级桶数作为自身 `bucket.num.actual`。

**元数据传播与序列化**：`PartitionRegistrationJsonSerde` 的新旧版本互转（v1 无 `bucket_count` 反序列化为 null、v2 正常回读）；`ClientRpcMessageUtils` 把 `PbPartitionInfo.bucket_count` 正确解析为 `PartitionInfo.getBucketCount()`（缺失时填充调用方提供的表级默认值，`PartitionInfo` 恒为已解析 int）。

**客户端端到端**（已落地：`PartitionBucketCountRescaleITCase`，7 例）：建分区表并写入 → ALTER 放大 `bucket.num` → 再建新分区，覆盖以下断言，证明五条读写路径在真实数据上按分区级桶数路由：
- 写入：老分区的 append/upsert 仍落在其原桶数的范围内，新分区落在新桶数范围内；动态创建的新分区亦然（含 stale Table 句柄回归用例）；
- 点查：老分区、新分区的主键 lookup 都能查回自身写入的数据（验证写路由与 lookup 路由的 N 值一致），prefix lookup 同覆盖；
- 流读：老分区只枚举其真实存在的 bucket、新分区按新桶数枚举，不重不漏；
- 批读与 count(\*) 下推：按各分区实际桶数统计，结果与写入条数一致。

现状：服务端、解析层与端到端用例均已落地（另有湖表端到端 `FlinkUnionReadRescaleBucketITCase`、Spark 侧 `SparkLakeRescaleBucketReadTest`、Rust 侧 `rescale_bucket_count.rs`）。

---

## 7. 开湖（Paimon）分区表的一期实现

> 本章范围：`table.datalake.enabled=true` 且 lake format 为 Paimon 的分区表（PK 表 + Log 表）。Iceberg/Hudi/Lance 不在一期范围（§2.2–§2.4）。

### 7.1 一期边界

一期开湖只做「新分区按新桶数写入 + 老分区维持原桶数不变、Tiering 到 Paimon 对齐」，**不做任何旧数据重刷/重组**（含 Paimon `INSERT OVERWRITE`）。也就是说，桶数的分区级语义与 §6 完全一致，Paimon 侧只需保证 Tiering 落表时按分区各自的实际桶数对齐，不引入重刷。

### 7.2 与非湖实现的关系

Fluss 侧的分区级桶数机制（`bucket.num.actual`、ALTER/回填、元数据传播、写/读/lookup 路由）与 §6 完全复用，Paimon 不改变这套主链路。开湖只在 Tiering 落 Paimon 这一段有额外约束：

*   **新分区**：`bucket.num.actual` = ALTER 后的新桶数，Tiering 建对应 Paimon 分区时按新桶数写入，天然无冲突。

*   **老分区**：`bucket.num.actual` 维持旧值，Tiering 继续按旧桶数写入其对应 Paimon 分区，不受 ALTER 影响。

*   **Fixed Bucket（有 `bucket.key` 的 log 表 / PK 表）**：旧分区**可继续 Tiering**，无需重刷。Paimon 按分区持久化 `FileEntry.totalBuckets`、读侧 total-aware，旧分区新文件继承其历史桶数、新分区按新桶数写。需三处 Fluss 侧改造（见 §2.1 结论）：(1) 放开开湖 `ALTER bucket.num` 并把新桶数传播进 Paimon schema `BUCKET`（传播失败 fail-loud，重跑同一 ALTER 收敛）；(2) Tiering writer 按分区 `bucket.num.actual` 覆盖 writer 桶数（bucketOverride，Paimon 原生桶数校验保持开启）；(3) `TieringSplitGenerator` 改用分区级实际桶数枚举。仅"把已有旧分区数据也重排成新桶数"才需 `INSERT OVERWRITE` 重刷，留二期。

*   **Unaware Bucket（无 `bucket.key` 的 log 表）**：全写 bucket-0，桶数无关，天然无冲突。

### 7.3 测试覆盖（已落地）

*   Paimon Tiering 分区表（Fixed Bucket）：ALTER 放大桶数后，旧分区按旧桶数、新分区按新桶数分别 Tiering 到 Paimon，校验 Paimon 侧各分区 `totalBuckets` 与 Fluss 分区桶数一致、旧分区数据不被重排、跨异构桶数分区读回结果正确。（`PaimonTieringTest`、`FlinkUnionReadRescaleBucketITCase`、Spark 侧 `SparkLakeRescaleBucketReadTest`）

---

## 8. Rust 客户端 per-partition bucket 适配

> 本章范围：`fluss-rust` 客户端。要求与 Java 客户端功能**完全一致**。

### 8.1 目标

Rust 客户端需提供与 Java 客户端等价的能力（连接、元数据、读写、lookup 等），并实现同一套「分区级优先、回退表级」的桶数路由。服务端逻辑（ALTER、回填、CAS + epoch fence、元数据传播）由 Java 服务端统一承担，两端共用，Rust 侧**只做客户端路由适配**，不重复实现服务端逻辑。

### 8.2 需要对齐的客户端改造点（对照 Java）

*   **元数据模型**：Rust 侧的 partition 元数据结构需承载分区级桶数（对应 Java `PartitionInfo.getBucketCount()` / `Cluster.getBucketCount(分区)`），并从 RPC 的 `PbPartitionInfo.bucket_count` 解析；缺失时映射为空并回退表级。

*   **写路由**：写入时按「该分区的实际桶数（缺失回退表级）」计算落桶，对应 Java `WriterClient` 的桶数输入口。

*   **读路由**：LogScanner 枚举分区的 bucket 时按该分区实际桶数 `[0, N)`，对应 Java 侧枚举逻辑。

*   **lookup 路由**：PK / prefix lookup 按分区实际桶数定位 bucket，对应 Java `AbstractLookuper.resolvePartitionBucketCount`（PK 与 prefix 共用同一解析）。

### 8.3 工作量与风险

*   核心逻辑就是「读元数据里的分区级桶数、缺失回退表级」，与 Java 侧同构；改造点集中在元数据解析与读/写/lookup 三处路由入口。

*   风险点：Rust 与 Java 的 RPC 序列化版本需对齐（`bucket_count` 字段的 v1→v2 兼容，旧分区无该字段时按空处理）。

### 8.4 测试覆盖（已落地）

*   Rust 客户端端到端：跨桶数分区的写入、lookup、扫描按各分区实际桶数路由，行为与 Java ITCase 一致。（`tests/integration/rescale_bucket_count.rs`，另有 metadata/lookup/writer 单测覆盖补刷与 staging 逻辑）
