# RocksQL 双线 Roadmap：内核与分布式图数据库

> 结论：把工作拆成两条线，用一层窄存储接口解耦。内核线负责单机正确性、查询与性能；分布式线负责分片、复制、路由和跨分片执行。建议先完成内核 MVP，同时并行验证 Raft 与路由；两条线在 P2 末完成第一次集成。

**产品终局**：以 KV（RocksDB）为单机存储的分布式属性图数据库，支持标准 Gremlin 子集、强一致单键写入、快照读、水平扩展和多语言客户端。不是通用 SQL 数仓，也不是完整 PostgreSQL 兼容层。

## 1. 当前基线

已合并的 PR #1、#2、#3 交付了一个可运行的单机 SQL 表引擎：表/索引 DDL、CRUD、二级索引、`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`、`GROUP BY`、`HAVING`、点查与索引扫描、CLI 和 CI。

它证明了 RocksDB 上的目录、主键编码、索引维护和聚合执行路径，但仍是一个单机、单写、无事务、无网络的 SQL 原型，不能直接当作分布式图数据库的终态。

### 已确认与待定

| 项目 | 当前结论 | 状态 |
|---|---|---|
| 存储基础 | 单机 RocksDB，分片后每片一个本地引擎 | 已确认 |
| 两条线 | 内核线 + 分布式线，以窄存储接口解耦 | 已确认 |
| 图模型 | 属性图（点/边/标签/属性），RDF 暂不作为第一目标 | 建议，待确认 |
| 查询语言 | 标准 Gremlin（Apache TinkerPop 官方语法）子集优先；不自造方言；SQL 作为后续分析接口 | 已确认 |
| 一致性 | 单键强一致；多键快照隔离；先不做全局可串行化 | 建议，待确认 |
| 首批场景 | 知识图谱/权限关系/风控关系，先选一个可量化场景 | 待确认 |

## 2. 两条线的交付边界

| 维度 | 内核线（Kernel） | 分布式线（Distributed） |
|---|---|---|
| 目标 | 单机图存储、查询、事务、嵌入接口 | 多节点分片、复制、路由、跨分片执行 |
| 首个交付 | 属性图 CRUD + 1–3 跳遍历 + 持久化 | 3 节点 Raft KV + 元数据/路由原型 |
| 不负责 | 副本共识、跨节点事务、集群运维 | 图查询语义、存储编码、单机执行器 |
| 接口 | 图 API / 标准 Gremlin 子集 / C ABI | 只依赖存储节点接口，不感知图内部 |
| 验收 | 单机数据正确、崩溃恢复、基准达标 | 副本一致、故障切换、无数据丢失 |

**解耦接口（P0 冻结 v0）**：存储节点提供 `get`、`scan`（前缀/范围）、`batch_write`、`snapshot`、`transaction`、`split/transfer`。分布式层只依赖这些能力；图查询、索引编码和事务语义留在内核线。

## 3. 内核线工作包

### K1 属性图存储

- **交付物**：点/边/标签/属性的键编码；出边和入边邻接索引；CRUD 与批量导入；持久化和重启恢复。
- **完成定义**：点、边、属性可增删改查；邻接查询按标签过滤；重启后数据一致；有编码单测和故障注入测试。
- **依赖**：复用现有 RocksDB 打开、批写、前缀扫描和键编码经验。

### K2 图查询与遍历

- **交付物**：基于官方 `Gremlin.g4` 的标准 Gremlin 子集（复用 `gremlin-rs` 解析器，不自写语法）；`V`/`E`、`out`/`in`/`both`、`has`/`hasLabel`、`values`/`id`/`label`、`limit`/`path`/`count`；1–3 跳遍历。
- **完成定义**：标准 Gremlin 语料可解析；固定数据集结果确定；未支持 step 明确报错而非降级；有超时、行数和最大跳数保护；见 [`QUERY_SPEC.md`](QUERY_SPEC.md)。
- **状态**：K2a（有界遍历）与 K2b（标准 Gremlin v0 解析/执行）已实现；未支持 step 显式报错。
- **依赖**：K1 的邻接索引和稳定键布局。

### K3 事务与恢复

- **交付物**：MVCC 快照读；单次多键原子写；WAL 恢复；冲突检测策略。
- **完成定义**：崩溃点恢复测试通过；读写快照隔离；冲突返回可识别错误；不出现半写状态。
- **状态**：K3 边界已冻结，K3a 快照读视图、K3b 单分片写事务与 K3c 进程崩溃恢复测试均已实现。见 [`TRANSACTIONS.md`](TRANSACTIONS.md)。
- **依赖**：K1；单分片事务边界已在 `TRANSACTIONS.md` 冻结。

### K4 性能与嵌入

- **交付物**：邻接扫描/缓存/批量遍历优化；稳定 C ABI；Python、Go 客户端；嵌入模式和 server 模式。
- **完成定义**：基准可复现；C ABI 有版本号和错误码；客户端有最小示例和兼容性测试。
- **依赖**：K2/K3 的查询和事务接口稳定后再冻结 ABI。

## 4. 分布式线工作包

### D1 存储节点接口与分片

- **交付物**：`get`/`scan`/`batch_write`/`snapshot`/`transaction` 的窄接口；按点 ID 或范围分片；分片切分与迁移的最小协议。
- **完成定义**：单机实现通过接口测试；分片路由可解释；迁移过程可回滚，不丢数据。
- **依赖**：与内核线在 P0 共同冻结接口 v0。
- **当前状态**：`src/storage_node.rs` 已有单 shard 本地适配：不透明字节 `get`/`scan`/`batch_write`、稳定读视图、epoch 围栏、请求幂等和 export/import；接口测试见 `tests/storage_node.rs`。`src/graph_records.rs` 已提供基于同一 `graph/v1` 布局的图记录适配器（点/边/邻接的 create/update/delete、标签过滤、级联删除和 reopen），并基于同一 read view 的 sequence 条件批写阻止多 store 的陈旧验证提交；冲突返回 typed `Conflict`，不自动重试。旧 StorageNode 实现对条件写默认返回 `Unsupported`，不降级；D2 已有独立版本化 D1V1 复制命令承载条件/幂等语义，并由独立 `replicated-graph` crate 实现同步 StorageNode trait 与 GraphRecordStore 的进程内 Raft 接线（leader 读屏障、有界稳定快照、prepared 请求原样重试）；见 `REPLICATED_GRAPH.md`。`GraphEngine` 接线、`begin` 事务接口、远程分片路由与拆分/迁移仍未实现。

### D2 复制与共识

- **交付物**：Raft 组、日志复制、快照、leader/follower 切换；副本放置策略。
- **完成定义**：3 节点集群可写入；杀掉 leader 后 30 秒内恢复；重复/乱序消息测试通过。
- **依赖**：D1；选型已定 openraft `=0.9.25`，见 [`D2_CONSENSUS.md`](D2_CONSENSUS.md)；三节点原型、多进程 TCP 验证已实现；节点使用 RocksDB `raft.db` 持久化日志与 `state.db` 不透明字节批次状态机，重启可恢复；`Suite::test_all` 已通过；D1 本地字节 `StorageNode` 适配已实现，图引擎接线仍待做。

- **应用语义切片**：版本化 `D1V1` 命令已复制 shard/epoch、request_id、expected sequence 与原子 data/sequence/dedup/applied marker；snapshot/restart 保留重试身份。详见 [`D2_D1_CONTRACT.md`](D2_D1_CONTRACT.md)。已有进程内 `StorageNode`/GraphRecordStore 接线，但未连接 GraphEngine 或远程客户端；固定进程内路由见 D3a；无淘汰去重预算耗尽后明确拒绝新写，不能视为生产容量方案。

### D3 元数据、路由与故障切换

- **交付物**：catalog、分片到节点映射、成员变更、客户端路由缓存、故障检测。
- **完成定义**：节点下线后路由自动收敛；新节点加入可迁移分片；元数据有版本和回滚。
- **依赖**：D2 的副本状态机。

- **D3a 静态 catalog 切片**：独立预配置 metadata D1 group 上的持久化、版本化 partition→shard→group/epoch/replicas catalog；leader barrier 读取、条件注册、精确重试与不可变绑定。进程内 `(group,node)` 隔离；无 quorum 失败关闭。详见 [`D3_STATIC_CATALOG.md`](D3_STATIC_CATALOG.md)。只提供固定候选进程内路由，不支持远程路由收敛、成员变更或迁移，不代表 D3 完成；catalog epoch 不构成跨独立 group 的 fence。

### D4 分布式事务

- **交付物**：跨分片 2PC 或快照事务；事务协调者；超时/恢复/悬挂事务处理。
- **完成定义**：跨分片写入要么全提交要么全回滚；协调者崩溃可恢复；隔离级别文档化。
- **依赖**：D1、D2、D3；内核 K3 的单分片事务接口。

### D5 分布式查询与遍历

- **交付物**：分区感知的计划；跨分片 2 跳遍历；结果合并与去重；超时/部分失败语义。
- **完成定义**：跨分片 2 跳结果正确；单分片查询不退化为全集群扫描；有慢查询和失败指标。
- **依赖**：D3 路由、D4 事务、K2 遍历算子。

### D6 运维与多租户

- **交付物**：部署编排、备份恢复、监控指标、限流/配额、租户隔离。
- **完成定义**：可重复的集群部署；备份可恢复；核心 SLI 有仪表盘和告警。
- **依赖**：D2–D5；在首个生产试点前完成最小集。

## 5. 阶段与退出条件

| 阶段 | 时间（估算） | 交付 | 退出条件 |
|---|---|---|---|
| P0 定义 | 2–3 周 | 四项决策、接口 v0、基准与验收口径 | 图模型/查询语言/一致性/首批场景签字；接口评审通过 |
| P1 内核 MVP | 6–8 周 | K1 + K2 | 单机属性图可 CRUD；1–3 跳正确；1,000 万点/5,000 万边基准可跑 |
| P2 并行 | 6–8 周 | K3 + K4 起步；D1 + D2 + D3 | 单机快照事务；C ABI 雏形；3 节点 Raft KV 可切换 |
| P3 集成 | 8–12 周 | D4 + D5 | 跨分片事务和 2 跳遍历正确；故障注入无数据丢失 |
| P4 硬化 | 持续 | D6、性能、兼容性、文档 | 可重复部署；备份恢复；首个试点验收 |

**关键路径**：P0 接口 → K1/K2 → K3 → D1 → D2/D3 → D4/D5。D2/D3 可在 K2 之后并行启动，但不能绕过 D1 的接口；D4/D5 不得早于 K3 的单分片事务边界。

## 6. 验收指标

| 线 | 指标 | 目标（首版） | 验证方式 |
|---|---|---|---|
| 内核 | 数据规模 | 1,000 万点、5,000 万边可导入并可查询 | 固定生成器 + 可复现基准 |
| 内核 | 遍历延迟 | 1–3 跳 p99 小于 50 ms（硬件待定） | 单机基准，记录查询形状和缓存状态 |
| 内核 | 恢复 | 崩溃后无半写，重启可恢复 | 故障注入 + WAL 回放测试 |
| 分布式 | 副本 | 3 节点、3 副本，leader 故障 30 秒内恢复 | 集群测试 + 网络分区注入 |
| 分布式 | 一致性 | 单键写入线性一致；跨分片事务不丢不重 | 并发写 + 故障恢复校验 |
| 分布式 | 跨分片查询 | 2 跳遍历结果正确，支持超时/部分失败 | 与单机参考结果对拍 |

## 7. 风险与决策门

| 风险 | 预警信号 | 动作 | Owner |
|---|---|---|---|
| 图分区导致跨分片遍历放大 | 2 跳查询命中多数分片；p99 随集群规模上升 | 先限制 2 跳；引入分区亲和/缓存；必要时把深度遍历降级为单分片 | 分布式负责人（待指定） |
| Raft/共识实现复杂 | P2 前半段仍无可用的 leader 切换 | P0 决定采用成熟库；保留自研作为备选，但不在首版并行两套 | 分布式负责人（待指定） |
| 事务范围失控 | 跨分片事务阻塞主路径；隔离级别反复变更 | 首版只承诺单分片快照 + 跨分片 2PC；全局可串行化延后 | 内核负责人（待指定） |
| 两条线接口漂移 | 分布式层开始直接依赖图内部结构 | P0 冻结接口 v0；任何变更走 ADR 和双方评审 | 产品/架构负责人 |
| 范围过大 | 同时做 SQL 兼容、图、分布式、云托管 | 首版只做属性图 + 标准 Gremlin 子集 + 单区域；其余进 backlog | 产品负责人 |

**决策门**：P0 结束前必须确认图模型、查询语言、一致性边界和首批场景；未确认则不进入 P1 实现。P2 结束前必须确认 Raft 方案和存储接口 v1；未通过则不进入跨分片事务。

## 8. Backlog（不排期）

- **SQL 原型**：复合主键、`ALTER TABLE`/`DEFAULT`/`UNIQUE`、JOIN/子查询/CTE、DISTINCT 聚合与窗口函数；在图内核稳定后再评估，不与图能力抢首版资源。
- **图能力**：路径查询、最短路、社区检测等图算法；首版只做属性图 CRUD 与 1–3 跳遍历。
- **运维**：多区域双活、跨云容灾、云托管控制台与计费；首版只做单区域集群。

## 9. 非目标

- 不做完整 SQL 数仓、PostgreSQL 兼容、复杂多表 JOIN 和窗口函数。
- 不做多区域双活、全球强一致和跨云容灾（首版单区域）。
- 不先做 RDF/SPARQL；属性图优先。
- 不先做云托管控制台和计费；先证明单机与 3 节点集群。

## 10. 下一步

1. 确认四项决策：图模型、查询语言、一致性边界、首批场景。
2. 把内核线和分布式线各自拆成独立工作流；内核先启动，分布式先做接口与 Raft 预研。
3. 在 P0 产出接口 v0、基准脚本和验收清单，再进入编码。

当前 PR #3 已合并，作为单机 SQL 原型保留；它不作为分布式图数据库的最终架构，但其 RocksDB 存储、键编码、索引和测试经验可直接复用到内核线。

### 单分片 replicated Gremlin 增量

`GraphRecordStore` 已复用标准 Gremlin v0 解析与算子，以一次 leader barrier
取得固定 D1 view，分页扫描与邻接访问始终经过原 adapter。保留 view 支持同一
sequence 的后续查询；context/epoch fencing 不绕过。此项仅为进程内单分片
闭环，不代表 D5 跨分片遍历或生产查询服务。资源和错误契约见 `QUERY_SPEC.md`。

### Query resource follow-up

The common local/D1 Gremlin executor now enforces a 64 MiB query-owned logical
byte budget, including simultaneous input/output and large-property duplicate
traversers. Defined fees, intentional compatibility impact, and excluded storage
view/allocator costs are in [QUERY_MEMORY.md](QUERY_MEMORY.md). This is not a
global concurrency quota or a heap/RSS hard limit.

### D3a bounded route resolver

固定进程内 `(group,node)` registry 上，新增 fresh metadata quorum lookup、有限候选/leader hint、共享绝对 deadline 与 admission 的路由。读 session 固定原 adapter/view，prepared 写重试保持原 request ID/epoch/sequence；未知结果不内部重试，Conflict/Fenced 终止。此项不含网络客户端、缓存、成员变更或迁移，D3 仍未完成。契约见 `D3_STATIC_CATALOG.md`。

### Same-host graph UDS gateway

Linux 上新增只读 UDS gateway，以受信任目录、socket 权限和 peer UID 限制同主机进程访问。每连接固定一个 resolver snapshot 和 deadline，只支持 open/read/close；帧、响应、并发和慢客户端等待有界。它不是跨主机 replica transport、生产网络服务、写协议或 D3 完成。契约见 `GRAPH_UDS_GATEWAY.md`。
