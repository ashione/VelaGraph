# VelaGraph Roadmap（实验阶段）

> 当前阶段：已有单机图内核、Raft 存储节点、静态路由、有界 observed 跨分片查询及 restricted managed 事务/同版本读取。继续以真实写入、查询和恢复路径补齐功能与容量证据，以 CoreHR 持续验收；容错、在线迁移与生产安全分别推进，实验切片可用不代表 D3、D4、D5 已全部完成。

**产品终局**：VelaGraph 的目标是成为存储与计算解耦的分布式属性图数据库（当前实现以本地 RocksDB/Raft 为持久化基础；对象存储权威与本地缓存是后续规划，尚未实现），支持标准 Gremlin 子集、强一致单键写入、快照读、水平扩展和多语言客户端。不是通用 SQL 数仓，也不是完整 PostgreSQL 兼容层。

## 高优先级：统一 gRPC/Protobuf 与对外服务接口

[#218](https://github.com/ashione/VelaGraph/issues/218) 跟踪内部协议统一及对外
**gRPC + HTTP 写入/查询服务**。按 2026-11-12 的方向提高优先级，与时态和
分布式功能并行推进，不等最终 UI/平台产品化；以下区分已交付切片与剩余验收，不将协议切片等同于完整发布。

截至 main `fa202c51`（2026-11-13），Raft peer、managed control/write、snapshot、
observed fragments/hierarchy shard jobs、fixed/renewable sessions 已有 typed gRPC。
`graph-service` 内部 managed 与 graph routes 已强制 gRPC。旧 driver/control、
coordinator job 入口和兼容 listener 尚未全部清理；详见
[通信清单](INTERNAL_GRPC_MANAGED.md#remaining-communication-inventory)。
存储 record proto 与 RPC service schema 分离；迁移不修改持久化记录、共识或 Gremlin 语义。

- **已交付基础切片**：remote snapshot 的 typed gRPC OpenSnapshot → Page → Close（#226）。
  observed data-local query fragments 的 Open → Fragment → Close 由 [#227](https://github.com/ashione/VelaGraph/issues/227)
  跟踪，复用真实 query sessions/admission，显式选择 gRPC coordinator backend。
  详见 [typed query transport](INTERNAL_GRPC_QUERIES.md)；observed hierarchy jobs 的 shard
  OpenHierarchy/Fragment/Close 已增加 typed gRPC 路径，但 coordinator job 入口仍是旧协议。不能将这些已交付切片当作旧入口和兼容路径已全部删除。
  [#229](https://github.com/ashione/VelaGraph/issues/229) 增加独立 fixed/renewable query
  session gRPC 路径，checked terminal-prefix DTO 保留完整证明及租约/序号语义；
  managed control 与对外 gateway 协议仍不在该切片内。
  用版本化 Protobuf 和 gRPC adapter 复用原执行器；按已验证 peer 身份复用 channel，
  保留固定视图、顺序、错误与累计预算。真实多进程差分测试后清理替代的 JSON 路径，
  不长期维护两套业务实现，不把 JSON 装进 bytes 就当完成协议迁移。
- **managed control/write 切片**：[#235](https://github.com/ashione/VelaGraph/issues/235)
  将现有 18 项 coordinator/member 操作映射到 typed gRPC，复用原状态机、身份、
  幂等与历史证明；详见 [内部 managed RPC](INTERNAL_GRPC_MANAGED.md)。
  [#242](https://github.com/ashione/VelaGraph/issues/242) 将服务入口的 managed 与 graph
  路由固定为 gRPC，移除临时选择开关；[#244](https://github.com/ashione/VelaGraph/issues/244)
  将 Service 生命周期与旧 TCP listener 解耦，graph-service 只绑定配置的公开端口，
  保留会话过期和 shutdown/drop 清理。旧 library/control/coordinator 入口仍须逐项
  迁移或删除，不能将服务入口切换当作统一通信已完成。
- **内部边界**：Raft、managed control/member 和 shard execution 分服务迁移。
  分别验证混合版本或明确拒绝、升级窗口和原恢复契约；不捆绑存储格式或 purge 变更。
- **外部边界**：独立公开 Graph/Write/Query 服务，gRPC 与 HTTP 使用相同 handler、
  类型和错误语义，支持真实服务化写入与查询。不得向公开入口泄露内部 prepare/commit、
  Raft 或 restore 管理操作。HTTP 是必要入口，不是另一个查询/事务引擎。
- **必须共同验收**：原请求 ID/命令幂等与丢 ACK 状态恢复；提交与可读状态区分；
  固定 V 的分页与游标绑定；查询 Start/Get/Renew/Fetch/Cancel；
  剩余 deadline 传递及取消清理；容量拒绝、bounded queue/bytes/in-flight。
  gRPC 流控不能替代应用层背压，RPC 超时不证明写入回滚。
- **查询语义不拔高**：自动取得最新 certified consistent V 与最小 V 约束须保持原
  一致性边界，不擅称线性一致；历史 V/T 仅开放已支持入口，observed 不冒充 managed。
  恢复、装载和目标视图校验未完成不得 READY/查询，不能以进程存活替代可服务状态。
- **版本与验证**：外部稳定 schema 与内部协议分离，保留被删除字段号/名，
  明确 breaking version；不依赖非规范 Protobuf 编码改变持久化身份/哈希。
  Rust 实现先验证 Go 生成客户端互通，HTTP 验证同一数据类型、写/查及失败结果。
  同环境记录冷/热连接、p50/p95/p99、CPU、内存和网络字节，不预设 gRPC 必然更快。
- 初始仅 loopback/受信环境；最小认证接口、独立 listener 和 readiness 随服务设计，
  公开部署/权限变更另行处理。完整可观测性/UI 仍按低优先级 #122，不阻塞基础协议统一。

参考 [etcd 服务分层](https://etcd.io/docs/v3.6/learning/api/)、
[gRPC deadline](https://grpc.io/docs/guides/deadlines/)、
[流控边界](https://grpc.io/docs/guides/flow-control/) 和
[Protobuf 演进](https://protobuf.dev/programming-guides/proto3/)。

## 已吸收的架构方向：通用执行、路由与可持续一致视图

2026-11-13 对照 NebulaGraph Community 3.8 后，采用其职责分层、图语义批量
RPC/算子下推和元数据驱动路由的原则，不照搬固定分片数、较弱跨图隔离或业务特化。
具体决策、当前边界、验收与官方来源见 [架构演进计划](ARCHITECTURE_EVOLUTION.md)。

- #218/#221：Query、Graph Storage、Meta/Control 边界明确；D1/Raft 保持图无关，
  HTTP/gRPC 共用 handler，统一通信不等于已完成分布式优化。
- #110/#111：通用批量邻接展开、过滤/投影/局部聚合下推、跨片 frontier 合并和背压；
  独立 oracle 验证同 V、bag/顺序/完整结果，高出度与偏斜负载不使用 CoreHR 特化。
- #221/#222：入口负载均衡与分片所有权分开；版本化路由、旧 owner fencing、
  baseline/delta 交接及失败回滚；在线迁移仍须恢复与成员变更前置。
- #109/#110/#223：研究参与者范围协调及 indexed MVCC/历史回收，保持 latest
  入场固定 V、精确原 ID 重试与历史承诺；不是放宽限额或授权直接删除历史。
- #191/#193/#196：恢复完整认证 cut 和所有必要数据后才 READY；checkpoint 运输、
  全局恢复、对象存储权威分别验收，保留 #124 独立前置，不绕过已有阻塞。

优先级保持：RPC 统一先行；通用查询与持续写入/恢复为核心，路由设计并行、弹性启用
依赖安全门槛；对象存储权威、Portal/完整观测后续。复用现有 issue 和 #224 发布阶段，
不新增重复总目标，不因本规划合入关闭实现 issue，也不将尚未合并的 RPC PR 列为完成。

## 后续产品化：轻量交互式 graph portal

[#220](https://github.com/ashione/VelaGraph/issues/220) 在 #218 的稳定 HTTP 查询、
任务/分页/错误接口之后实现，当前仅规划。基础分布式读写、一致性与恢复是关键路径；
高优先级 gRPC/HTTP 并行推进，portal 不抢占上述工作，也不重复 #122 运维观测平台。

首版是只读查询工具：输入标准受支持 Gremlin，运行/取消，在图与表之间切换，
点击点/边查看属性和 qualified ID。两种视图使用同一结果，保留多重边、路径、重数、
类型及 missing/Null；展开邻居须显式请求并遵循同一固定视图，不能混合多个 V。
显示状态、错误、耗时、V/可用 T、一致性和分页/展示限制；Unverified 不显示为 certified，
恢复未就绪不显示为空图。渲染/分页有界、取消和过期可见，不把截断或失败当完整成功。

先随服务自托管/loopback 验证真实浏览器到 HTTP 的成功及失败流程，再更新快速开始。
首版不做写编辑器、集群管理、可视化建模或独立监控平台；不发布独立网站，不扩大
CORS/权限或上传遥测。此 UI 只读范围不取消 #218 对外服务必须支持写入的要求。

## 发布里程碑

按 [发布验收计划](RELEASE_PLAN.md) 从 **v0.0.1** 起发布实验版本：
先固定拓扑 gRPC/HTTP 服务，再持续一致读写与恢复、弹性路由并发，最后观测与 Portal。
验收通过后才打 tag/release，不把规划或单个 PR 合并当作发布完成。
总跟踪 [#224](https://github.com/ashione/VelaGraph/issues/224)；新增缺口分别为
[#221](https://github.com/ashione/VelaGraph/issues/221) 入口负载均衡/会话归属/连接复用、
[#222](https://github.com/ashione/VelaGraph/issues/222) 动态发现/合格副本选择/分片迁移、
[#223](https://github.com/ashione/VelaGraph/issues/223) 通用写并发。
双时间、对象存储权威按独立切片验收，本体仍待基础 ready 后考虑；不承诺首版生产就绪。

## 1. 当前基线

截至 2026-11-10，本次核对基于 main `a0b7147`（已合并 #142/#144/#146）。
[当前能力索引](CURRENT_STATUS.md)列出入口、源码/测试和开放工作；下列状态不是对未合并 PR 的验收。
当前代码已超出最初 PR #1–#3 的单机 SQL 原型：

- **内核**：属性图存储与标准 Gremlin 有界子集，单分片快照读、写事务与进程崩溃恢复测试；旧独立 SQL 表引擎已移除，见[迁移说明](SQL_REMOVAL.md)。
- **分布式基础**：持久化 Raft 节点、版本化 D1 条件/幂等写、静态 catalog 与固定远程路由。自动路由收敛、成员变更和在线迁移尚未完成。
- **实验查询**：显式 ID 跨分片 in/out、多跳、去重、过滤、计数与终结 valueMap；全局 V() 属性扫描及有界扫描→out/in→过滤/去重/count/valueMap 组合；有界原生层级 repeat；增量层级计数、实际顶点结果分页和所选属性分页。
- **验收**：CoreHR 20 万人、5000 部门，人员/部门上下级和一人多部门，固定生成器与独立关系/属性 oracle，PR/main 全量 CI。此场景运行在同一云端机器的独立进程，每数据分片一个副本，不是多机吞吐、全局快照或高可用证明。

**当前已接通**通用全局属性筛选 → 有界跨片 out/in/both → 过滤、limit、去重及终结 count/values/valueMap，具体范围见[组合契约](GLOBAL_VERTEX_TRAVERSAL.md)。扫描与遍历使用独立累计预算、同一固定会话和原截止时间，组合结果不默认截取 100 条。全局筛选后的单个有界 repeat（总深度≤32）现已接通；更广的组合（如全局起点 path、无显式 limit 的无跳转 values）仍未开放；大结果分页仍只适用于文档列出的层级 job 形状，不是任意查询分页。

**Observed graph/v2 路径**的跨片边写入仍分阶段执行、不是原子事务；查询返回 **Unverified** 和实际各分片快照向量，**不是全局原子快照**。它与下面的 managed 固定 cohort 入口不同，不自动获得 managed 事务/版本保证。两者均无生产故障切换或任意低层写入下的完整邻接认证。参见 [README](../README.md)、[查询规范](QUERY_SPEC.md)及各 API 的单独契约。

[显式一致性协商](QUERY_CONSISTENCY_NEGOTIATION.md)的旧 `GlobalCommittedSnapshot` 要求仍对同步查询和 job 返回 Unsupported，不静默降级。**另一个已实现的 restricted managed 入口**以固定、空 baseline cohort 认证同一 V，不等于为任意 catalog/旧 observed API 开放全局快照。

协调决策账本、参与者 prepare/commit/abort、连续 applied watermark、完整 cohort/empty-baseline 认证、外部请求 ID/原 Plan 恢复和远程 managed gateway 已接通。支持点/边创建、属性更新、边删除、条件属性补丁及[原子边替换](MANAGED_EDGE_REPLACEMENT.md)：一次事务删除旧关系并创建新 qualified ID，保留旧 V 与旧 ID tombstone。partial apply 不得开放较新的 certified V。普通弱读仍可能看到逐片发布；`resolved_prefix` 本身不是可读版本。节点固定在同机独立进程的现有验证不等于多机 HA。见[受管事务](MANAGED_GRAPH_TRANSACTIONS.md)、[gateway](MANAGED_GRAPH_GATEWAY.md)及[固定版本查询](MANAGED_FIXED_TRAVERSALS.md)。

Managed Traverse/TraversePage 已支持 routing-v3 的 qualified `id()`、元素/标量 `dedup()` 和完整结果分页，支持单个有界 out/in `repeat` 与 post-body emit（总深度≤32），支持终结 selected `valueMap`、严格标量 `is`、有界 `range` 及 `simplePath`/`cyclicPath` 实际遍历对象路径过滤，仍不支持 data-local hierarchy jobs。当前会先 materialize 小型 cohort snapshot，不能把 observed 全量 CoreHR 通过等同于 managed 全规模动态一致性验收。

S1 archive reads、S2 coordinator checkpoint、S3 member checkpoint、S4 retained gateway control 已合并。当前 bound durable metadata 为 coordinator v16/member v17，内部 RPC v6；冷启动 replay、完整 SnapshotV1 和部分证明仍依赖完整历史。两个 profile 都保留 16 MiB reserved history 及独立 intent/D1/query 限制；checkpoint 不回收配额，也不是远程备份。见[容量契约](COHORT_CAPACITY_PROFILE.md)。

### 仍在推进的依赖与验收

- [#108](https://github.com/ashione/VelaGraph/issues/108) / [#109](https://github.com/ashione/VelaGraph/issues/109)：已交付上述读取/active/control 切片，未完成有界冷恢复、完整流式快照、闭合 checkpoint/credit reuse 或生命周期扩展。不能把已合并 #113 再列为待实现，也不能因此关闭父任务。
- [#124](https://github.com/ashione/VelaGraph/issues/124) / [#118](https://github.com/ashione/VelaGraph/issues/118)：durable purge/install 安全前置与文件流式 snapshot 仍未交付；本次文档刷新不发布其实现或声称恢复安全问题已解决。
- [#110](https://github.com/ashione/VelaGraph/issues/110) → [#111](https://github.com/ashione/VelaGraph/issues/111)：先给 certified V 接通可验证的 data-local 查询/资源契约，再验收全规模 managed CoreHR。已合并的 id/dedup 是语义补齐，不是容量扩大。
- [#127](https://github.com/ashione/VelaGraph/issues/127)：gateway phase timing 已由 #133 合并；[#143](https://github.com/ashione/VelaGraph/pull/143) 的生命周期归因也已合并；这些诊断不等于完整产品观测能力。#146 已合并 active durable HistoryStatus 的单次 verified cut，关闭 #145；仍逐请求校验完整 raw/terminal，不缓存、不放宽容量，也不证明整体 Submit/lifecycle 倍速。见[限定与验证](HISTORY_STATUS_VALIDATION.md)。
- [#128](https://github.com/ashione/VelaGraph/issues/128)：failure-only paged-snapshot diagnostics 已由 #131 合并；原偶发失败根因仍未确认，单次绿灯不等于根因修复。
- [#122](https://github.com/ashione/VelaGraph/issues/122)：产品观测性、调优和运维视图仍为低优先级计划，不把诊断计时当成完整产品监控。

### 已确认与待定

| 项目 | 当前结论 | 状态 |
|---|---|---|
| 存储基础 | 当前每片本地 RocksDB/Raft；后续评估对象存储权威、可丢弃本地缓存 | 当前实现明确；后续设计见 #191，尚未迁移 |
| 两条线 | 内核线 + 分布式线，以窄存储接口解耦 | 已确认 |
| 图模型 | 属性图（点/边/标签/属性），RDF 不作为当前目标 | 已实现基础模型 |
| 查询语言 | 标准 Gremlin（Apache TinkerPop 官方语法）子集优先；不自造方言；旧 SQL 原型已移除，未来分析接口须单独论证 | 已确认 |
| 一致性 | v1/K3 单分片事务；observed v2 分阶段写/独立快照；restricted managed 固定 cohort 事务与 certified V 已实现，general Global 仍拒绝 | 入口边界明确，完整 D4 未完成 |
| 首批场景 | CoreHR：20 万人、5000 部门，上下级与多部门关系，支持逐步完善的通用查询 | 已选定并进入全量 CI |

## 对象存储与索引演进（后续计划，非已实现能力）

统一跟踪 [#191](https://github.com/ashione/VelaGraph/issues/191)。当前本地
RocksDB/Raft 仍是持久化权威，不能清空后当缓存重建。持续推进通用功能与性能，
本计划不以 CoreHR 特化或更换查询引擎换取单一场景指标。

1. **先冻结 durability 契约，再做单分片 adapter 实验**：比较远端 WAL 确认
   与独立复制日志层。若所有本地盘均可丢弃，写 ACK 必须等待远端 WAL 和必要的
   跨片提交决定持久化；量化延迟、费用、可用性及 RPO/RTO。只在隔离测试夹具
   验证丢弃缓存后的完整恢复，不直接迁移或删除现有权威文件。
2. **快照 + 不可变索引 + 增量日志**：索引 N 与有界 delta 必须精确构成固定 V，
   包括删除、tombstone 和 bag 语义。tail 缺失不得退回旧结果。PR #179 仅是
   索引账本设计基础，不代表生产图索引完成；与 #110 的容量工作分别验收。
3. **一致视图与写后读**：复用已合并 PR #188（#187）的 `OpenLocalQueryAfter` 显式版本下界，不重复实现；
   自动 write bookmark、客户端因果会话及 job/profile 支持仍待补齐。
   latest 在入场时固定安全 V，不随执行刷新；lower-bound 不等于线性化 latest。
   单对象 CAS 不能替代跨片提交协议、完整 cohort 认证与 applied 水位。
4. **恢复与升级设计门槛**：先明确 manifest generation/CAS、writer fencing、
   持久快照保活、GC roots 与中断上传、格式兼容和滚动升级。复用 #109/#118，
   #124 既有 purge/install 前置仍独立未完成；本计划不实现或绕过该前置。
5. **同等场景 benchmark**：沿用 #111 的 20 万人/5000 部门独立 oracle，
   对齐结果、查询语义、写 ACK durability、冷暖缓存、硬件和数据规模；分别报告
   单 cell 与跨分片、p50/p95/p99、吞吐、RSS、远端请求费用及恢复，不把现有
   observed 全量测试当 managed 全规模一致性证明。
6. **观测与平台仍低优先级**：复用 #122，纳入缓存/远端 I/O、索引 lag、
   commit/applied 水位及 lease/恢复诊断；必要验证信号先做，完整平台留到产品化。

参考 [SlateDB write durability](https://slatedb.io/docs/design/writes/)、
[persistent checkpoints](https://slatedb.io/docs/design/checkpoints/) 与
[HydraDB architecture](https://github.com/hydra-db/hydradb/blob/af6344037bffb2a354bf32c2b312ba4921a1583f/architecture.md)。
这些是设计参考，不表示已选定依赖、复用其代码或完成跨 cell 能力验证。

## 本体查询与语义层（基础就绪后评估，当前不启动实现）

后续规划由 [#215](https://github.com/ashione/VelaGraph/issues/215) 跟踪。
**先完成基础功能，再考虑本项**：不抢占当前通用查询、分布式一致性、容量与恢复主线，
不把图中可表示概念/关系等同于已有本体推理，也不承诺完整 OWL/SPARQL 兼容。

- **启动门槛**：#110/#111 的通用查询及目标规模动态一致性、#109 等相关历史/恢复
  基础须有可复现验收证据；明确核心读写、固定视图、恢复后查询门禁与资源预算
  已适用于拟支持语义。不是合并一个 PR 就视为基础 ready。
- **分步范围**：先评估通用概念/实例、类型/子类型和关系元数据；再考虑有限、
  可解释的类型/关系继承、逆关系与显式规则；RDF 导入导出、SPARQL、RDFS/OWL
  受限 profile 等标准互操作单独评审。约束验证与推理分开，不改变现有
  literal hasLabel 的含义，不硬编码 CoreHR，也不另造专用查询引擎。
- **一致性与成本**：本体、规则及数据绑定一致版本；时态推理依赖 #196 的同 V/T
  语义，禁止混合时期。实施时验证多继承、环、删除撤销、跨片闭包与推导来源，
  以独立非 HR oracle 和真实负载评估查询改写或增量物化，不预先选定方案。
- **保持边界**：本项未实现、无交付时间承诺；不新增运行时依赖或格式变更，
  不重复 #122 观测平台、#191 对象存储和 #196 双时间工作。基础就绪后先复核
  成熟系统与标准，再拆实现任务。

## 2. 两条线的交付边界

| 维度 | 内核线（Kernel） | 分布式线（Distributed） |
|---|---|---|
| 目标 | 单机图存储、查询、事务、嵌入接口 | 多节点分片、复制、路由、跨分片执行 |
| 首个交付 | 属性图 CRUD + 1–3 跳遍历 + 持久化 | 3 节点 Raft KV + 元数据/路由原型 |
| 不负责 | 副本共识、跨节点事务、集群运维 | 图查询语义、存储编码、单机执行器 |
| 接口 | 图 API / 标准 Gremlin 子集 / C ABI | 只依赖存储节点接口，不感知图内部 |
| 验收 | 单机数据正确、崩溃恢复、基准达标 | 副本一致、故障切换、无数据丢失 |

**存储接口边界**：当前 D1 已提供不透明字节 `get`/`scan`/条件 `batch_write`、稳定读视图及 export/import。完整 `begin` 事务接口、在线 `split/transfer` 仍是目标，不是已冻结且可用的能力；以 [STORAGE_NODE_INTERFACE.md](STORAGE_NODE_INTERFACE.md) 为准。查询层负责图语义，复制层不解释图记录。

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
- **当前状态**：`src/storage_node.rs` 已有单 shard 本地适配：不透明字节 `get`/`scan`/`batch_write`、稳定读视图、epoch 围栏、请求幂等和 export/import；接口测试见 `tests/storage_node.rs`。`src/graph_records.rs` 已提供基于同一 `graph/v1` 布局的图记录适配器（点/边/邻接的 create/update/delete、标签过滤、级联删除和 reopen），并基于同一 read view 的 sequence 条件批写阻止多 store 的陈旧验证提交；冲突返回 typed `Conflict`，不自动重试。旧 StorageNode 实现对条件写默认返回 `Unsupported`，不降级；D2 已有独立版本化 D1V1 复制命令承载条件/幂等语义，并由独立 `replicated-graph` crate 实现同步 StorageNode trait 与 GraphRecordStore 的进程内 Raft 接线（leader 读屏障、有界稳定快照、prepared 请求原样重试）；见 `REPLICATED_GRAPH.md`。`GraphEngine` 接线、`begin` 事务接口与拆分/迁移仍未实现；静态远程路由见 `REMOTE_ROUTING.md`。

### D2 复制与共识

- **交付物**：Raft 组、日志复制、快照、leader/follower 切换；副本放置策略。
- **完成定义**：3 节点集群可写入；杀掉 leader 后 30 秒内恢复；重复/乱序消息测试通过。
- **依赖**：D1；选型已定 openraft `=0.9.25`，见 [`D2_CONSENSUS.md`](D2_CONSENSUS.md)；三节点原型、多进程 TCP 验证已实现；节点使用 RocksDB `raft.db` 持久化日志与 `state.db` 不透明字节批次状态机，重启可恢复；`Suite::test_all` 已通过；D1 本地字节 `StorageNode` 适配已实现，图引擎接线仍待做。

- **应用语义切片**：版本化 `D1V1` 命令已复制 shard/epoch、request_id、expected sequence 与原子 data/sequence/dedup/applied marker；snapshot/restart 保留重试身份。详见 [`D2_D1_CONTRACT.md`](D2_D1_CONTRACT.md)。已有进程内 `StorageNode`/GraphRecordStore 接线，但未连接 GraphEngine；远程客户端和静态路由见 `REMOTE_ROUTING.md`，固定进程内路由见 D3a；无淘汰去重预算耗尽后明确拒绝新写，不能视为生产容量方案。

### D3 元数据、路由与故障切换

- **交付物**：catalog、分片到节点映射、成员变更、客户端路由缓存、故障检测。
- **完成定义**：节点下线后路由自动收敛；新节点加入可迁移分片；元数据有版本和回滚。
- **依赖**：D2 的副本状态机。

- **D3a 静态 catalog 切片**：独立预配置 metadata D1 group 上的持久化、版本化 partition→shard→group/epoch/replicas catalog；leader barrier 读取、条件注册、精确重试与不可变绑定。进程内 `(group,node)` 隔离；无 quorum 失败关闭。详见 [`D3_STATIC_CATALOG.md`](D3_STATIC_CATALOG.md)。提供固定候选进程内和静态远程路由，不支持自动远程路由收敛、成员变更或迁移，不代表 D3 完成；catalog epoch 不构成跨独立 group 的 fence。

### D4 分布式事务

- **交付物**：跨分片 2PC 或快照事务；事务协调者；超时/恢复/悬挂事务处理。
- **完成定义**：跨分片写入要么全提交要么全回滚；协调者崩溃可恢复；隔离级别文档化。
- **当前状态**：固定 empty-baseline cohort 的受管事务、原 ID/Plan 恢复和同版本会话已实现；任意 baseline/writer、动态路由、无限保留和全规模动态一致性尚未验收。弱 observed API 不继承此保证。
- **依赖**：D1、D2、D3；内核 K3 的单分片事务接口。

### D5 分布式查询与遍历

- **交付物**：分区感知的计划；跨分片 2 跳遍历；结果合并与去重；超时/部分失败语义。
- **完成定义**：跨分片 2 跳结果正确；单分片查询不退化为全集群扫描；有慢查询和失败指标。
- **当前状态**：D5 的有界功能切片已实现并持续扩展，见 [分布式查询](DISTRIBUTED_QUERY.md)、[全局扫描](SERVER_GLOBAL_SCAN.md)、[层级计数](HIERARCHY_COUNT_JOBS.md)、[结果分页](HIERARCHY_RESULT_PAGES.md)、[属性分页](HIERARCHY_PROPERTY_PAGES.md)。这不等于通用查询规划、完整邻接或完整 D5 验收。
- **依赖**：静态 D3 路由和 K2 算子可先支撑明确标注 Unverified 的实验查询；D4 不是这些功能切片的前置阻塞项。任何跨片原子性或全局一致性承诺，仍必须等待相应事务/快照协议与独立验收，不能从功能结果正确推导。

### D6 运维与多租户

- **交付物**：部署编排、备份恢复、监控指标、限流/配额、租户隔离。
- **完成定义**：可重复的集群部署；备份可恢复；核心 SLI 有仪表盘和告警。
- **依赖**：D2–D5；在首个生产试点前完成最小集。

### D6a 远程 checkpoint 与灾难恢复（未来设计，尚未实现）

先明确恢复契约，实施仍排在当前通用查询功能之后；本条目不引入云存储适配器，也不表示已有远程备份或恢复工具。

- **三种能力分开验收**：Raft 副本用于组内复制，不能替代历史备份，也不能防止所有副本所在故障域丢失或错误写入被复制；本地状态机快照/未来 RocksDB checkpoint 是备份构件，和源数据同盘仍会一同丢失；远程备份须把一个可验证恢复集保存在独立故障域，并能脱离原机器恢复。当前 Raft 快照不等于已支持 RocksDB checkpoint 或对象存储备份。
- **首版先做协调停写恢复点**：关闭所有数据写入及 catalog/放置变更入口，排空或查明在途和未知结果操作；固定 catalog 与全部数据分片集合，记录各组已提交/已应用位置、应用 sequence、epoch 与格式版本，再捕获匹配的快照。不能分别复制运行中的 `raft.db`/`state.db` 文件并假定一致；后续不停写备份需要新的协调协议，不能暗中降级为逐片随意取快照。
- **恢复集与发布协议**：同时覆盖 metadata/catalog、全部数据分片及必要的成员/版本/应用重试身份。每次备份使用不可变 generation；分片对象写入并核对大小/checksum 后，才原子发布唯一完整 manifest，列出所有对象、备份/集群身份、catalog revision、group/shard/epoch、applied log index/term、membership、格式版本和 D1 sequence/dedup 恢复位置。上传中断、缺片、混合 generation、校验失败或版本不兼容不得成为“可恢复”成功；checksum 只验证字节完整性，不证明跨片语义一致。
- **独立故障域与保留**：远端位置、访问控制和备份保留策略须独立于被备份机器；源盘丢失或本地清理不能连带删除唯一恢复副本。具体存储服务、权限、费用、加密和删除策略在实施前另行确认；本路线图不授权配置账户或上传数据。
- **非原子写限制**：graph/v2 分阶段跨片边写入可能已留下源/反向索引不一致，停写不会自动修复历史半写，也不会产生 D4 事务或全局原子快照。无法确认的在途操作、缺失关联或不一致状态必须显式记录并阻止宣称“关系完整恢复”；诊断快照不等于通过验收的一致恢复点。恢复后仍保留 Unverified，除非另有完整性协议和证据。
- **恢复演练是完成条件**：在隔离的新集群、不可访问原盘的条件下，从指定 manifest 恢复 catalog 和全部分片，按受控流程处理集群身份、成员配置、epoch/旧会话隔离，防止旧集群继续接收写入或旧令牌误用；不能复制旧 vote/membership 后直接改节点 ID。校验数据、写入、查询和精确重试身份；CI 跑小规模“备份→隔离原目录→恢复→核验”，定期跑全量 CoreHR 关系/属性 oracle 并记录资源峰值。覆盖上传/发布/恢复中断、对象损坏、缺片和恢复重试。不得仅凭“上传成功”“下载成功”或节点启动认定可恢复。
- **RPO/RTO 单独度量**：RPO 以事故时刻相对最近已完整发布且验证可恢复的数据位置衡量可接受数据丢失；无后续日志归档/回放协议就不承诺任意时间点或零丢失恢复；未来归档须匹配 Raft group/index/term 与快照版本，不能将 RocksDB WAL 当作可直接替换的 Raft 日志。RTO 从启动灾难恢复到完整校验并可安全服务计算，包含下载、重建、校验和切换。先给出目标与备份频率，再用明确数据量/带宽/故障场景演练记录实际值；现在不承诺数值。

设计依赖与现有边界见 [D2_CONSENSUS.md](D2_CONSENSUS.md)、[D2_D1_CONTRACT.md](D2_D1_CONTRACT.md)、[D3_STATIC_CATALOG.md](D3_STATIC_CATALOG.md) 和 [EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md)。

## 5. 阶段与退出条件

| 阶段 | 当前状态 | 下一项退出条件 |
|---|---|---|
| P0 方向与接口 | 属性图、标准 Gremlin、D1/K3 边界和 CoreHR 场景已明确，不再从 SQL 原型重新起步 | 新能力继续按版本化契约与独立审查扩展 |
| P1 内核 | K1/K2、K3 单分片能力已有实现；不等于全部规模/ABI目标完成 | 保持现有正确性回归，并按实际硬件验证规模目标 |
| P2 分布式基础 | Raft 节点、静态 catalog/路由与远程图路径已有实现；自动切换、迁移仍有缺口 | 按 D2/D3 单独验证失效、收敛和恢复，不以 CoreHR 单副本场景代替 |
| P3 通用查询功能（当前主线） | 已有显式 ID 跨片关系、层级作业及有界全局筛选后的 out/in 组合；继续扩展受验收约束的通用组合 | 通用算子组合、行/属性/计数结果与独立 oracle 一致；边界拒绝、快照与全量 CI 通过 |
| P3 一致性与恢复（进行中） | Restricted managed 固定 cohort 事务、certified V、原 ID/Plan 重试和恢复切片已实现；observed 路径仍非原子，完整 D4 未完成 | 保留已有故障门禁，完成有界生命周期/恢复与 fixed-V 查询容量，再以全规模 managed CoreHR 独立验收 |
| P4 生产硬化（后续） | 性能、容错、在线迁移、备份、多机部署和安全尚不能视为完成 | 明确硬件/负载的指标、恢复演练、权限/传输保护及试点验收 |

**推进原则**：优先完成通用图功能，以 CoreHR 检查实现而不是在产品代码中写死人员/部门逻辑。性能改进必须有同配置可复现对比；不以扩大预算或移除拒绝测试代替优化。当前没有经实测确认的整阶段完工日期，原估算周数不作为交付承诺。


## 6. 长期验收目标（不是已达成结果）

| 线 | 指标 | 目标（首版） | 验证方式 |
|---|---|---|---|
| 内核 | 数据规模 | 1,000 万点、5,000 万边可导入并可查询 | 固定生成器 + 可复现基准 |
| 内核 | 遍历延迟 | 1–3 跳 p99 小于 50 ms（硬件待定） | 单机基准，记录查询形状和缓存状态 |
| 内核 | 恢复 | 崩溃后无半写，重启可恢复 | 故障注入 + WAL 回放测试 |
| 分布式 | 副本 | 3 节点、3 副本，leader 故障 30 秒内恢复 | 集群测试 + 网络分区注入 |
| 分布式 | 一致性 | 单键写入线性一致；跨分片事务不丢不重 | 并发写 + 故障恢复校验 |
| 分布式 | 跨分片查询 | 2 跳遍历结果正确，支持超时/部分失败 | 与单机参考结果对拍 |

上述 1000 万点/5000 万边、p99、三副本恢复和跨片一致性指标仍需各自的硬件、负载和故障证据。当前 20 万人/5000 部门的全量 CI 不证明这些目标已达成。

## 7. 风险与决策门

| 风险 | 预警信号 | 动作 | Owner |
|---|---|---|---|
| 图分区导致跨分片遍历放大 | 2 跳查询命中多数分片；p99 随集群规模上升 | 保留明确的跳数、工作与内存上限；优化批量访问/分区亲和；超限显式失败，不静默降级 | 分布式负责人（待指定） |
| Raft/共识实现复杂 | 端到端图路径缺少失效/恢复证据 | 继续采用已选 openraft，单独验收复制层与图路径的故障语义 | 分布式负责人（待指定） |
| 事务范围失控 | 跨分片事务阻塞主路径；隔离级别反复变更 | 分别遵守 K3、observed v2 和 restricted managed fixed-cohort 契约；扩大事务/路由/恢复范围需单独设计验收 | 内核负责人（待指定） |
| 两条线接口漂移 | 分布式层开始直接依赖图内部结构 | 保持版本化字节接口；语义变更先更新契约并独立评审 | 产品/架构负责人 |
| 范围过大 | 同时做 SQL 兼容、图、分布式、云托管 | 首版只做属性图 + 标准 Gremlin 子集 + 单区域；其余进 backlog | 产品负责人 |

**决策门**：图模型、查询语言、CoreHR 场景及当前一致性边界已有实现依据。超出既有 fixed-cohort 契约的事务、迁移或全局快照仍须先冻结协议和失败语义；现有实验查询的成功不替代这些审批与验证。

## 8. Backlog（不排期）

- **未来分析接口**：只有真实图查询需求证明必要时才评估；不恢复旧独立 SQL 表引擎，也不默认承担其兼容性。
- **图能力**：更广的路径查询、最短路、社区检测与通用聚合。已实现的有限路径/层级 repeat 不能视为任意路径算法支持。
- **运维**：多区域双活、跨云容灾、云托管控制台与计费；首版只做单区域集群。

## 9. 非目标

- 不做完整 SQL 数仓、PostgreSQL 兼容、复杂多表 JOIN 和窗口函数。
- 不做多区域双活、全球强一致和跨云容灾（首版单区域）。
- 不先做 RDF/SPARQL；属性图优先。
- 不先做云托管控制台和计费；先证明单机与 3 节点集群。

## 10. 下一步

1. 在已接通的有界全局筛选 → out/in 组合上，按实际需求逐步扩展仍 Unsupported 的算子与大结果消费方式；先明确语义、预算和分页契约，不把任意组合默认为已支持。
2. 持续以独立随机小图和 20 万人/5000 部门 CoreHR 验证新增组合，覆盖 bag/去重、过滤/limit 位置、空结果、属性输出和失败边界；CoreHR 仅是输入，不能成为执行特化或从结果反推预期。
3. 并行推进 #108/#109 的剩余历史/恢复边界与 #110 的 fixed-V 查询容量，先冻结资源、失败和兼容契约，再以 #111 联合验收。#124/#118 的 purge/install 前置未交付前，不宣称流式恢复或放开生命周期。
4. 以 #127 的已测瓶颈选择独立、可复现的优化，不靠放宽预算取得通过。在线路由/迁移、生产安全、远程备份与多机验证分别建证据，不把同机结果外推。

## 11. 分项实现记录与契约

下列条目描述各次**独立切片**的边界，不应把早期切片“不支持”的描述当作整个当前产品仍不支持。当前支持组合以 [QUERY_SPEC.md](QUERY_SPEC.md) 和具体入口契约为准，阶段总览见上文。

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

### D2 TCP resource safety

Legacy newline-JSON v0 now bounds frame allocation/encoding, concurrent handlers
and outbound exchanges, uses shared total deadlines, and tracks shutdown of
connections. Oversized batches can be reduced through openraft hints. Control
remains unauthenticated and intended only for trusted isolated tests; this is
not production transport security or D3 completion. See `D2_TCP_LIMITS.md`.

### Functional remote graph prototype

A separate [development-only TCP graph adapter](REMOTE_GRAPH.md) connects normal
single-shard graph CRUD and client-side Gremlin to independently running Raft
nodes. It uses bounded atomic snapshot copies and exact conditional D1 writes,
not a server-side Gremlin protocol or remote D3 resolver. Crash/failover and
network-partition validation of this new end-to-end graph path are deferred;
this does not complete D3/D4/D5 or production transport security.

### Static remote catalog and data groups

[REMOTE_ROUTING.md](REMOTE_ROUTING.md) adds a fresh remote catalog snapshot and
fixed `(group,node)` endpoint registry to the existing resolver. Normal graph
CRUD/Gremlin can target independent data groups; pinned sessions and exact CAS
retries reuse the existing contract. Nine-process tests exercise metadata plus
two data groups. No migration, cross-partition edge traversal, automatic failover claim,
TLS/authentication or production D3 completion is implied.

### D5a bounded global scans (2026-11-07)

The additive [global Gremlin scan](GLOBAL_SCAN.md) API merges V/E identities
across static catalog partitions with ordered has/hasLabel filters, values/label
projections, global limit/count, stable `(ID, partition)`
order, one snapshot per shard and one absolute deadline. It has no cross-edge
traversal, global snapshot isolation or transactions. Full-copy snapshot and
logical query budgets have explicit distinct bounds/exclusions. Actual metadata
and two independent data processes exercise the functional path. D5 remains open.

## Isolated v2 append-only experiment

[EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md) documents a separate
versioned namespace and explicitly non-atomic source/reverse append protocol.
It does not change this document's existing v1/K3/Gremlin contract or complete
D4/D5. Its out/in observations cannot certify missing-index completeness.


D5 experimental functional slice: isolated v2 observed Gremlin now supports
global/explicit V starts with qualified out/in/both, label/property filtering, vertex/edge paths,
limits and observed counts. An additive result-v2 entrypoint adds values/label
projections with qualified provenance and preserved bag multiplicity, keeping
the original Eq API intact. Results always expose Unverified completeness and a
per-shard snapshot vector. Full adjacency visibility, global consistency, repair
and complete D5 remain deferred. See EXPERIMENTAL_CROSS_EDGES.md.

Experimental graph/v2 now exposes additive observed_gremlin_v3 E()/E(ids)
filtering and projection, qualified EdgeRef results and paths, bounded canonical
paging and source/reverse/endpoint checks. All results remain Unverified;
repair, cross-shard transactions and completeness certification remain future
work. Legacy observed APIs are unchanged. The v3 functional subset now also supports
edge adjacency and endpoint transitions with qualified paths, shared budgets,
and explicitly unverified observations; this is not complete D5.

### Experimental v2 edge properties

The additive properties-only coordinator validates both observed indexes and
endpoints, then prepares one source-shard conditional Put. Identity/label/endpoints
remain fixed. Immutable exact retry preserves historical dedup after newer writes;
Null/removal are explicit. This does not implement deletion, repair, distributed
transactions or production transport security. V1 contracts remain unchanged.

### Reviewed experimental edge deletion extension

The opt-in v2 edge mutation API additionally supports permanent tombstone deletion.
It is an explicit R2/R4/R12 scope exception, not a change to v1 single-batch,
failed-create, cascade, K3 or query contracts. Cross-shard source tombstone/out
removal and target in removal are staged, not atomic; same-shard deletion uses
one CAS batch. IDs are never reused. Reads validate visible residual indexes and
charge tombstone candidates before skipping them. Recovery and GC are unsupported.
See [deletion contract and versioned format](EXPERIMENTAL_CROSS_EDGES.md#additive-permanent-edge-deletion)
for upgrade restrictions, bounds and attempt-local retry outcomes.

### Experimental v2 vertex properties

Qualified vertex property patches now use an immutable owner-shard CAS with exact
historical retries. Create APIs remain append-only and v1 stays unchanged.
Vertex deletion explicitly returns Unsupported because observed adjacency and
non-global snapshot vectors cannot prove complete absence or fence remote source
writes. No cascade or unsafe isolated-vertex exception is supplied.
See [vertex mutation contract](EXPERIMENTAL_CROSS_EDGES.md#additive-vertex-property-mutation).

### Bounded observed server coordination experiment

The [bounded multi-hop server coordinator](DISTRIBUTED_QUERY.md) is an additive
partial D5 experiment: immutable startup placement, fixed data-local sessions
and typed lookup/adjacency/filter/projection fragments avoid whole-shard network
downloads. Up to three incoming/outgoing hops share the original sessions, deadline and
cumulative budget, including cycles and shard revisits. Online catalog
acquisition, broader traversal planning, complete adjacency, global snapshot
isolation, retries/failover and production security remain unfinished.
The experimental server coordinator additionally supports bounded vertex
`dedup()` for distinct-person CoreHR counts after incoming/outgoing joins.
Identity is partition-qualified, order/path follow the first occurrence, and
upstream corruption/work cannot be bypassed. This does not add global scans,
scalar deduplication, pagination, native recursive traversal or global snapshots.

Server global `V()` scanning now has a separate bounded iterator over retained
shard sessions, with local filter/limit/count pushdown and deterministic
(partition, ID) order. This does not add global E, adjacency, pagination, native
recursion or global atomic snapshots. See `SERVER_GLOBAL_SCAN.md` for its explicit
new scan budget and untouched explicit-ID traversal bounds.

A first native hierarchy slice now supports one bounded `repeat(single in/out)`
with `times(1..32)` and optional post-emit inside explicit-ID server queries.
It preserves path bags, finite cycles, fixed snapshots and existing budgets.
Arbitrary/unbounded recursion, nested repeats and global-start adjacency remain
deferred; see `SERVER_HIERARCHY.md`.

The next narrowly scoped API, `HIERARCHY_COUNT_JOBS.md`, provides incremental
server-native counts with fixed leased views and weighted frontiers. Its separate
work/memory/lease bounds are explicit; it does not widen synchronous queries or
claim unlimited closures, arbitrary repeat bodies, or ordered row pagination.

Emit plus terminal dedup now reuses a qualified identity entry for both depth
weights and the emitted flag. This reduces duplicate state within the unchanged
250,000-entry ceiling; cycles still propagate weighted bags and all resource
limits remain enforced. Ordered result pagination is now implemented as a separate bounded slice in
[HIERARCHY_RESULT_PAGES.md](HIERARCHY_RESULT_PAGES.md); selected property pages
are specified separately in [HIERARCHY_PROPERTY_PAGES.md](HIERARCHY_PROPERTY_PAGES.md).

The explicit-ID synchronous server path now supports bounded terminal selected
`valueMap` for ordinary person/department joins, independently of long-running
hierarchy jobs. Multi-field rows preserve qualified provenance, missing/Null
semantics and bag order under the original synchronous budget. Global-start
joins, arbitrary aggregations and general Gremlin remain deferred. CoreHR CI
adds batch-person, batch-department, active-members and distinct-colleague map
oracles on both smoke and 200,000-person fixtures.

- Fixed-cohort read foundation: explicit immutable coordinator membership and empty-baseline participant certification, with bounded same-version materialized sessions. See [contract and remaining boundaries](CERTIFIED_COHORT_READS.md). Remote graph GlobalCommittedSnapshot remains Unsupported.

- Managed graph/v2 write/read slice: complete source/reverse transaction plans, property read-dependency guards, edge deletion and certified snapshot reads for deterministic one-partition-per-shard cohorts. Vertex deletion/cascades, general routing manifests and remote Global admission remain pending. Durable request admission is described below. See [contract](MANAGED_GRAPH_TRANSACTIONS.md).

- Durable request admission foundation: atomically retain external-ID/canonical-command/complete-Plan/Begin with resolution reservation and exact restart recovery. The restricted typed managed network gateway and server-side same-version session registry below use this foundation.

### Restricted managed gateway slice

A separate trusted-network managed TCP facade now connects durable external request
admission/recovery to server-owned managed graph handles and expiring certified
fixed-version standard Gremlin read sessions through the existing official parser.
Typed reads remain private execution plumbing. Real loopback tests cover transaction retry, partial
apply, restart, identity and resource failure paths. This is not multi-host Raft
placement, auth/TLS, remote GlobalCommittedSnapshot Gremlin, indexed MVCC, GC or
full D4/D5 production completion. See [gateway contract](MANAGED_GRAPH_GATEWAY.md).

The gateway additionally supports bounded [conditional property patches](MANAGED_GRAPH_GATEWAY.md#conditional-property-patches) for vertices and edges.
Exact stored-value conditions share the prepared snapshot and its original
participant CAS/edge reverse guard; retries recover the admitted Plan rather
than checking fresh state. This does not expand lifetime or snapshot capacity.

### Fixed remote managed-node transport

The managed gateway can now pin coordinator and participant RPC endpoints in
separate OS processes while retaining its original external protocol, durable
request Plan and same-V Gremlin sessions. The local and remote paths share the
complete-cohort acquisition algorithm and compare retained terminal-prefix
evidence. Real child-process tests cover partial publication and persistent
restart recovery. This is a trusted plaintext, static-placement single-replica
validation slice, not multi-machine/HA evidence, auth/TLS, old remote Global
query admission or D4/D5 completion. See [contract](REMOTE_MANAGED_TRANSPORT.md).

### Managed fixed-version traversal and pages

The existing bounded routing-v3 executor now has a private read-provider seam.
Managed Traverse/TraversePage execute against one certified cohort GraphSnapshot
and preserve V across complete-result pages, while the legacy provider retains
its weak observed metadata. Small independent-process fixtures exercise cyclic
reporting, multiple department membership and concurrent mutation counterexamples.
Data-local fragment/hierarchy job integration, larger managed bounds and full-scale
CoreHR CI on this new path remain future work. See
[contract](MANAGED_FIXED_TRAVERSALS.md); no unrestricted Global admission is enabled.

The routing-v3 and managed Traverse/TraversePage paths additionally support
qualified vertex/edge `id()` scalar projection, with checked i64 conversion,
ordered bag semantics and unchanged fixed-version/session budgets. This does not
extend the separate distributed server coordinator or complete managed capacity.

### Exact terminal read evidence

Managed fixed-cohort acquisition now fixes V using bounded metadata and compares
versioned exact terminal-prefix segments, excluding archived external requests and
Plans. Old-V evidence is independent of later writes; latest-V evidence remains
O(V). Durable archive, prepare/advance, retention limits and the older Global
Unsupported boundary are unchanged. See [contract](TERMINAL_PREFIX_EVIDENCE.md).

### Global scan selected properties and identity dedup

无跳 `g.V()` 现在支持过滤/limit 间的顶点身份 `dedup()` 以及终端指定字段
`valueMap(...)`，与已有 scan-to-traversal 组合分开验收。去重保留不同分区的
同号 ID；投影复用扫描的固定分片会话，不重新打开快照。默认100、显式limit
位置、扫描/投影预算及整项失败不变。独立小图 oracle 与 CoreHR 场景覆盖该
组合；仍为 Unverified，不代表全局事务或任意 Gremlin 组合完成。

### Durable request point recovery (A2.1)

原 A2.1 版本为外部请求 ID/原 Plan 增加同库派生索引，恢复与 admission 回读只取单个绑定，
索引、Journal、applied marker 同批持久化；旧 v5 原子迁移到 v8，snapshot
原子重建索引，内部 RPC 升至 v3。仍保留 256 请求、1024 事务、16 MiB Journal
上限以及完整历史，不代表无限写入、GC、MVCC 或大规模动态一致查询完成。
将来解除上限前，全部 cohort 成员（含非参与者）的决定消费容量预留必须有证明。
见[契约](DURABLE_REQUEST_INDEX.md)。

### Global scan prefix identity dedup

通用全局筛选现在允许首个 out/in/repeat 前的顶点 dedup，与扫描已有唯一
(partition,id) 身份等价；后续边遍历和 repeat 的 bag 重数不受影响。独立小图
与 CoreHR 验证前缀/后缀去重、limit 原位置和汇合计数，不扩大预算或一致性声明。

### 持久终态分段索引基础

固定cohort协调者/成员将连续resolved/applied prefix按绝对ordinal保存为32条一段，
与权威Journal、请求索引、数据和Raft水位同批持久化；实际Summary/Terminal/Prefix/Read
读取所需段并核对精确历史。v9/v10坏段不回退，旧版本明确迁移，快照仍完整。
保留256/1024/16MiB等上限，不引入独立成员quota，不宣称无限写入或性能改善；
后续权威ledger拆分与完整cohort容量grant另行验收。见[契约](DURABLE_DECISION_SEGMENTS.md)。

### Incremental managed decision consumption

Managed catch-up transfers bounded raw-event suffixes at an exact fixed cut, then
consumes retained decisions one ordinal at a time on every cohort member. RPC v4
adds checked SHA-256 event cursors; complete history and exact snapshots remain
required, and all existing lifetime caps remain. Observed resolved decisions do
not advance applied visibility. See [contract and upgrade boundaries](INCREMENTAL_DECISION_CONSUMPTION.md).
Lazy authoritative history/identity recovery and complete-cohort pre-admission
capacity grants are still required before those caps can change; no performance experiment is part
of this functional slice.

### Authoritative event ledger with bounded lifetime

[Raw event authority](AUTHORITATIVE_EVENT_LEDGER.md) replaces bound coordinator/member
embedded archives with typed v11/v12 metadata and canonical original-Command
segments. Open/apply/source reads reconstruct authority from those rows; one synced
batch publishes the descriptor, new tail, data and indexes. Legacy migration and
full logical snapshots remain atomic. This removes the live full-archive rewrite,
not complete replay/memory/snapshot work or lifetime caps. Shared codec/replay
replaces duplicated paths; full-cohort pre-admission capacity and bounded retained-state
recovery remain required before safely lifting limits.

### Immutable byte-budget cohort capacity

[The fresh-cohort profile](COHORT_CAPACITY_PROFILE.md) binds one common 16 MiB
reserved-history contract to the complete cohort before admission. Actual durable
member generations and server-owned confirmation replace caller capability claims.
Byte-derived structural guards replace the new profile's legacy request/transaction
counts; original Plans, holes, old V, local D1 limits and complete recovery remain.
Existing cohorts cannot be retagged. Shared policy/codec/validation paths avoid a
second persistence engine. This is a bounded lifecycle extension, not unlimited
writes, lazy retained archives or a performance milestone.

### Archive-backed coordinator recovery reads (S1)

The historical S1 bound coordinator disk v15 uses the shared raw namespace walker
for actual LookupRequest and HistoryPage operations. It retains one exact original ID/Plan
and transaction or one bounded event page, not all IDs. Every successful found,
absent or old-cut response first verifies the complete raw SHA-256 chain against
a private cut published with validated state under the state lock. Cold open
still performs strict semantic replay; the cut is not a self-certified disk root.

The same slice retires the derived bound request index. Old v11/v13 (and earlier
published coordinator formats) retain actual legacy reads until successful apply;
full old-index validation precedes atomic migration. Disk v15 rejects request
rows/manifests, while genuinely unbound disk v8 retains only its empty manifest.
Explicit validated snapshot repair remains available. See the
[ledger contract](AUTHORITATIVE_EVENT_LEDGER.md#archive-backed-coordinator-reads-s1).

In S1, the full Journal remains in writer/member state, cold replay, logical
snapshots and other client/control paths. Reads are O(total history); the 16 MiB
budget,
D1 historical-data limits, complete-cohort grants, RPC/deadlines and both profiles
are unchanged. No performance or removed-limit claim is made. Subsequent slices
must remove remaining decide/status/Prepare dependencies, bound active state and
full-cohort capacity, and close retained-archive checkpoint/snapshot recovery.
Fresh frozen-source correctness, process-crash, 1,025-write release and remote CI
gates remain required; this entry does not claim they have passed for S1.

### Consumed coordinator active checkpoint (S2, merged)

[The merged S2 slice](COORDINATOR_ACTIVE_CHECKPOINT.md) writes disk v16 and replaces
the durable bound coordinator's resident and ordinary apply-candidate Journal
with active `(R,N]` state, retaining terminals behind holes. The actual writer
checks full raw closure, uses batch-overlay identity lookups and stages raw/terminal
tails with exact shared reservation accounting. Draining R never refunds history
or grants. Runtime and private cut share one immutable checkpoint; canonical
metadata and complete current snapshot bytes remain resident.

Cold replay, member state, explicit Journal/control/Terminal paths and complete
SnapshotV1 materialization remain. Actual v15 and older inputs migrate only on a
successful batch; explicit snapshot repair checks the independently anchored old
prefix and exact checkpoint. Both profiles, 16 MiB, member/D1 limits, grants,
RPC v5 and deadlines remain unchanged. No GC, larger archive or performance claim.
S2 merged in #112; S3 member active state merged in #113 and S4 retained gateway
control in #117. The preceding paragraph records S2's original boundary, not a
claim that current members still use resident Journals. Frozen-source correctness,
fresh release 1,025-write recovery and remote CI remain required for later changes.
All-member closed checkpoints/credit reuse, bounded retained archives and remaining
full-history operations, and streamed snapshot closure remain open before removing
the lifetime bound.

### S4 retained gateway control

Actual managed execute/status/retry/abort and Prepare use bounded exact target
evidence and retained original-event cuts. See [the contract](RETAINED_GATEWAY_CONTROL.md)
for wire6/new Raft command upgrade restrictions, unchanged finite quotas and
explicit full-proof/snapshot boundaries. This slice merged in #117; test presence
or historical runs do not waive fresh-head gates for later changes.

### Global traversal terminal scalar properties

全局筛选后 out/in/有界 repeat 支持终端单字段 `values(key)`，复用固定分片会话与属性 fragment。
保留有序 bag、限定分区的来源身份及 missing/Null 区别，不默认截取100条。
无显式 limit 的无跳 values、标量后处理及全局原子快照仍未开放；预算和存储格式不变。
见 [组合契约](GLOBAL_VERTEX_TRAVERSAL.md)。

### Explicitly limited global scan values

无跳全局扫描支持显式顶点 limit 后终端单字段 values，保留过滤/limit 顺序、
限定分区的来源身份、等值 bag 和 missing/Null 区别；missing 不补扫以填满 limit。
复用原固定会话与标量执行器，扫描/遍历预算独立累计，无格式或一致性变化。
无显式 limit 的 values、标量后处理仍拒绝。见 [扫描契约](SERVER_GLOBAL_SCAN.md)。

### Qualified element dedup in fixed-version traversals

Shared routing-v3 and managed Traverse/TraversePage now support no-argument
vertex/edge `dedup()`, preserving qualified identity, first path and ordered
filter/limit placement. Scalar support is specified below; path dedup and
modulators remain unsupported. Existing
work/bytes, fixed-V and complete-result cursor limits remain; this is a general
query composition, not a managed-capacity, storage or recovery change. See
[contract](MANAGED_FIXED_TRAVERSALS.md#qualified-element-deduplication).

### Scalar dedup in fixed-version traversals

The shared routing-v3, single-partition V2Observed server and managed traversal
paths support standard no-argument `dedup()` after `values`, `label` and `id`.
Strict typed value equality retains the first source and input order, including
cross-partition ID scalars; positional limits and old-V result pages are unchanged.
Text retention is charged before cloning and every input consumes work. This is
a generic query-composition slice under #149/#110, not a capacity increase or
completion of data-local fixed-V execution. Legacy APIs, path/scoped dedup and
modulators remain unchanged. See the
[scalar contract](MANAGED_FIXED_TRAVERSALS.md#scalar-deduplication).


### Native bounded repeat on fixed-version traversals

Shared routing-v3、单分区 V2Observed server 和 managed Traverse/TraversePage
复用现有官方 repeat AST 与原固定快照，支持单个 out/in body、times(1..32) 和
可选 post-body emit。保留 qualified bag/path、循环、limit/filter/dedup 的位置语义，
累计 work/bytes/rows、TTL 与完整结果分页界限不变。此项 #155 只补齐通用查询语义，
不完成 #110 的 data-local fixed-V 容量目标，也不依赖 #124 或扩大生命周期配额。
见 [完整契约](MANAGED_FIXED_TRAVERSALS.md#bounded-native-repeat)。


### Selected vertex maps at fixed versions

Shared routing-v3、单分区 V2Observed server 和 managed Traverse/TraversePage
支持终端指定 1–8 字段的 vertex valueMap，复用官方 AST、同一快照与既有
PropertyMap 结果。qualified 来源、有序 bag、missing/Null、默认结果上限与完整
分页语义保持；共享 bounded property codec/validator 消除重复逻辑。
2 KiB 单 map、累计 work/bytes、TTL 和生命周期限制不变。#157 只补齐通用
多属性查询组合，不完成 data-local fixed-V 容量或 #110/#111 全量验收。
见[契约](MANAGED_FIXED_TRAVERSALS.md#selected-vertex-property-maps)。


### Shared strict scalar filtering

The routing-v3 / managed fixed-V / single-partition V2Observed executor accepts
standard `is(literal)` after scalar projection, with strict typed equality and
in-place is/dedup/limit/count composition. An explicit shared stream-kind
preflight preserves empty-input rejection and each entrypoint's consistency
boundary. This adds functionality without increasing materialized query capacity,
changing session lifetime or claiming data-local fixed-V execution. See
[the contract](MANAGED_FIXED_TRAVERSALS.md#strict-scalar-filtering).

The managed data-local fixed-V path now separates [fixed cohort membership](FIXED_COHORT_MEMBERSHIP.md)
(up to the existing 64-member protocol bound) from its one-to-eight query lanes.
Observed fanout, global capture/history budgets, and immutable cohort ownership
are unchanged; online expansion and membership beyond 64 remain future work.

### Full-cohort advancement fanout

[Bounded catch-up](BOUNDED_COHORT_FANOUT.md) separates 1–4 member pipelines
(default 3, matching existing RPC admission) from fixed membership and query
lanes. It preserves exact per-member history cuts, ordered consumption, complete
terminal certification and original deadlines. Snapshot capture remains serial
under its aggregate budget. Process benchmarks cover 3/9/12 members with small
graphs; larger histories/data, multi-host scaling and online resizing remain
unproven, rather than inferred from these latency measurements.

### 通用双时间图（单分片实验切片已实现，分布式目标未完成）

[#196](https://github.com/ashione/VelaGraph/issues/196) 与
[设计计划](BITEMPORAL_GRAPH_PLAN.md) 跟踪业务有效时间 T 与事务系统版本 V。
现有 managed fixed-V 本身不是双时间；独立的单分片 temporal store 已实现
下述本地 V/T 能力，不能将两个入口拼接成已交付分布式时态一致性。
真实跨片同 V/T、全规模动态 CoreHR、区间路径、保留/GC 与恢复集成仍须分别验收；
不硬编码 HR，不自造 Gremlin 方言。关联 #109/#110/#111、#191/#192 对象存储
及 #193/#194 checkpoint；持久化载体不自动提供时间语义，也不取代原有功能性能主线。

### #196 单分片持久化时间断言与邻接切片

`temporal_store` 在独立、显式版本化数据库中保存 canonical 点/边修订，
一次同步 WriteBatch 提交本地系统版本、有效区间及邻接候选索引，支持固定 V/T
的历史点读、端点可见性与 reopen/进程退出恢复验证。显式 ID 的官方 Gremlin
点查询和索引 out/in/both 遍历已复用原执行器；候选边及端点在同 V/T 重新解析，
不能把不同时期的关系拼为路径。当前 format 2 显式拒绝旧实验 format 1 数据库，
没有自动迁移或不完整索引回填。详见 [范围与格式](TEMPORAL_STORE.md)。

它仍使用本地系统版本，不是 distributed cohort V；全局 V()/E() 起点仍未开放。
**该第三个 temporal DB 不在既有双 DB checkpoint 或对象存储备份的覆盖范围内**，
现有上传/恢复不能作为时态恢复证据。后续须单独集成一致 cut 的权威记录与索引来源，
恢复校验成功后才开放目标视图查询；真实跨片同 V/T 与全规模验收继续跟踪 #196，
本节更新不关闭该任务，也不声称整体分布式双时间已完成。
