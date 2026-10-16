---
id: "1016-k0s4"
title: "继续完成 SQLite 七天历史全量验收：增加可复现的容量性能验证工具，验证604800原始样本与完整指标集，定位查询写入瓶颈并按证据优化，记录实测边界"
status: "completed"
current_phase: 5
phase_name: "Complete"
max_phases: 5
dev_mode: "current"
auto_commit: false
use_worktree: false
worktree_dir: ""
base_branch: "main"
created_at: "2026-10-16T07:23:45.826851"
completion_promise: "<promise>DO_COMPLETE</promise>"
---
















# 需求说明

继续完成 SQLite 七天历史全量验收：增加可复现的容量性能验证工具，验证604800原始样本与完整指标集，定位查询写入瓶颈并按证据优化，记录实测边界

## 背景与约束

上一轮已完成功能和真实浏览器交付；本轮用户要求继续，补齐未完成的全量容量与性能验收。沿用当前工作区、禁止自动提交和推送。采用 do/pinable 开发流程：验收工具与生产性能修复分文件独立实施，主会话负责测量和最终集成。当前磁盘可用约88GiB，真实默认快照约2KiB，聚合约35KiB，预计全量数据库数GiB。上一轮1007桶夹具并非604800全量原始样本，必须明确区分。运行环境Go1.26.3/darwin arm64，所有Go命令移除继承GOROOT。先记录基线，再做有证据的优化，不引入全局缓存或外部时序库。

## 实现说明

- 完成 SQLite 七天完整容量验收工具（显式环境变量启用，默认CI跳过）：真实 Enqueue/Flush 写入604800条、523维完整默认指标形状及合成值；验证raw与三层rollup、窗口裁剪、单点身份、Meta水位、预算、并发幂等重写和Close/reopen。冻结时钟在首次cleanup/recover前注入。v2 fixture ID绑定wire版本；带专属marker与实际行数核对，可安全复用全部生成但尚待验收的数据，仍重跑全部断言。
- 修复Point后候选从cutoff扫描至目标时间的问题：全局/运行内拆为四条SQL，时间下界合并为max(cutoff,at)，分别使用时间索引；后候选10826→21/23个VM步骤。恢复先查未封存run，再逐run主键读取最高seq尾部；封存只UPDATE is_final=0，消除全量库关闭时重写1.2GB聚合payload的超时。
- 聚合私有payload改为带版本数字kind九项tuple；保留读取旧object，原始JSON、表结构、codec=json及对外协议保持一致。963桶复制库payload减少69.2%，逐桶状态及完整Range响应等价。新程序写入后旧程序不能读取新数组格式，已记录降级兼容限制。
- 全局Range只剩一个候选运行时按rollup复合主键读取；多运行仍走全局时间索引。保留相同预算与完整响应，并覆盖rollup-only候选及多运行回归。入队校验移除每个数值单独的Decoder，保留JSON类型检查与原始整数字节，入队拒绝超出聚合有限数值范围的巨大指数；API回归确认后续正常样本不会被阻塞。
- 最终604800全量报告通过：DB5211832320B（4.854GiB），raw604800、rollup60480/10080/1008，无丢样/写入错误；默认globalPoint p95 0.366ms，default global7d p95 467.062ms（优化前同库561.473ms），169点/1h/1389038B，20轮包含JSON编码且未清理OS缓存。首次完整构建511.265秒发生在校验分配优化前；后续新建3600条库验证优化写入路径，没有冒称重测优化后全量写入吞吐。
- README、设计与容量验收文档已补充兼容性、复现命令和实测边界，四份JSON报告保存在任务目录。已有前端资源未在本轮修改。采用current工作区、auto_commit=false，不提交、不推送。
- 最终验证：go test -short ./...、go test -race -short ./...、go vet ./...，以及CGO_ENABLED=0的linux/amd64、windows/amd64、darwin/arm64构建全部通过；gofmt、git diff --check、文档链接与报告完整性检查通过。巨大指数与后续正常Flush回归通过；加入有限数值检查后的校验基准85803ns/op、57171B/op、780allocs/op。

## 进度日志

- 2026-10-16T07:23:45.826851 · 阶段 1/5 (Understand) · 任务已创建（开发模式：current，自动提交：false）
- 2026-10-16T07:24:57 · 阶段 3/5 (Design) 开始 · 沿用已确认SQLite设计和current/不提交配置；新增显式启用的604800原始样本验收工具，主会话并行采集现有查询CPU与分配基线，按证据优化
- 2026-10-16T07:25:20 · 阶段 4/5 (Implement) 开始 · 性能基线20次查询：p50 683ms、p95 696ms，每次分配约260MB；正在根据CPU profile定位解码和矩阵保留开销，同时开发全量验收工具
- 2026-10-16T07:31:08 · 阶段 4/5 · 额外EXPLAIN复核发现recoverTails仍从samples全扫描且逐行相关max(seq)，已记录需改为从未封存run逐个主键尾部LIMIT1；批量与单条提交基线已分开记录
- 2026-10-16T07:33:50 · 阶段 4/5 · 3600条真实全形状smoke已构建并通过验收，平均payload3213字节，旧聚合格式DB约38.3MB，单点约12ms证实扫描问题；正式604800运行等待紧凑codec与Point/恢复索引修复合入后执行
- 2026-10-16T07:35:20 · 阶段 4/5 · 聚合私有wire选定带版本数字kind固定9项tuple，抽样47KB降至14.75KB，仍读取旧object格式；验收工具已通过3600真实样本smoke并补充global查询与初始化时钟复核
- 2026-10-16T07:45:10 · 阶段 4/5 · Point与恢复修复的normal/race/vet和执行计划回归通过；后候选10826VMsteps→21/23，smoke单点p95约0.35ms。紧凑codec逐桶状态与Range全响应等价通过，抽样p95 696→414ms，继续降低解码分配后进入完整604800验收
- 2026-10-16T07:48:19 · 阶段 4/5 · codec完成：963桶payload降低69.2%，抽样p95 696→416ms且分配260→207MB；评审发现验收fixture identity缺少聚合wire版本，先修复复用版本再执行604800全量
- 2026-10-16T07:50:16 · 阶段 4/5 · 验收复用标识已绑定aggregatePayloadVersion，工具与固定文件名升级v2并通过回归；604800条全量验收开始，通过真实Enqueue/Flush分批构建，未并行运行CPU密集检查以免干扰测量
- 2026-10-16T07:59:19 · 阶段 4/5 · 604800条已真实写入，raw 2.924GB、DB约5.21GB、build511s、无丢样，关闭验收暴露sealOpenRuns无条件重写全部71568聚合行导致10s超时；正在修为只更新is_final=0，并安全复用已生成但未完成验收的本工具fixture，保留首次报告
- 2026-10-16T08:03:23 · 阶段 4/5 · Close重复重写修复后604800全量功能/并发/Close重开验收通过；DB5211832320B，per-run7d p95 470.9ms，UI默认global7d p95 561.5ms，响应相同169点1h；继续优化单适用run的global聚合读取，保留多run全局索引路径与相同返回预算
- 2026-10-16T08:07:13 · 阶段 4/5 · 最终新库3600样本smoke通过，含真实新Enqueue校验与聚合/重开；523维校验微基准中位数397266→80693ns，1330108→57186B/op（-95.7%）。单run global聚合主键读取与rollup-only/多run回归通过；开始同一604800完整库最终20轮复测
- 2026-10-16T08:10:19 · 阶段 4/5 · 最终全量复测通过：default global7d p95 467.062ms（同库561.473→467.062，响应不变），globalPoint0.366ms；Go short/race/vet及三平台CGO0构建通过。最终评审另发现合法JSON巨大指数会使聚合ParseFloat失败并卡pending，正在补入队有限数值验证与拒绝后后续正常Flush回归
- 2026-10-16T08:13:07 · 阶段 5/5 (Complete) 开始 · 实现与评审问题均已修复，完整604800验收及最终新库smoke通过，Go short/race/vet和三平台CGO0构建全部通过，文档与JSON证据已完成
- 2026-10-16T08:13:07 · 阶段 5/5 · 最终校验通过；Go格式与diff检查、文档链接及报告核对无问题，仓库无临时DB/CPU profile；准备按auto_commit=false完成流程，不提交推送
