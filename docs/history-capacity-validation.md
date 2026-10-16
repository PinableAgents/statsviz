# SQLite 历史容量与性能验收

`TestHistoryFullVolumeAcceptance` 是一个显式启用的本地验收工具，用于构建真实的 7 天 SQLite 历史 fixture，并记录容量、写入吞吐和查询延迟。常规 `go test` 不设置环境变量时会立即跳过，不会在 CI 中生成 604800 条样本。

## 验收内容

- 通过 `internal/plot` 公开的 `NewList`、`Config`、`Aggregation` 和 `Collect` 读取当前 Go runtime 的完整默认配置与一份合法快照。
- 默认按 1 秒间隔生成 604800 条原始快照，查询窗口恰为 7 天（604800 秒），首末样本相隔 604799 秒。fixture 仅固定墙钟时间和采样频率，不等待真实 7 天。
- 每批最多 60 条通过真实 `Store.Enqueue` 和 `Flush` 写入，验证 `Dropped=0`、队列为空且 `LastError` 为空。
- 快照保留 runtime 的完整数值维度，并根据 gauge/rate/delta/histogram/event 语义生成可复现的合成数值变化；这些数值不是七天真实业务采样。
- 校验原始样本数、三层持久 rollup 数、首尾样本、中间 Point 身份、Meta 水位、全跨度预算和部分桶边界裁剪。
- 使用已有样本身份做幂等重写，在少量 `Enqueue/Flush` 与 Range 并发时验证读写，样本总数不变。
- 正常 `Close` 后重开数据库，再次校验最旧原始点。
- Meta、per-run/global Point、1m、1h、24h 以及 per-run/global 7d 查询分别记录首次读取、一次预热，以及不包含这两次的 p50/p95/max、响应字节、step、点数和分配量。`point_global` 和 `7d_global` 不带 `run_id`，对应 UI 默认访问路径，工具会先断言它们与单 run fixture 的 per-run 语义完全一致。

工具不会清理操作系统文件缓存；JSON 报告中 `os_cache_explicitly_cleared=false` 明确记录这一点。保留边界和分批物理清理已由常规 `TestRetentionBoundaryAndPhysicalCleanup` 覆盖，全量工具不重复构造大量过期数据。

查询延迟和分配量包含 Store 调用及返回值的 JSON 编码，更接近 HTTP 层实际需要承担的成本。工具显式使用 60 条一批、主动 Flush 的加速写入；吞吐与累计分配不能直接解释为默认每秒一条、每秒提交的 CPU 或常驻内存开销。RSS 按每 10000 条与阶段边界采样，报告最大值是采样最大值，并非连续跟踪得到的真实峰值。

## 输出安全与复用

`STATSVIZ_HISTORY_ACCEPTANCE_DIR` 必须是 `/tmp` 或当前系统临时目录下的专用子目录。工具只使用两个固定名称：

- `statsviz-history-acceptance-v2.db`
- `statsviz-history-acceptance-v2.json`

数据库包含专用 marker，记录工具 ID、格式版本、样本数、间隔、固定时钟、当前 Go 版本对应的配置哈希、快照形状哈希和状态。fixture ID 还绑定聚合 payload 版本，v2 工具使用独立文件名，避免复用旧 object 格式夹具来衡量紧凑格式。验收打开数据库时会在 `Open` 首次 cleanup/recover 之前注入该固定时钟，不会先用真实墙钟清理 fixture。只有所有样本持久、功能断言、并发检查及 Close/reopen 通过后，状态才会变为 `complete`。完整且参数一致的 fixture 可直接复用。`incomplete` 只有在全部身份参数匹配、已记录生成结束后的 RunID、且只读核对该运行实际 raw 行数完整时才可复用为待验收候选；仍重新执行全部功能、并发和 Close/reopen 检查，通过后才标记 `complete`。其他未完成或参数不同的工具专属 fixture 会在获得路径锁后重建。缺少 marker、marker 不匹配或报告不属于本工具时会直接拒绝覆盖。

## Smoke 命令

小规模验证至少使用 3600 条，对应 1 小时跨度。24h/7d 查询项在 smoke 报告中会明确记录实际有效跨度，不会把 1 小时冒充为 7 天。

```bash
STATSVIZ_HISTORY_ACCEPTANCE=1 \
STATSVIZ_HISTORY_ACCEPTANCE_DIR=/tmp/statsviz-history-acceptance-smoke \
STATSVIZ_HISTORY_ACCEPTANCE_SAMPLES=3600 \
STATSVIZ_HISTORY_ACCEPTANCE_ROUNDS=3 \
env -u GOROOT go test ./history/sqlite \
  -run '^TestHistoryFullVolumeAcceptance$' -count=1 -timeout=0 -v
```

## 604800 条全量命令

不设置 `STATSVIZ_HISTORY_ACCEPTANCE_SAMPLES` 时默认生成 604800 条；不设置 `STATSVIZ_HISTORY_ACCEPTANCE_ROUNDS` 时每类查询执行 20 轮正式测量。建议使用独立目录，并保留 `-timeout=0`。

```bash
STATSVIZ_HISTORY_ACCEPTANCE=1 \
STATSVIZ_HISTORY_ACCEPTANCE_DIR=/tmp/statsviz-history-acceptance-full \
env -u GOROOT go test ./history/sqlite \
  -run '^TestHistoryFullVolumeAcceptance$' -count=1 -timeout=0 -v
```

工具每生成 10000 条输出一次进度。报告会同时记录实测 DB/WAL 字节、平均样本 payload，以及按当前 smoke 线性外推的 604800 条原始 payload 和 DB+WAL 体积。外推值仅用于全量运行前规划 `/tmp` 空间，不是全量实测值；最终容量结论必须以 604800 条报告为准。

## 604800 条完整数据实测（2026-10-16）

环境为 Apple M2（8 核、16GiB）、Go1.26.3、darwin/arm64。单运行，26 组 payload 序列（包含 lastgc 事件，对应 UI 25 张图）、523 个数值单元：98 gauge、14 rate、5 delta、405 histogram、1 event。使用完整默认 runtime 形状和合成数值，经实际 Enqueue/Flush 加速生成七天数据。

最终报告 `passed=true`、`phase=complete`，所有 raw/rollup 行数、首尾和中间单点、预算、边界裁剪、Meta 水位、并发幂等写入以及 Close/reopen 均通过。报告保存在仓库内，便于复核：

- [首次完整构建记录](../do-tasks/1016-k0s4/full-volume-first-build.json)：604800 条全部生成，随后发现 Close 封存超时；此报告整体为失败，不代表最终验收。
- [全局查询优化前](../do-tasks/1016-k0s4/full-volume-before-global-query.json)：封存修复后功能验收通过，保留全局查询 p95=561.473ms 的基线。
- [最终完整验收报告](../do-tasks/1016-k0s4/full-volume-final.json)：复用同一数据库，重新执行全部断言与20轮查询，无需再次生成数据；`build.reused=true`，构建时长为0表示没有重建。
- [最终新库 smoke](../do-tasks/1016-k0s4/final-smoke.json)：优化入队校验后重新写入3600条，验证新的写入路径；构建1.576秒、2284.7条/秒，未将该吞吐外推为全量结果。

| 容量项目 | 实测值 |
| --- | ---: |
| raw / 10s / 1m / 10m 行数 | 604800 / 60480 / 10080 / 1008 |
| raw payload 合计 / 平均 | 2,924,746,051 bytes（2.724GiB）/ 4835.9 bytes |
| 三层 rollup payload 合计 | 1,204,740,631 bytes |
| 最终 DB / WAL | 5,211,832,320 bytes（4.854GiB）/ 0 bytes |
| writer 状态 | dropped 0、last_error 空、queue 0 |
| 并发验证 | 180 次幂等重写 + 5 次 Range，raw 总数保持604800 |

首次全量构建为511.265秒、1182.95条/秒，此时尚未优化每个标量的 Decoder 分配。写入阶段每10000条采样的最大RSS为31,342,592 bytes（29.89MiB），该阶段Go累计分配约863.2GiB；后者是累计分配，并非常驻内存。最终新库 smoke 与校验微基准已验证后续写入优化，但没有再次生成604800条来重测优化后的全量写入吞吐或累计分配。

以下每类均为20轮正式查询，另列首次读取，排除一次预热；延迟单位为ms，包含Store调用与完整响应JSON编码，没有清理OS文件缓存：

| 查询 | 首读 | p50 | p95 | 最大 | 响应 bytes | 粒度 / 点数 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Meta | 1.533 | 0.537 | 0.635 | 0.659 | 37,290 | — / — |
| Point（限定运行） | 0.379 | 0.319 | 0.349 | 0.352 | 42,178 | raw / 1 |
| Point（全局） | 0.348 | 0.344 | 0.366 | 0.389 | 42,178 | raw / 1 |
| 1m | 11.687 | 11.340 | 11.509 | 11.520 | 393,043 | raw / 60 |
| 1h | 37.509 | 37.245 | 37.903 | 37.970 | 437,793 | 1m / 60 |
| 24h | 91.082 | 89.900 | 92.743 | 119.669 | 1,088,924 | 10m / 144 |
| 7d（限定运行） | 460.686 | 460.766 | 463.282 | 465.688 | 1,389,038 | 1h / 169 |
| 7d（全局，UI 默认） | 495.439 | 460.075 | 467.062 | 470.761 | 1,389,038 | 1h / 169 |

同一完整库中，默认全局7d查询 p95 从561.473降至467.062ms，减少16.8%；仍返回相同的169个点、1小时粒度、1,389,038 bytes，完整响应与限定运行查询等价。169点是UTC一小时桶与七天查询起止边界共同决定，并非丢样。全局Point p95为0.366ms；本机此单运行、缓存未清理的20轮样本达到设计中的100ms/500ms目标。

全局7d累计Go分配约273.2MB/次，Point约260KB/次；查询阶段结束时采样RSS为172,408,832 bytes（164.42MiB），这是阶段采样而非连续测量峰值。不能将本次结果解释为冷缓存、长期高并发、多运行生产p95、真实七天持续运行CPU或滚动删除后的稳定文件高水位。

基于当前523维、每秒采样的实测，可为独立历史库预留至少8GiB可用空间，并监控meta中的DB/WAL、队列、丢样和LastError。增加自定义指标或提高到250ms采样时应重新运行容量验收；SQLite会复用删除释放的页，7天滚动窗口不意味着文件每天缩小。

## 已落实的性能优化

- Point 将保留窗口和目标时间合并成一个后候选下界，并拆分全局/运行内 SQL。3600 条库中后候选从 10826 个 VM 步骤降至 21/23 个，避免中间时间查询从保留窗口起点扫描。
- 启动恢复先读取未封存运行，再逐运行按主键取最高 seq 的尾样本，只重算尾部 10 秒桶及父桶。
- 聚合采用私有 v1 数字 kind/固定九项 tuple，继续读取旧 object。963 桶复制库中 payload 从 45,376,710 降至 13,988,922 bytes，状态与完整 Range 响应逐项等价；抽样20轮 p95 从 695.937 降至 415.773ms，分配从259.6降至206.6MB。这是稀疏夹具的优化对比，最终判断以完整数据实测一节为准。
- 全量运行发现 Close 会无条件重写未结束运行的全部 71,568 个聚合桶，即使它们已 final；约1.2GB payload 被重复更新并触发10秒超时。现在只更新 `is_final=0` 的桶，触发器回归确认已 final 行不发生 UPDATE，完整库 Close/reopen 已通过。
- 全局 Range 已知只有一个候选运行时，聚合读取直接使用复合主键，省去全局时间二级索引回表；多运行保留 `rollups_time`。rollup-only 场景下 global/per-run 响应完全等价，多运行结果仍包含所有运行。
- 入队校验保留完整 JSON 语法及类型检查，改为识别已验证 token 的 number/null 类型并检查有限数值范围，移除每个数值单元的新 Decoder；原始JSON不经浮点重编码。拒绝超出聚合数值范围的巨大指数和字符串数字，避免它进入写入队列后才因聚合解码失败。

新程序可读取新旧聚合格式，旧程序无法读取新写入的 v1 数组聚合。升级前应保留数据库副本供降级使用；这次没有启用 payload 压缩或引入外部时序库。

在 Apple M2、Go1.26.3 上，默认523维快照校验微基准执行5次、每次1秒，中位数如下。它测量 `validObject`，不包含 SQLite 事务或整次 Enqueue。

| 指标 | 优化前 | 优化后 |
| --- | ---: | ---: |
| ns/op | 397,266 | 85,803 |
| B/op | 1,330,108 | 57,171 |
| allocs/op | 3,481 | 780 |

分配字节降低95.7%，耗时降低78.4%。可用下面的保留基准复测当前实现：

```bash
env -u GOROOT go test ./history/sqlite -run '^$' \
  -bench '^BenchmarkValidObjectRuntimeSnapshot523$' -benchmem -benchtime=1s -count=5
```

## 优化前的 3600 条 smoke 参考

2026-10-16 在 `go1.26.3 darwin/arm64` 上使用旧版 v1 工具、旧 object 聚合格式执行一次，未清理 OS 文件缓存。这是 1 小时、3600 条的小规模结果，**不是 604800 条全量验收，也不代表当前紧凑格式的容量**。以下作为优化前的参考保留。

| 项目 | 实测值 |
| --- | ---: |
| runtime plot / 数值单元 | 26 / 523 |
| 类型分布 | gauge 98、rate 14、delta 5、histogram 405、event 1 |
| raw / 10s / 1m / 10m 行数 | 3600 / 360 / 60 / 6 |
| fixture 构建时间 / 吞吐 | 2.642 s / 1362.6 samples/s |
| 平均 raw payload | 3212.7 bytes/sample |
| DB / 结束时 WAL | 38,293,504 / 4,152 bytes |
| writer 状态 | dropped 0、last_error 空、queue 0 |
| 并发验证 | 180 次幂等重写 + 5 次 Range，样本数仍为 3600 |
| 1m p95 / 响应 | 8.498 ms / 248,971 bytes，60 raw points |
| 1h p95 / 响应 | 48.285 ms / 437,094 bytes，60 points，step 1m |

按该 smoke 的平均 payload 线性外推，604800 条 raw payload 约为 1.81 GiB；按旧格式 DB+WAL/样本线性外推约为 5.99 GiB。后者包含完整 523 维三层 rollup 和 SQLite 页/索引的 smoke 比例，仍可能受页复用、WAL checkpoint 及数值长度变化影响。启动全量前建议 `/tmp` 至少预留 8 GiB 空间；这仅是容量规划值，不得当作全量实测。

随后在同一数据库上仅复用完整 fixture 执行 3 轮查询，日志明确输出“复用完整 fixture”，报告中 `build.reused=true`，没有重新生成 3600 条样本。首次构建报告已另存为 `/tmp/statsviz-history-acceptance-smoke-1016-first-build.json`，因此下列复用查询报告没有覆盖首次构建证据。Point 没有 step 概念；表中的 7d 项在 smoke 中实际有效跨度仍为 1 小时。

| 查询 | 首读 | p95 | 分配/轮 | step / points |
| --- | ---: | ---: | ---: | ---: |
| per-run Point | 0.353 ms | 0.329 ms | 241,261 bytes | 不适用 / 1 |
| global Point | 0.391 ms | 0.352 ms | 241,437 bytes | 不适用 / 1 |
| per-run 7d（实际 1h） | 48.119 ms | 48.501 ms | 24,571,165 bytes | 1m / 60 |
| global 7d（实际 1h） | 52.771 ms | 53.274 ms | 24,591,552 bytes | 1m / 60 |

global 与 per-run 候选查询分别通过完整响应等价断言，覆盖真实 UI 默认省略 `run_id` 的 SQL 入口；本组数据仍只是 3600 条复用 smoke，不是 604800 条全量性能结论。
