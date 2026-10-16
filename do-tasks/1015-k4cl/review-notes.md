# 集成复核记录

最终状态：下方按时间记录的待核实项均已修复并复验，保留过程记录便于追踪。最终交付结果见文末。

## 前端待核实

- 聚合点的 seq 是桶末样本序号，相邻桶通常不连续。不能直接按 seq 差大于 1 就在每个聚合桶间插入缺口；原始点才适用这个规则，聚合应依赖 gaps/桶覆盖信息。
- 聚合历史与实时尾部合并时，需要避免同一聚合桶与桶内秒级样本同时作为同等曲线点混画；水位过滤逻辑必须实际影响合并结果。
- LatestRequest.cancel 需要使正在解码或不遵守 AbortSignal 的旧请求失效，而不只是中断 fetch；成功返回前检查代次和 signal。
- 时间缓存需要同时有样本数与字节预算；不能在自定义指标较多或高频采样时仅靠 20000 个样本限制占用。
- 确认 API 已知缺口、跨运行、空样本及多配置热力图正确断开，且历史图不被 WS 配置事件替换。

## 第二轮复核重点

- 后端原草稿每次写入回扫三个粒度的原始快照，已要求仅最小桶处理 raw，较大桶合并低级聚合，避免监控自身增加高开销。
- Event 聚合应保留真实时间；聚合标志不能被画成 1970 年时间。
- 前端 metadata 上限要刷新；fallback 配置不能重复生成图表；交互开始即取消旧请求。
- 当前根包与 internal/plot 的短测试和 race 通过；前端已有 11 个测试通过，后续仍需验证修复后的版本和实际页面。

## 已确认环境

- `env -u GOROOT go ...` 可使用 Go 1.26.3。
- 前端 npm ci / npm run build 基线通过。
- 全量基线的默认示例使用 8080，被已有服务占用；不终止该服务。
- `/tmp/statsviz-browser-check/node_modules/playwright` 和独立 headless Chrome 可用于最终界面验证。

## 浏览器实测（第一轮）

- 独立端口、真实无历史 WebSocket、Vite 页面：无 JavaScript 错误，1m/5m/10m、暂停恢复、主题可操作。
- 确认重复图表缺陷：配置只有 25 张图，但 DOM 出现 50 张图；原因是 history.enabled=false 时 run_id=0 仍被 schemaEntriesFor 当作额外 live schema。第二轮必须检查图表数等于配置数。
- 实际 Go 1.26.3 默认快照约 523 个数值单元；7 天概览必须自动降低粒度以满足总数值预算。

## SQLite 待完成后复核

- Store.Close 完成信号与 closeErr 读写顺序，避免 Flush/Close 并发 race；Flush 超时应返回，后台写入可继续。
- 重启时 EndedAt 不应被当作停机期间存在数据；前端以 run 分段断开。
- 边界部分桶与水位应来自同一读事务；原始整数不经 float64 落盘。

## 查询草稿第二轮（需开发完成后验证）

- 523 维 × 3（主值/min/max）× 337（7 天/30 分钟）= 528753，超过 300000 预算。目前 Range 只 ErrTooLarge 未重试粗粒度，完整七天UI可能必定413。必须自动再粗化，保留413给维度单点都超预算等无法满足情况。
- readRollupRange 对600000ms大桶的首尾整个区间调用readRowsForAggregate，声明“最多10秒raw”与实现不符。应层层切分10m→1m→10s，最末边界raw≤10s。
- readWatermarks SELECT max(seq) GROUP BY run_id WHERE ts>=cutoff 可能每次扫描7天raw。用run列表每run索引末尾查询或持久化水位；Meta min/max合并查询同样可能扫全量，拆成时间索引首尾LIMIT1。
- ceilDiv/value+divisor-1 与 to-from 对极端int64查询溢出，需要限制合理时间或溢出安全运算。

## 浏览器实测（真实 SQLite，第二轮）

- /tmp/statsviz-dense-e2e.db 模拟7天跨度、1007个10分钟桶、每点523维，Range返回 history: result too large，耗时845ms，证实完整维度预算问题。
- Vite代理真实历史服务后，WS正常、图表25张且无控制台错误，但前端未发出meta请求。怀疑 LatestRequest 构造器存储未bind的 window.fetch 后以 this.#fetch 调用造成 Illegal invocation（Node单测不会发现），需浏览器验证并修复。

- 已确认原生 fetch Illegal invocation：LatestRequest.json 第46行以字段方法调用未绑定Window的fetch。临时在浏览器绑定fetch后，真实SQLite流程通过：25图、7d切换、526行单点详情、固定历史范围稳定且无JS错误。最终必须去掉临时绑定重新验证。
- 前端以duration_ns做实时尾部权重，但WS目前不携带duration_ns；需在兼容metrics消息中添加采样实际间隔（旧客户端忽略新字段），避免纳秒权重与fallback=1混用。
- readRuns按run started/end墙钟过滤可能漏掉时钟回拨后的真实样本；查询应以samples/rollups覆盖为依据。

## 已执行 EXPLAIN 的阻塞级性能问题

`readRowsForAggregate` 的 OR 条件使SQLite只使用PRIMARY KEY(run_id=?)，每次重建10秒桶都会扫描该run全量raw；子查询max(seq)也走同样主键。1007行fixture的EXPLAIN返回 SEARCH samples USING PRIMARY KEY (run_id=?)，没有ts范围；604800行时每秒扫描7天不符合设计。需拆成时间索引range查询+按时间索引取前置样本（ORDER BY ts_ms DESC,seq DESC LIMIT1）或UNION ALL后在小结果里按seq排序；不要让ORDER seq破坏时间范围扫描。readWatermarks同样确认为SCAN samples USING COVERING INDEX samples_run_time，需要逐run末尾LIMIT1或持久水位。

七天523维预算自适应已由存储开发者完成：同fixture现在169点、step3600000、815974字节、1.617秒返回成功。后续预估维度预算应避免先构建超预算粒度再完整读第二遍。

## 嵌入资源和最终页面验证

- `npm test` 当前25项全部通过，`npm run build`成功。
- 仓库既有 `scripts/zip.sh` 在macOS因 `rm dist.zip -f` 报错（已删除zip后失败）；主控已用zip命令重新生成并逐文件比较4个嵌入产物与dist一致。建议改为 `rm -f dist.zip`。
- 真实Go嵌入页面（无Vite、无替代fetch）：25张图，1h/6h/24h/7d、真实单点、固定范围、暂停、reload均通过，全部history请求200，无JS错误。
- SIGTERM正常关闭并使用同一路径重启，run1→run2，旧run的seq96真实点仍可读取，最早时间保留。
- `RingBuffer.js` 已无消费者，当前改动无必要且slice(0)会返回整个数组（原版本为空）；应还原非必要变更或保留原语义。
- 暂停跟随视图时onPreset仍走currentBounds frozenView，选择5m/10m可能仍显示暂停前范围；需检查暂停下用户操作是否能主动更新窗口。

## 第三轮真实浏览器复现

- 暂停1m视图后点击5m，图表跨度仍60000ms，而应变为300000ms；自定义输入会切history可动，但预设操作被frozenView冻结。
- 当前画面只有run2时，手输run1时间：无run_id直接API found=true/run1；UI却发送run_id=2并显示附近没有真实采样点。手输绝对时间不能自动限定当前图中最近run；只有点击具体schema图表时才限定run。

## 最终修复及验收（2026-10-16）

- 原始桶重建改用有界时间索引查询和序号前置样本查询；上级聚合合并下级桶，边界逐级下降到不足十秒的 raw；EXPLAIN 回归已覆盖。元信息首尾和每运行水位使用索引 LIMIT 查询。
- 完整 523 维七天概览预估总单元预算后选择一小时粒度，避免先读超预算粒度；1007 桶夹具最终返回 168 点、811378 字节、724ms。该结果是单次观察，夹具未包含七天全量原始快照，不代表生产 p95 达标。
- 正常关闭和新运行启动均按最高已持久化 seq 对应时间封存旧 run，空运行使用自身开始时间；读取元数据时限制 4096 个运行、10000 个缺口、1MiB 单运行配置及 8MiB 总字节，Range 固定元数据只预检一次。
- 修复默认 fetch receiver、重复 schema、缓存字节预算、请求代次取消、真实 GC 时间及实时 duration_ns；聚合桶不会被普通 seq 跨越误判缺口。
- 实时新运行缺持久配置时使用明确 run_id 的 WS 聚合声明；直方图无基线或断序时保留有效覆盖并标记 incomplete，不伪造完整尾部。历史和 WS 按水位、样本身份及相同桶粒度合并。
- 手输时间不再误带当前 run_id 或当前运行容差；暂停切换预设主动更新跨度；slider、zoom、preset、实时和配置切换取消旧 point。RingBuffer.js 保持原版，macOS ZIP 脚本已修正。
- 最终 Go short、race、vet 及 linux/amd64、windows/amd64、darwin/arm64 的 CGO_ENABLED=0 构建通过；前端 33/33 测试和构建通过。dist.zip 与 dist 四个文件逐字节一致，无测试服务地址。
- 最终真实 Go 嵌入页面验收：25 图、四个长预设、526 行旧运行快照详情、暂停 5m=300000ms、固定历史不随 WS 推移、慢 point 取消、返回实时和刷新均通过；15 个历史 HTTP 响应全为 200，无 JS 错误。同一数据库已进入 run3，run1 仍可回看。
- 未跑固定端口的既有 TestExamples（short 模式跳过），本机 8080 被非本任务服务占用；历史示例已在独立回环端口实际启动验收。未提交、推送或发布。
