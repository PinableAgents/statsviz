# 历史功能实施契约

这是前后端并行开发的字段约定，详细设计见 sqlite-history-design.md。命名以本文件为准，新增 JSON 字段不改变现有 series 格式。

## Go 公共包 history

- `Kind`：字符串类型，常量 `Gauge="gauge"`、`Rate="rate"`、`Delta="delta"`、`Histogram="histogram"`、`Event="event"`。
- `SeriesSpec`：`Kinds []Kind`（JSON kinds），`Heatmap bool`（JSON heatmap）。热力图可用单个 Kind 表示所有桶。
- `RunConfig`：`StartedAtMS int64`、`IntervalNS int64`、`Config json.RawMessage`、`Aggregation map[string]SeriesSpec`。
- `Run`：`ID int64`（id）、`StartedAtMS int64`（started_at_ms）、`EndedAtMS *int64`（ended_at_ms，可空）、`IntervalNS int64`（interval_ns）、`ConfigHash string`（config_hash）、`Config json.RawMessage`（config）、`Aggregation map[string]SeriesSpec`（aggregation）。
- `Sample`：`RunID int64`（run_id）、`Seq int64`（seq）、`Timestamp int64`（timestamp）、`DurationNS int64`（duration_ns）、`Series json.RawMessage`（series）。Series 必须保存原始 JSON 对象，不能先转换 float64 再落盘。可增加内部增量/有效性字段。
- `RangePoint`：样本身份和时间字段同 Sample，`Series json.RawMessage`、`Min json.RawMessage`（min，可空）、`Max json.RawMessage`（max，可空）；允许增加覆盖信息。Series 内保持 plotName → 数值数组，可用 null 表示缺失。
- `Watermark`：`RunID int64`（run_id）、`Seq int64`（seq）。
- `Gap`：`RunID int64`（run_id）、`From int64`（from）、`To int64`（to）、`FirstSeq int64`（first_seq）、`LastSeq int64`（last_seq）、`Reason string`（reason）。
- `Status`：至少 `LastCommitMS int64`（last_commit_ms）、`QueueDepth int`（queue_depth）、`Dropped int64`（dropped）、`LastError string`（last_error）、`DBBytes int64`（db_bytes）、`WALBytes int64`（wal_bytes）。
- `Meta`：`Enabled bool`（enabled）、`From int64`（from）、`To int64`（to）、`RetentionMS int64`（retention_ms）、`IntervalMS int64`（interval_ms）、`Runs []Run`（runs）、`Status Status`（status）、`Watermarks []Watermark`（watermarks）。
- `RangeRequest`：`From, To, RunID int64`、`MaxPoints int`、`Plots []string`。
- `RangeResponse`：`From int64`（from）、`To int64`（to）、`StepMS int64`（step_ms）、`Points []RangePoint`（points）、`Runs []Run`（runs）、`Gaps []Gap`（gaps）、`Watermarks []Watermark`（watermarks）。空集合编码为 []。
- `PointRequest`：`At, RunID, ToleranceMS int64`。
- `PointResponse`：`Found bool`（found）、`RequestedAt int64`（requested_at）、`OffsetMS int64`（offset_ms）、`Sample *Sample`（sample，可空）、`Run *Run`（run，可空）。

`Store` 接口方法：

```text
StartRun(context.Context, RunConfig) (Run, error)
Enqueue(Sample) error
Flush(context.Context) error
Meta(context.Context) (Meta, error)
Range(context.Context, RangeRequest) (RangeResponse, error)
Point(context.Context, PointRequest) (PointResponse, error)
```

错误至少提供 `ErrInvalidQuery`、`ErrTooLarge`、`ErrClosed`、`ErrQueueFull`；SQLite 具体 Store 另提供 Close。外部调用者拥有 Store 的生命周期，Server.Close 只停止生产并等待 Flush。

## 服务端与 HTTP

根包 `History(history.Store) Option` 显式启用持久化，`Server.History() http.HandlerFunc` 提供统一历史入口，可自行注册 `root/history/`。Register 同时注册该路径。未启用时 meta 返回 enabled=false，其余历史接口返回明确不可用状态。

WS config 增加 `history:{enabled,run_id,interval_ms}`，WS metrics.data 增加 run_id、seq 和 duration_ns，保留 timestamp 和 series。duration_ns 与同一快照写入历史存储的实际采样间隔一致；旧客户端可忽略该新增字段。

- `GET history/meta` → Meta。
- `GET history/range?from=...&to=...&run_id=可选&max_points=1500&plots=逗号分隔` → RangeResponse。
- `GET history/point?at=...&run_id=可选&tolerance_ms=可选` → PointResponse。

路径相对 Root。参数时间均为 UTC Unix 毫秒，区间左闭右开。参数错误 400，超大响应 413，历史未启用或存储不可用为 503（meta 除外），未知路径 404，方法不符 405。响应不得缓存。查询超时 2 秒，响应 JSON 上限 8 MiB，错误返回 JSON error。

## 文件分工

- 存储开发：history/**、go.mod、go.sum，可校正 docs/sqlite-history-schema.sql 与实际迁移一致；不要修改根包或前端。
- 服务接入开发：statsviz.go、clients.go、history.go、userplot.go、internal/plot/**、Go 接入测试、README、_example/history 示例；不要修改 history/** 或 go.mod。
- 前端开发：internal/static/src/**、index.html、前端测试与必要 npm scripts；不要修改 Go 或资源 zip。
- 根会话负责统一测试、接口集成、评审、最终构建与 dist.zip。
