# 可观测性（Phase 1）

每个进程都暴露 `GET /metrics`（Prometheus 文本格式）。`gateway-api` 同时提供业务 API；`dispatcher` 和 `batch-controller` 只提供 `/healthz`、`/readyz`、`/metrics`。选主进程另有 `GET /leaderz`：200 表示本进程持有 fencing token，503 表示 standby。

指标名与 [RFC 0001 §5.6](rfcs/0001-production-architecture.md) 一致。Phase 1 只实现不依赖 MySQL 的序列。文件和作业仍在 Redis，所以没有对象存储或数据库延迟。

抓取时按进程角色汇总。队列深度、已租约数、共享 in-flight、batch 作业状态在每次抓取时读 Redis。尝试次数、deadline slack、gate 决定和本地 budget 记在**实际处理该请求的 dispatcher** 上，API 进程的 `/metrics` 看不到这些计数。

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `llm_async_gateway_queue_depth` | gauge | `pool`, `tier` | 就绪队列长度。`tier` 为 `interactive`、`async`、`batch` |
| `llm_async_gateway_claimed` | gauge | `pool` | 当前租约中的请求数 |
| `llm_async_gateway_inflight` | gauge | `pool`, `tier` | Redis 共享 in-flight 持有者。崩溃进程的租约过期后不再计入 |
| `llm_async_gateway_dispatch_budget` | gauge | `pool`, `tier`, `gate` | 剩余准入预算，范围 [0, 1]。Phase 1 的 `gate` 是 `local` |
| `llm_async_gateway_gate_decisions_total` | counter | `pool`, `tier`, `gate`, `reason` | `continue`、`refuse_local`、`refuse_shared` |
| `llm_async_gateway_attempts_total` | counter | `pool`, `tier`, `result` | `ok`、`retry`、`expired`、`cancelled`、`failed` |
| `llm_async_gateway_deadline_slack_seconds` | histogram | `pool`, `tier` | claim 时刻到 deadline 的秒数。负值表示已经过期 |
| `llm_async_gateway_queue_wait_seconds` | histogram | `pool`, `tier` | 入队到 claim 的等待秒数 |
| `llm_async_gateway_upstream_seconds` | histogram | `pool`, `tier`, `code` | 单次上游 HTTP 耗时 |
| `llm_async_gateway_batch_jobs` | gauge | `status` | Redis 中各状态的 batch 作业数 |
| `llm_async_gateway_tokens_total` | counter | `pool`, `tier`, `direction` | 上游 `usage` 里的 token。`direction` 为 `prompt` 或 `completion` |

`docker compose` 把两个 API 分别映射到 `8080` / `8081`，两个 dispatcher 映射到 `8082` / `8083`，两个 controller 映射到 `8084` / `8085`。全局 in-flight 要把两个 dispatcher 的计数相加，或者直接读任意一个进程上的 `llm_async_gateway_inflight`（它读的是同一份 Redis）。

## PromQL

队列深度（async 与 batch 分开看）：

```promql
sum by (tier) (llm_async_gateway_queue_depth{pool="default"})
```

共享 in-flight，以及它是否顶到配置的 `MAX_CONCURRENCY`。Phase 1 没有把上限做成指标，把下面的 `4` 换成实际的 `MAX_CONCURRENCY`：

```promql
sum(llm_async_gateway_inflight{pool="default"})
sum(llm_async_gateway_inflight{pool="default"}) / 4
```

尝试结果。`rate` 的窗口按抓取间隔调整；compose 演示用 `1m` 即可：

```promql
sum by (tier, result) (rate(llm_async_gateway_attempts_total{pool="default"}[1m]))
```

失败和过期占全部尝试的比例：

```promql
sum(rate(llm_async_gateway_attempts_total{result=~"failed|expired"}[5m]))
/
sum(rate(llm_async_gateway_attempts_total[5m]))
```

Deadline slack。负桶表示 claim 时已经超过 deadline。`histogram_quantile` 需要把各 dispatcher 的桶先相加：

```promql
histogram_quantile(
  0.5,
  sum by (le, tier) (rate(llm_async_gateway_deadline_slack_seconds_bucket{pool="default"}[5m]))
)
```

已经过期才被领走的比例（`le="0"` 桶包含所有 slack ≤ 0 的观察）：

```promql
sum by (tier) (rate(llm_async_gateway_deadline_slack_seconds_bucket{le="0"}[5m]))
/
sum by (tier) (rate(llm_async_gateway_deadline_slack_seconds_count[5m]))
```

被共享闸门拒绝的速率（两个 dispatcher 都顶满时会出现）：

```promql
sum by (tier) (rate(llm_async_gateway_gate_decisions_total{reason="refuse_shared"}[1m]))
```

## Grafana

一个 dashboard 放四张图就覆盖 Phase 1 的退出条件：

1. **Queue depth**：上面的 `sum by (tier)`，按 `tier` 分色。
2. **In-flight**：`sum by (tier) (llm_async_gateway_inflight)`，再加一条常量阈值线等于 `MAX_CONCURRENCY`。两个 dispatcher 同时跑时，这条和不应超过该阈值。
3. **Attempts**：`sum by (result) (rate(llm_async_gateway_attempts_total[1m]))`。
4. **Deadline slack**：P50 / P90 的 `histogram_quantile`，单位秒。另加一条 slack ≤ 0 的比例。

抓取配置里把每个进程的 `/metrics` 当成独立 target，用 `role` 标签区分 `gateway-api`、`dispatcher`、`batch-controller`。计数器只在产生过该标签的进程上增加；`sum()` 会把副本加总。

## Trace

`POST /v1/requests` 在 API 进程生成 W3C `traceparent`，放进队列消息的 metadata（不会回写到客户端可见的 `metadata`）。Dispatcher claim 之后把同一个 trace id 传到上游 HTTP 的 `traceparent` 头。一条请求的 span 依次是 `http.serve`、`dispatch.claim`、`dispatch.upstream`、`dispatch.ack`。Batch 路径还有 `batch.validate`、`batch.enqueue`、`batch.finalize`。

Phase 1 把 span 打到进程日志，没有接 OTLP exporter。
