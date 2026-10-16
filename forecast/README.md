# 容量预测（capacity forecast）

一个很小的 CPU 侧 Python 工作器。它读取资源池的历史容量指标，对**受保护需求**做时间序列预测，并写出一份带版本号的预算快照。以后的 Brain / Planner 可以读这份快照，决定要为异步和批请求留多少 GPU。这一版**不接** Go 网关，也**不做**扩缩容。

预测的不是 GPU 利用率，也不是 `free_gpu`。每个池的服务资源预算是：

```text
A(t) = max(C(t) - P(t) - B(t), 0)
```

| 符号 | 含义 | 这一版怎么得到 |
|---|---|---|
| `C(t)` | 池的有效健康容量 | 已知输入。用截止时刻之前最后一条可用观测，再加上显式的时间窗覆盖（预留、维护、节点排空） |
| `P(t)` | 不能让给本服务系统的受保护需求 | **唯一被预测的量** |
| `B(t)` | 平台预留 / 安全缓冲 | 已知输入。命令行默认值，或时间窗里的覆盖 |
| `A(t)` | 服务资源预算 | 由上面三项推导，不单独拟合 |

池的键是 `cluster × node_pool × gpu_type × resource_policy`。

`A` 是池的**总预算**，含本服务系统已经占着的 GPU。消费方自己减去当前 serving 占用。本系统的 `serving_allocated` 是单独的已知输入，**不进入 `P`**，避免规划器把自己的占用再预测一遍、形成反馈环。

受保护需求：

```text
P(t) = protected_allocated + protected_pending    （pending 列存在时）
P(t) = protected_allocated                        （整列缺失时）
```

集群打满时，已经跑起来的 `protected_allocated` 会被截断，排队的部分在 `protected_pending` 里。所以只要这一列存在，就要加进去。单元格为空是「不知道」，不是 0。整列都没有，才表示生产者没有这个指标，此时不把 pending 算进 `P`。

有效容量（三个量必须互不重叠；已经离开 `healthy_capacity` 的节点不要再记进 `maintenance_capacity`）：

```text
C(t) = max(healthy_capacity(t) - known_reservations(t) - maintenance_capacity(t), 0)
```

保守预算和中位预算（`q = 1 - α`，默认 `q = 0.95`）：

```text
A_lower(t)  = max(C(t) - Q_q(P(t)) - B(t), 0)
A_median(t) = max(C(t) - Q_0.50(P(t)) - B(t), 0)
```

算例：`C = 256`，`P50 = 144`，`P95 = 184`，`B = 8`，则中位预算 `104`，保守预算 `64`。任何一边小于 0 都钳成 0。

这些数是统计估计，**不是 SLA**，也不是截止时间一定能完成的保证。下面这些放置问题这里都不处理，消费方必须自己检查：

- GPU 在节点上的碎片
- 整机 / 拓扑约束
- 回收延迟
- 队列配额

## 明确不做的事

- 不扩缩容，不改副本数，不调用 Kubernetes
- 不改 admission，不写账本，不碰 Redis 队列
- 不承接推理请求
- 不把快照接进 Go planner / Brain（当前仓库里的 `internal/budget` 仍是进程内并发闸门）
- 不做 MLForecast、LightGBM、AutoGluon、Crane
- 不把本系统的 serving 占用当作预测目标

## 数据合约

输入是 CSV 或 Parquet，一行一个时间点、一个池。推荐时间戳用 UTC 的 ISO-8601（`2026-01-05T00:00:00Z`）。也接受 Unix 秒 / 毫秒 / 纳秒。

| 列 | 必需 | 含义 |
|---|---|---|
| `timestamp` | 是 | 观测时刻 |
| `cluster` | 是 | 池键 |
| `node_pool` | 是 | 池键 |
| `gpu_type` | 是 | 池键 |
| `resource_policy` | 否 | 池键。缺列时当作空字符串 |
| `healthy_capacity` | 是 | 池里健康 GPU 数，尚未扣预留和维护 |
| `protected_allocated` | 是 | 已占用、且不能让出的 GPU |
| `protected_pending` | 否 | 受保护但还在排队的 GPU。有此列时进入 `P` |
| `reclaimable_allocated` | 是 | 可以回收的占用。只记录，不进入 `P` |
| `serving_allocated` | 是 | **本服务系统**已经拿着的 GPU。不进入 `P`，也不从 `A` 里扣 |
| `known_reservations` | 是 | 此刻生效、要从 `C` 里扣掉的预留 |
| `maintenance_capacity` | 是 | 此刻仍算在 `healthy_capacity` 里、但要为维护/排空扣掉的 GPU |
| `data_quality` | 是 | 只有 `ok` 的数字可以进模型 |
| `data_origin` | 否 | 合成样例写 `synthetic_example`。生产数据不要填这个值 |

`data_quality` 不是 `ok`（例如 `scrape_failed`、`missing`、`gap`、`partial`、`collection_error`）时，这一行上的数字全部作废，**即使它们是 0**。采集失败写成 0 是缺口，不是空池。

另外只把下面这些当成采集错误扔掉，其余都保留：

- 非数字的计数字段
- 任意计数字段为负
- 同一时间戳上互相矛盾的重复行

真实的突发（突然升高又落下）会原样送进模型，不做滚动平滑、不做截尾。

### 缺口

网格是等间隔的。缺行、失败采集、空白 pending（当 pending 参与 `P` 时）都是缺口。

- 短缺口（默认不超过 2 个步长，`--max-gap-fill`）用**前值填充**。这只是「上次看到的需求还在」，不会把突发抹平，也不会把缺口填成 0。
- 更长的中间缺口不填充。训练序列从最后一个这样的缺口之后重新开始，缺口之前的历史丢掉。
- 序列尾巴上的长缺口直接报错。静默把旧需求一直带到 `as_of`，会让快照看起来很新、依据却是一次中断。若确实要延长前值填充，显式加大 `--max-gap-fill`。
- 填充只用过去的值，不用未来的值。

`C` 不预测。截止时刻之前最后一条可用的 `(healthy, reservations, maintenance)` 当成基线；预测区间里的变化只来自覆盖文件。

覆盖文件（`--overrides`）示例：

```json
{
  "buffer_default": 8,
  "windows": [
    {
      "start": "2026-04-02T00:00:00Z",
      "end": "2026-04-02T06:00:00Z",
      "healthy_capacity": 240,
      "known_reservations": 0,
      "maintenance_capacity": 16,
      "buffer": 12,
      "reason": "node drain",
      "announced_at": "2026-04-01T00:00:00Z"
    }
  ]
}
```

窗口是左闭右开。未写的字段保持基线。重叠时文件里靠后的窗口胜出。命令行 `--buffer` 优先于 `buffer_default`；两边都没给时 `B = 0`（不替平台发明一个缓冲）。

工作器把文件里的窗口都当成「现在已知」。回测会看 `announced_at`：晚于该 origin 才宣布的窗口不会被用到。没有 `announced_at` 的窗口视为一直已知。

## 快照

工作器为每个池写一份快照，外面再包一层：

```json
{
  "schema_version": "capacity-forecast.v1",
  "kind": "capacity_forecast_snapshot_set",
  "generated_at": "2026-04-02T00:01:00Z",
  "example_data": false,
  "disclaimer": "...",
  "pools": [ { "scope": {}, "as_of": "...", "model": {}, "horizon": {}, "points": [] } ]
}
```

- `as_of` 是**数据截止时间**，不是生成时间。生成时间在 `generated_at`。
- `model.quantile` 是 `1 - α`。默认 `0.95`，对应的中心区间 level 是 90：这些基线的原生区间是对称的，`hi-90` 就是模型的 P95，点预测是区间中心，在这个对称模型下也就是 P50。
- 每个点有 `protected_demand_p50`、`protected_demand_upper`（即 `Q_(1-α)`）、`capacity`、`buffer`、`budget_median`、`budget_lower`。
- 需求预测会钳在 0 以上。上分位若低于 P50，会抬到 P50，避免「保守预算」反而比中位预算更大。
- `observed.serving_allocated` 是截止时刻前最后一条可用观测，给消费方自己扣减。它不是预测目标。值为 `null` 时表示不知道，不是 0。
- `example_data: true` 表示输入带了 `data_origin=synthetic_example`。

`disclaimer` 会重复上面的非 SLA、放置可行性、总预算含当前 serving 这几件事，避免只写在 README 里。

### 模型

发布用的模型只有两个，由 `--model` 选择，默认 `mstl`：

- `SeasonalNaive`：重复最长的、训练长度装得下的季节。周季节装得下时用周（5 分钟网格上是 2016），这样一天中的形状还在那一周里。装不下就退到日季节（288）。
- `MSTL`：日季节和周季节一起分解（同样是 288 和 2016，或由步长推出的 `[每天的点数, 每周的点数]`）。趋势用库默认的非季节 AutoETS。MSTL 内部对每个季节分量仍是 SeasonalNaive。

步长必须整除 1 个 UTC 日，季节长度才是整数。季节按 **UTC** 的日和周计算。若业务周期是某个本地时区，先把时间戳换算好再写入；这一版没有时区参数。

区间来自 statsforecast 的原生等尾区间，不是分位数回归，也不是 conformal。残差不对称或突发很重时，P95 的经验覆盖率可以明显偏离 95%。回测就是用来看这件事的。覆盖率接近 95% 也仍然不是 SLA。

AutoETS 只出现在回测里，作为对照，**不会**写进快照。日季节长度超过 48（5 分钟网格是 288）时默认跳过，因为季节 ETS 的状态和信息准则搜索会重很多。确认要跑再加 `--autoets-force`。

### 预测范围

默认档位是初始建议，**没有用真实流量验证过**：

| 距离 `as_of` | 步长 |
|---|---|
| 0–30 分钟 | 5 分钟 |
| 30 分钟–6 小时 | 5 分钟 |
| 6–24 小时 | 15 分钟 |
| 24 小时–7 天 | 1 小时 |

默认地平线是 24 小时，所以最后一档只有在 `--horizon` 长于 24 小时时才会出现。0–30 分钟的「1–5 分钟」取了 5 分钟，和 288/2016 的例子一致。历史网格更粗时，档位步长会被抬到网格上（1 小时的历史不会被插值成 5 分钟）。更细的步长只有在输入本身更细时才有意义。用 `--tiers` 指向一个 JSON 文件可以换掉默认档位：

```json
{
  "tiers": [
    {"until_seconds": 1800, "step_seconds": 300},
    {"until_seconds": 21600, "step_seconds": 300},
    {"until_seconds": 86400, "step_seconds": 900}
  ]
}
```

地平线必须是历史步长的整数倍。

## 怎么跑

在 `forecast/` 目录：

```bash
python -m pip install -e ".[dev]"

# 合成样例。这不是任何集群的真实指标。
python -m capacity_forecast.synthetic --output examples/synthetic_history.csv --step 5min --days 21

python -m capacity_forecast.worker \
  --history examples/synthetic_history.csv \
  --output examples/snapshot.json \
  --model mstl \
  --quantile 0.95 \
  --buffer 8 \
  --horizon 24h

python -m capacity_forecast.backtest \
  --history examples/synthetic_history.csv \
  --output examples/backtest.json \
  --horizon 24h \
  --min-train 14d \
  --origin-step 1d \
  --buffer 8
```

`examples/` 已在仓库 `.gitignore` 里。5 分钟、三周的 MSTL 拟合大约是十几秒一次；回测每个 origin 都会重拟合。测试用的是小时网格，避免 CI 被 5 分钟序列拖住。

常用参数：

| 参数 | 默认 | 含义 |
|---|---|---|
| `--model` | `mstl` | `mstl` 或 `seasonal_naive` |
| `--quantile` | `0.95` | 受保护需求的上分位，即 `1-α` |
| `--buffer` | `0` | 平台缓冲 `B` |
| `--horizon` | `24h` | 预测多远 |
| `--step` | 从数据推断 | 网格步长，如 `5min`、`1h` |
| `--max-gap-fill` | `2` | 最多前值填充几个步长 |
| `--as-of` | 文件里最晚的时间 | 数据截止。更晚的行会被忽略 |
| `--overrides` | 无 | `C` / `B` 的时间窗 |
| `--autoets` | 关 | 仅回测。多一个 AutoETS 对照 |

工作器只读文件、只写 JSON。可选的 Prometheus 适配器是库函数，不是默认路径：`PrometheusHistorySource` 对每个计数字段做一次 `/api/v1/query_range`，PromQL 需要已经聚合成一个池。缺采样点会落成 `data_quality=gap`，不会填 0。查询名是调用方给的，本仓库没有约定真实的指标名。

## 回测

滚动起点：对每个 origin，只用 `timestamp <= origin` 的行做清洗和拟合，再拿 origin 之后的**真实观测**给 P50 误差和上分位覆盖率打分。

- 缺口结果不打分，也不当成 0。
- `mean_error = 实际 P - P50`。正数表示把受保护需求估低了，预算会偏乐观。
- `upper_coverage` 是 `实际 P <= Q_(1-α)` 的比例。名义上应接近 `--quantile`（默认 0.95）。
- 每个预测步长各有一行，另外有一个 `overall`。
- 合成数据里的矩形突发是故意留下的。对称的正态区间经常罩不住它们，覆盖率低于 95% 是预期中的压力，不是把突发平滑掉的理由。

回测报告的 `schema_version` 是 `capacity-forecast-backtest.v1`。

## 测试

```bash
cd forecast && python -m pytest -q
```

仓库根上的 `make ci` 仍然只跑 Go。`make forecast-test` 单独跑这个包。GitHub Actions 里 Go 和这个包是两个 job，Python 失败不会改 Go 的测试命令。

## 已知限制

- 只预测 `P`。`C` 和 `B` 不会从历史里外推；没写进覆盖文件的未来维护，快照看不到。
- P50 / P95 是原生对称区间的中心和上端点，不是单独训练的分位数模型。突发多、分布歪时要看回测覆盖率，不要假设它已经校准到 95%。
- 季节是 UTC 的日和周。没有本地时区，没有年季节。
- 训练序列在长缺口之后被截断，缺口前的历史不再使用。
- 不处理碎片、拓扑、回收延迟、队列配额，也不保证异步请求能在 deadline 前跑完。
- 文件是唯一被要求的输入。Prometheus 适配器不会发现多条序列，也不会重试。
- 没有和 Go planner 的接口。快照只是一份 JSON。
