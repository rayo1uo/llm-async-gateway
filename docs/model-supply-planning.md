# 模型供给规划

本文是规划器的现行规格，取代调研笔记 §5.3 的伪代码。实现是 `internal/supply.Plan`：纯函数，同一份输入永远得到同一份输出。它不进网关的请求路径，不读 Redis，不发 HTTP。

执行面（actuator）和放置（placement）在本仓库之外。规划器只回答：在当前预算下，每个模型在每种 GPU 上应该有多少个副本。

## 不在范围内

- 不调用 Kubernetes，不修改 Deployment，不调度 Pod，不抢占。
- 不实现异构 MILP / Mélange。只有下面这一条快速路径。
- 不平滑泄洪需求，也不把「刚决定加上的副本」当成已经在服务。

## 旧 §5.3 哪里会算错

旧伪代码不能按原文实现。对照如下。

| 旧写法 | 后果 | 现在 |
|---|---|---|
| `D_drain = Q / max(T - cold, ε)`，队列只有一个 deadline | `T ≤ cold` 时分母变成 ε，需求被放大到无穷，副本数跟着爆炸 | 只有 `T > cold` 的桶才有泄洪率；更紧的桶记入 `unsatisfiable_tokens`，不占预算 |
| 到达率和泄洪率一起做 EWMA | 泄洪被历史平均值拖住，赶不上当前直方图 | 只平滑到达率；泄洪每个 tick 用当前直方图重算 |
| `R_m = min over g of gpu_need[m,g]` | 无界需求被折成「跨类型 GPU 数的最小值」，换到更慢的类型就不够 | 无界需求一直是 tok/s；选定具体类型之后才算副本数 |
| 每个 GPU 类型各取一次 `reserved_min_gpus` | 预留被按类型重复计算 | 预留是模型级的 GPU 数，已经分出去的要扣掉 |
| 分配循环里立刻用新副本更新 φ，并按 φ 给所有模型各分一点 | 冷启动还没结束就算在服务；预算被拆开，两个本来能救一个的模型一起错过 deadline | 新副本只进入 warming；同 tier 里先整笔救能赶上的模型 |
| 普通 tick 的 cooldown 也套在预算收缩上 | 驱逐之后还要等冷却才缩容 | 预算变小或 footprint 已经超过 `B_g` 时立刻重算 |

## 输入

`Plan(Request) (Response, error)`。

- `Budgets`：每种 GPU 的 `B_g`，单位是 GPU 个数。调用方已经扣掉更高优先级的占用。
- 每个模型：
  - `Tier`：0 最高。
  - `Weight`：同 tier 内的权重。≤0 时按 1。
  - `ReservedMinGPUs`：预留下限，**GPU 个数**，跨类型合计，不是每种类型各一份。
  - `SoftMaxGPUs`：软顶，单位也是 GPU。负数表示不设顶；0 表示这个模型不能占卡。分配前用它截断需求。
  - 到达率：`ArrivalTokensPerSec`，或 `ArrivalRequestsPerSec` 乘以平均输入加输出 token。`EWMAAlpha ∈ (0,1]` 时与 `ArrivalEWMA`（上一拍的 tok/s）混合。alpha 为 0 则用本拍原始值。
  - 可选的引擎侧信号：`EngineTokensPerSec` 或请求速率，换算方式同上。
  - `Backlog`：直方图，不是整条队列一个 deadline。建议至少三档：30s 内、5 分钟内、更晚。每档带 `Remaining` 和 token 质量（`Tokens`，或 `Requests` × 平均输入加输出 token）。
  - `Supply`：该类型上已经 ready 的副本，以及 warming 批次和剩余 ETA。
  - `Profiles`：每种 GPU 的 `GPUsPerReplica`（`u`）、`MuTokensPerSec`（该形状下每副本 tok/s；也可以给 `MuRequestsPerSec` 再换算）、`ColdStart`。`Infeasible` 或 μ=0 的类型直接丢掉。
  - `Cooldown`、`LastPlanChange`、`MaxStep`：普通 tick 的滞后。`MaxStep` 为 0 表示不限步长；负数会被拒绝。
- `Epsilon`：泄洪窗口的下限。0 表示 1ms。
- `Horizon`：计算 φ 的时刻。0 表示取该模型可行类型里最长的冷启动（warming ETA 更长时用 ETA）。
- `Eviction` 或 `PreviousBudgetGPUs` 大于新预算的总和：视为预算收缩，绕过冷却和步长。

调用方如果拿到的是请求数而不是 token，必须同时给出平均输入和输出 token，规划器负责相乘。剖面 μ 是「这个形状下每副本的 tok/s」。

## 输出

每个模型：

- `TargetReplicas`：选定类型上的目标副本数。没选中的类型不出现。
- `DemandTokensPerSec`：截断之后的有效需求 `D_m`。
- `SmoothedArrivalTokensPerSec`、`DrainTokensPerSec`：两个分量，泄洪没有被 EWMA 混进去。
- `ReadyTokensPerSec`：计划里仍然保留的、**现在已经 ready** 的副本吞吐。
- `WarmingTokensPerSec`：计划保留的 warming 副本，加上本拍新决定启动的副本。新副本的 ETA 是所选类型的冷启动。
- `UnsatisfiableTokens`：靠扩容也赶不上的 token 质量。
- `Phi`：见下文。需求为 0 时响应里记 0。
- `ReservedDeficitGPUs`：预留没能落地的 GPU 数。凑不齐一整副本时显式报告，不悄悄少留。
- `Notes`：`reserved`、`reserved_deficit`、`reserved_rounded_up`、`triage`、`triage_unsavable`、`fairness`、`clamped_soft_max`、`unsatisfiable_backlog`、`cooldown_hold`、`step_clamped`、`deadline_bypass`。

另外有每种 GPU 的 `BudgetUsed`，以及 `EvictionReplan`。

## 需求

记可行类型里最短的冷启动为 `cold_min`。对直方图里每一档，token 质量为 `Q`，剩余时间为 `T`：

- `T ≤ cold_min`（或根本没有可行类型）：这一档不能靠扩容挽救。`Q` 全部计入 `unsatisfiable_tokens`，泄洪率为 0。禁止用 `max(T - cold, ε)` 把分母抬成 ε。
- `T > cold_min`：泄洪率 `Q / max(T - cold_min, ε)`。多档相加。

有效需求：

```text
D_m = max(平滑后的到达率, 可满足档的泄洪率之和, 引擎侧 tok/s)
```

分配之前，把 `D_m` 截到软顶在**最快可行类型**上能提供的吞吐：

```text
n_cap = floor(soft_max_gpus / u_fastest)
D_m   = min(D_m, n_cap * μ_fastest)
```

最快指每副本 μ 最高。这样一条坏信号不能在注水时占满整个集群。软顶为负数时不截断。

无界需求始终是 tok/s。不会先算成「各类型 GPU 需求的最小值」。副本数只在选中具体类型 `g` 之后出现。对一块还要由该类型新副本承担的速率：

```text
n = ceil(remaining_tokens_per_s / μ[g])
```

实现上是在这个类型上一次加一个副本，直到时间积分覆盖直方图（triage）或视界上的速率盖住 `D_m`（公平性）。从零开始、单一类型、没有已就绪副本时，公平性阶段的结果等于这个 `ceil`。已经 ready 的副本按整个 `T` 积分，不会再用这条公式把它们买第二遍。

## 分配

硬约束：模型 `m` 在类型 `g` 上的每个副本占用 `u[m,g]` 张卡，

```text
sum_m x[m,g] * u[m,g] ≤ B_g
```

装不下模型的类型（剖面标记不可行，或 μ=0，或 `u` 大于剩余预算 / 软顶）不参与。

顺序：

1. **预留。** 所有模型按 tier 从高到低，同 tier 内权重大的在前。预留是 GPU 数。某个类型上已经为这个模型留下的卡要从剩余预留里扣掉，不能对每个类型各留一份。`u` 不能整除时，仅当剩余预算和软顶都放得下一整副本，才向上取整；否则停在已经放下的副本，并把差额写入 `ReservedDeficitGPUs`。放置时优先占用该类型上已经 ready 的副本，其次是 warming，再按每张卡的 tok/s 从高到低。这样不会把正在服务的副本换成一次新的冷启动。
2. **Triage，按 tier 从高到低。** 不是把预算按权重分给所有模型。在一个 tier 内，只给「把剩余预算全部给它，可满足的积压能在各自 deadline 前排完」的模型加副本，并且一次加到刚刚够。加的顺序是 deadline 更紧的优先，其次权重大。即使把剩余预算全部给它也赶不上的模型，除了预留之外一个副本都不再给。不把预算切成两半让两边都错过。
3. **φ 的加权 max-min，仍在这个 tier 内，并且发生在该 tier 的 triage 之后、下一个 tier 的 triage 之前。** 高 tier 把到达率补到 φ≥1 之后，低 tier 才能用剩下的卡去做泄洪。低 tier 的预留不受影响，因为预留在第 1 步已经占住。

φ 的定义：

```text
φ_m = rate(H) / D_m
```

`rate(H)` 是时刻 `H` 已经在服务的 tok/s：ready 副本从 0 时刻起算，warming 和新副本只在各自 ETA 之后算。`H` 默认是该模型最长冷启动，所以新副本要等冷启动结束才进入 φ，不会在 `t = 0` 被当成已就绪。`D_m = 0` 的模型不参加这一步。每一步把一个副本给当前 `φ / weight` 最小、并且还能再放一个副本的模型。副本所在类型选每张卡 tok/s 最高、并且在 `H` 之前能够就绪的类型。

「能排完」看的是时间积分，不是把新副本的 μ 乘上整个窗口。把可满足的档按 deadline 从早到晚累加，时刻 `T` 之前产出的 token 必须不少于这些档的 token 之和。每个副本只在自己的 ETA 之后产出：

- 现在 ready，且计划保留：整个 `[0, T]`。
- 已有 warming，ETA 为 `η`：只有 `T > η` 的那一段。
- 本拍新加的副本：只有 `T > cold_start[g]` 的那一段。

## 滞后和驱逐

普通 tick 先算出不考虑滞后的理想计划，再向当前 footprint（ready + warming）收敛：

- 还在 `Cooldown` 内：保持当前副本数。
- 否则若 `MaxStep > 0`：每次在差距最大的类型上加或减一个副本，一共最多 `MaxStep` 步。0 表示直接采用理想计划。

两种情况同时绕过冷却和步长：

1. 预算收缩。`Eviction` 为真，或 `PreviousBudgetGPUs` 大于新预算总和，或当前 footprint 在某个类型上已经超过 `B_g`。footprint 已经超预算时，从当前副本往下砍（见下）。只是标记了收缩、但 footprint 仍放得进新预算时，直接采用理想计划。
2. 存在可满足的桶，而当前 ready / warming 的时间积分赶不上它的 deadline。再等一个冷却周期就会错过。

收缩本身：

- `sum(x · u) > B_g` 时立刻减，不等冷却。
- 每次拿掉一个副本。顺序是 tier 从低到高，然后超出预留的 GPU 数更多的优先，然后 φ 更高的优先。需求为 0 时，排序用的 φ 视为正无穷，让没有需求的超额副本先走；响应里的 `Phi` 仍写 0。
- 不低于本拍预留阶段实际放下的 GPU 数，除非总预算小于各模型预留之和。那时从低 tier 向上剥离预留，并报告 `ReservedDeficitGPUs`。
- 高 tier 的预留如果当前没跑起来，而低 tier 还占着超出预留的卡，先把这些卡要回来，再谈低 tier 的超额。
- 某个类型超预算、但总卡数仍够预留时，把该类型上的副本挪到还有余额的类型，而不是直接把预留吃掉。

缩进预算之后如果还有剩余，继续按上面的 triage 和 φ 分配。这一步只使用剩余预算，不会把刚减掉的副本原样加回已经满的类型。

## 和网关的关系

派发闸门仍然按请求数限制并发和速率。队列里的 deadline 直方图、剖面和 `B_g(t)` 由调用方组装后交给 `Plan`。示例见 `internal/supply` 的 `ExamplePlan`。

```bash
go test -race ./internal/supply/
```

仓库的 `make race` 还会跑网关测试；那些测试使用 miniredis。供给规划这一包不需要 Redis。
