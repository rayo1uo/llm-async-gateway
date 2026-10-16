"""Assemble the versioned forecast snapshot.

The snapshot forecasts protected demand P. C and B are taken from the last
usable observation plus explicit windows. Serving occupancy is copied into
``observed`` for the consumer and is not part of P or of A.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import pandas as pd

from capacity_forecast import SCHEMA_VERSION
from capacity_forecast.budget import CapacityWindow, resolve_inputs, serving_budgets
from capacity_forecast.cleaning import infer_step, prepare_pool
from capacity_forecast.horizons import (
    Tier,
    default_tiers,
    duration_seconds,
    forecast_offsets,
    resolve_tiers,
    season_lengths,
)
from capacity_forecast.modeling import (
    PUBLISHED_MODELS,
    model_metadata,
    predict_protected,
    select_seasons,
)
from capacity_forecast.schema import (
    BUDGET_FORMULA,
    BUDGET_LOWER_FORMULA,
    BUDGET_MEDIAN_FORMULA,
    DISCLAIMER,
    EXAMPLE_DATA_ORIGIN,
    PLACEMENT_NOT_HANDLED,
    POOL_KEYS,
    validate_snapshot,
)
from capacity_forecast.sources import HistoryTable


def isoformat_z(timestamp: pd.Timestamp | None) -> str | None:
    if timestamp is None:
        return None
    stamp = pd.Timestamp(timestamp)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    stamp = stamp.floor("s") if stamp.nanosecond == 0 and stamp.microsecond == 0 else stamp
    text = stamp.isoformat()
    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    return text


def _observation(value: float | None, timestamp: pd.Timestamp | None) -> dict:
    if value is None or not math.isfinite(float(value)):
        return {"value": None, "timestamp": None}
    return {"value": float(value), "timestamp": isoformat_z(timestamp)}


@dataclass
class ForecastConfig:
    quantile: float = 0.95
    buffer: float = 0.0
    horizon: pd.Timedelta = pd.Timedelta(hours=24)
    max_gap_fill: int = 2
    step: pd.Timedelta | None = None
    tiers: list[Tier] | None = None
    model: str = "mstl"
    windows: list[CapacityWindow] = field(default_factory=list)
    honor_announced_at: bool = False

    def published_model_key(self) -> str:
        if self.model not in PUBLISHED_MODELS:
            raise ValueError(
                f"model must be one of {PUBLISHED_MODELS}; AutoETS is backtest-only"
            )
        return self.model


def _scope_dict(key: tuple) -> dict[str, str]:
    if not isinstance(key, tuple):
        key = (key,)
    return {name: str(value) for name, value in zip(POOL_KEYS, key, strict=True)}


def _split_pools(table: HistoryTable) -> list[tuple[dict[str, str], pd.DataFrame]]:
    frame = table.frame
    grouped = frame.groupby(list(POOL_KEYS), sort=True, dropna=False)
    pools: list[tuple[dict[str, str], pd.DataFrame]] = []
    for key, group in grouped:
        pools.append((_scope_dict(key), group.reset_index(drop=True)))
    if not pools:
        raise ValueError("history has no rows")
    return pools


def _example_data(group: pd.DataFrame) -> bool:
    if "data_origin" not in group.columns:
        return False
    origins = set(group["data_origin"].astype(str))
    return origins == {EXAMPLE_DATA_ORIGIN}


def build_pool_snapshot(
    scope: dict[str, str],
    group: pd.DataFrame,
    *,
    config: ForecastConfig,
    pending_in_target: bool,
    now: pd.Timestamp,
    as_of: pd.Timestamp | None = None,
) -> dict:
    model_key = config.published_model_key()
    cutoff = pd.Timestamp(group["timestamp"].max() if as_of is None else as_of)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    step = config.step or infer_step(group.loc[group["timestamp"] <= cutoff, "timestamp"])
    prepared = prepare_pool(
        group,
        as_of=cutoff,
        step=step,
        max_gap_fill=config.max_gap_fill,
        pending_in_target=pending_in_target,
    )
    daily, weekly = season_lengths(prepared.step)
    seasons, weekly_included = select_seasons(len(prepared.protected_demand), daily, weekly)
    base_seconds = duration_seconds(prepared.step)
    horizon_seconds = duration_seconds(config.horizon)
    tiers, tier_notes = resolve_tiers(config.tiers or default_tiers(), base_seconds, horizon_seconds)
    offsets = forecast_offsets(tiers)
    horizon_steps = offsets[-1][0] // base_seconds
    predicted = predict_protected(
        prepared.timestamps,
        prepared.protected_demand,
        step=prepared.step,
        horizon_steps=horizon_steps,
        seasons=seasons,
        model_keys=[model_key],
        quantile=config.quantile,
    )
    model_name = next(iter(predicted))
    forecast = predicted[model_name]
    wanted = [prepared.as_of + pd.Timedelta(seconds=offset) for offset, _ in offsets]
    inputs = resolve_inputs(
        prepared.baseline,
        config.buffer,
        config.windows,
        wanted,
        known_at=prepared.as_of,
        honor_announced_at=config.honor_announced_at,
    )
    wanted_index = pd.DatetimeIndex(wanted)
    if wanted_index.tz is None:
        wanted_index = wanted_index.tz_localize("UTC")
    aligned = forecast.reindex(wanted_index)
    if aligned["p50"].isna().any():
        raise ValueError(
            "model forecast does not cover the requested horizon "
            f"(first model timestamp {forecast.index[0]}, "
            f"first requested {wanted_index[0]})"
        )
    points = []
    for (offset, step_seconds), stamp, capacity_input, (_, row) in zip(
        offsets, wanted_index, inputs, aligned.iterrows(), strict=True
    ):
        p50 = float(row["p50"])
        upper = float(row["upper"])
        median, lower = serving_budgets(capacity_input.capacity, p50, upper, capacity_input.buffer)
        point = {
            "timestamp": isoformat_z(stamp),
            "horizon_seconds": int(offset),
            "step_seconds": int(step_seconds),
            "protected_demand_p50": p50,
            "protected_demand_upper": upper,
            "quantile": float(config.quantile),
            "capacity": float(capacity_input.capacity),
            "buffer": float(capacity_input.buffer),
            "budget_median": median,
            "budget_lower": lower,
        }
        if capacity_input.reason is not None:
            point["override_reason"] = capacity_input.reason
        points.append(point)
    notes = list(prepared.notes) + list(tier_notes)
    if not pending_in_target:
        notes.append(
            "protected_pending column is absent, so P = protected_allocated only. "
            "Include the column, with explicit zeros where pending is really zero, "
            "when saturation can truncate protected_allocated"
        )
    if not weekly_included:
        notes.append(
            f"weekly season length {weekly} does not fit in {len(prepared.protected_demand)} "
            "training points; using the daily season only"
        )
    if prepared.report["imputed_ffill_points"]:
        notes.append(
            f"forward-filled {prepared.report['imputed_ffill_points']} short gaps; "
            "failed scrapes were not treated as zero occupancy"
        )
    metadata = model_metadata(
        model_name,
        seasons,
        config.quantile,
        weekly_included=weekly_included,
    )
    # SeasonalNaive repeats one season (the longest that fits). Say so.
    if model_name == "SeasonalNaive":
        metadata["seasonal_naive_season_length"] = int(seasons[-1])
        notes.append(
            "SeasonalNaive repeats the longest season that fits "
            f"({seasons[-1]} steps) so a weekly cycle keeps the daily shape inside it"
        )
    quality = dict(prepared.report)
    quality["training_start"] = isoformat_z(prepared.timestamps[0])
    quality["training_end"] = isoformat_z(prepared.timestamps[-1])
    quality["weekly_seasonality_included"] = weekly_included
    quality["notes"] = notes
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": {key: scope[key] for key in POOL_KEYS},
        "as_of": isoformat_z(prepared.as_of),
        "generated_at": isoformat_z(now),
        "model": metadata,
        "horizon": {
            "base_step_seconds": base_seconds,
            "until_seconds": horizon_seconds,
            "tiers": [
                {
                    "from_seconds": tier.from_seconds,
                    "until_seconds": tier.until_seconds,
                    "step_seconds": tier.step_seconds,
                }
                for tier in tiers
            ],
            "notes": tier_notes,
        },
        "points": points,
        "observed": {
            "protected_demand": _observation(prepared.observed_protected, prepared.observed_protected_at),
            "protected_allocated": {"value": prepared.observed_protected_allocated},
            "protected_pending": {"value": prepared.observed_protected_pending},
            "serving_allocated": _observation(prepared.serving_allocated, prepared.serving_allocated_at),
            "reclaimable_allocated": _observation(
                prepared.reclaimable_allocated, prepared.reclaimable_allocated_at
            ),
            "capacity": {
                "value": prepared.baseline.effective,
                "timestamp": isoformat_z(prepared.baseline.timestamp),
                "healthy_capacity": prepared.baseline.healthy_capacity,
                "known_reservations": prepared.baseline.known_reservations,
                "maintenance_capacity": prepared.baseline.maintenance_capacity,
            },
        },
        "quality": quality,
        "semantics": {
            "target": "protected_demand",
            "protected_demand": "protected_allocated + protected_pending when pending is present",
            "capacity": "C(t)=max(healthy_capacity(t)-known_reservations(t)-maintenance_capacity(t),0)",
            "budget_formula": BUDGET_FORMULA,
            "budget_lower_formula": BUDGET_LOWER_FORMULA,
            "budget_median_formula": BUDGET_MEDIAN_FORMULA,
            "total_budget_includes_current_serving": True,
            "serving_allocated_excluded_from_protected_demand": True,
            "statistical_estimate_not_sla": True,
            "placement_not_handled": list(PLACEMENT_NOT_HANDLED),
        },
        "disclaimer": DISCLAIMER,
    }


def build_snapshot_document(
    table: HistoryTable,
    config: ForecastConfig,
    *,
    now: pd.Timestamp | None = None,
    as_of: pd.Timestamp | None = None,
    pool_filter: dict[str, str] | None = None,
) -> dict:
    """Forecast every pool in ``table`` and wrap the snapshots in one document."""
    generated = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if generated.tzinfo is None:
        generated = generated.tz_localize("UTC")
    else:
        generated = generated.tz_convert("UTC")
    pools = []
    example_flags = []
    for scope, group in _split_pools(table):
        if pool_filter and any(scope.get(key) != value for key, value in pool_filter.items()):
            continue
        example_flags.append(_example_data(group))
        pools.append(
            build_pool_snapshot(
                scope,
                group,
                config=config,
                pending_in_target=table.pending_in_target,
                now=generated,
                as_of=as_of,
            )
        )
    if not pools:
        raise ValueError("pool filter matched no rows")
    document = {
        "schema_version": SCHEMA_VERSION,
        "kind": "capacity_forecast_snapshot_set",
        "generated_at": isoformat_z(generated),
        "example_data": all(example_flags),
        "disclaimer": DISCLAIMER,
        "pools": pools,
    }
    validate_snapshot(document)
    return document
