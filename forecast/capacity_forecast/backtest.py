"""Rolling-origin backtest of protected-demand forecasts.

Each origin is fit only on rows at or before that origin. Gap filling uses
the same rule as the worker, so it cannot see the future. Outcomes that are
gaps are skipped. They are not scored as zero occupancy.

The reported upper coverage is the fraction of realized protected demand at
or below Q_(1-alpha). For a calibrated 0.95 quantile that fraction is about
0.95. It is a diagnostic, not an SLA. Bursts in the synthetic example are
expected to stress the models' symmetric native intervals.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from capacity_forecast import BACKTEST_SCHEMA_VERSION
from capacity_forecast.cleaning import observed_protected_map, prepare_pool
from capacity_forecast.horizons import (
    default_tiers,
    duration_seconds,
    parse_duration,
    resolve_tiers,
    season_lengths,
)
from capacity_forecast.modeling import (
    MODEL_NAMES,
    include_autoets_model,
    predict_protected,
    select_seasons,
)
from capacity_forecast.schema import DISCLAIMER, EXAMPLE_DATA_ORIGIN, POOL_KEYS
from capacity_forecast.snapshot import ForecastConfig, _split_pools, isoformat_z
from capacity_forecast.sources import FileHistorySource, HistoryTable, load_windows


def score_points(
    predictions: dict[pd.Timestamp, tuple[float, float]],
    actuals: dict[pd.Timestamp, float],
) -> list[tuple[pd.Timestamp, float, bool]]:
    """Pair forecasts with real outcomes.

    ``predictions`` maps a timestamp to ``(p50, upper)``. Timestamps missing
    from ``actuals`` are gaps and are not appended, so they cannot become a
    zero in the error or the coverage.
    """
    scored: list[tuple[pd.Timestamp, float, bool]] = []
    for timestamp, (p50, upper) in predictions.items():
        if timestamp not in actuals:
            continue
        actual = float(actuals[timestamp])
        error = actual - float(p50)
        covered = actual <= float(upper) + 1e-9
        scored.append((pd.Timestamp(timestamp), error, covered))
    return scored


def _summarize(errors: list[float], covered: list[bool], quantile: float) -> dict | None:
    if not errors:
        return None
    array = np.asarray(errors, dtype=float)
    return {
        "n": int(len(errors)),
        "mae": float(np.mean(np.abs(array))),
        "rmse": float(np.sqrt(np.mean(np.square(array)))),
        "mean_error": float(np.mean(array)),
        "upper_coverage": float(np.mean(np.asarray(covered, dtype=float))),
        "target_coverage": float(quantile),
    }


def _origins(
    timestamps: pd.Series,
    step: pd.Timedelta,
    min_train: pd.Timedelta,
    horizon: pd.Timedelta,
    origin_step: pd.Timedelta,
) -> list[pd.Timestamp]:
    start = pd.Timestamp(timestamps.min())
    end = pd.Timestamp(timestamps.max())
    first = start + min_train
    last = end - horizon
    if first > last:
        raise ValueError(
            "not enough history for min_train plus horizon; "
            f"history {start.isoformat()} .. {end.isoformat()}"
        )
    if origin_step % step != pd.Timedelta(0):
        raise ValueError("origin step must be a multiple of the history step")
    steps = int(math.ceil((first - start) / step))
    cursor = start + steps * step
    origins: list[pd.Timestamp] = []
    while cursor <= last:
        origins.append(pd.Timestamp(cursor))
        cursor = cursor + origin_step
    if not origins:
        raise ValueError("no rolling origins fell inside the history")
    return origins


def _add(buckets: dict[int, dict[str, list]], horizon_seconds: int, error: float, covered: bool) -> None:
    bucket = buckets.setdefault(horizon_seconds, {"errors": [], "covered": []})
    bucket["errors"].append(error)
    bucket["covered"].append(covered)


def run_backtest(
    table: HistoryTable,
    config: ForecastConfig,
    *,
    min_train: pd.Timedelta,
    origin_step: pd.Timedelta,
    include_autoets: bool = False,
    autoets_force: bool = False,
) -> dict:
    """Compare SeasonalNaive and MSTL, optionally AutoETS, on each pool."""
    pool_reports = []
    notes: list[str] = []
    for scope, group in _split_pools(table):
        group = group.sort_values("timestamp")
        step = config.step or infer_step_from_group(group)
        origins = _origins(group["timestamp"], step, min_train, config.horizon, origin_step)
        actuals = observed_protected_map(group, table.pending_in_target)
        daily, weekly = season_lengths(step)
        model_keys = ["seasonal_naive", "mstl"]
        run_autoets, autoets_note = include_autoets_model(
            daily, enabled=include_autoets, force=autoets_force
        )
        if autoets_note:
            notes.append(f"{scope['cluster']}/{scope['node_pool']}: {autoets_note}")
        if run_autoets:
            model_keys.append("autoets")
        buckets: dict[str, dict[int, dict[str, list]]] = {MODEL_NAMES[key]: {} for key in model_keys}
        used_origins = 0
        skipped: list[str] = []
        base_seconds = duration_seconds(step)
        horizon_seconds = duration_seconds(config.horizon)
        _tiers, tier_notes = resolve_tiers(
            config.tiers or default_tiers(), base_seconds, horizon_seconds
        )
        # Score every base step out to the horizon. Tier steps only change
        # which timestamps the snapshot publishes.
        horizon_steps = horizon_seconds // base_seconds
        for origin in origins:
            past = group.loc[group["timestamp"] <= origin].copy()
            try:
                prepared = prepare_pool(
                    past,
                    as_of=origin,
                    step=step,
                    max_gap_fill=config.max_gap_fill,
                    pending_in_target=table.pending_in_target,
                )
                seasons, _ = select_seasons(len(prepared.protected_demand), daily, weekly)
                predicted = predict_protected(
                    prepared.timestamps,
                    prepared.protected_demand,
                    step=step,
                    horizon_steps=horizon_steps,
                    seasons=seasons,
                    model_keys=model_keys,
                    quantile=config.quantile,
                    allow_autoets=include_autoets,
                )
            except ValueError as exc:
                skipped.append(f"{isoformat_z(origin)}: {exc}")
                continue
            used_origins += 1
            for name, frame in predicted.items():
                keyed = {
                    pd.Timestamp(timestamp): (float(row.p50), float(row.upper))
                    for timestamp, row in frame.iterrows()
                }
                for timestamp, error, covered in score_points(keyed, actuals):
                    # Score every base step. Coarser snapshot tiers are a
                    # publication choice; the error curve is more useful dense.
                    horizon = int(round((timestamp - origin).total_seconds() / base_seconds))
                    if horizon < 1 or horizon > horizon_steps:
                        continue
                    _add(buckets[name], horizon * base_seconds, error, covered)
        if used_origins == 0:
            raise ValueError(f"every origin failed for pool {scope}: {skipped[:3]}")
        per_model = {}
        for name, by_horizon in buckets.items():
            horizons = []
            all_errors: list[float] = []
            all_covered: list[bool] = []
            for horizon_key in sorted(by_horizon):
                summary = _summarize(
                    by_horizon[horizon_key]["errors"],
                    by_horizon[horizon_key]["covered"],
                    config.quantile,
                )
                if summary is None:
                    continue
                summary["horizon_seconds"] = int(horizon_key)
                horizons.append(summary)
                all_errors.extend(by_horizon[horizon_key]["errors"])
                all_covered.extend(by_horizon[horizon_key]["covered"])
            per_model[name] = {
                "per_horizon": horizons,
                "overall": _summarize(all_errors, all_covered, config.quantile),
            }
        pool_reports.append(
            {
                "scope": {key: scope[key] for key in POOL_KEYS},
                "origins_requested": len(origins),
                "origins_used": used_origins,
                "skipped_origins": skipped,
                "models": per_model,
                "tier_notes": tier_notes,
            }
        )
    example = False
    if "data_origin" in table.frame.columns:
        example = set(table.frame["data_origin"].astype(str)) == {EXAMPLE_DATA_ORIGIN}
    return {
        "schema_version": BACKTEST_SCHEMA_VERSION,
        "example_data": example,
        "quantile": float(config.quantile),
        "alpha": 1.0 - float(config.quantile),
        "horizon_seconds": duration_seconds(config.horizon),
        "min_train_seconds": duration_seconds(min_train),
        "origin_step_seconds": duration_seconds(origin_step),
        "models": ["SeasonalNaive", "MSTL"] + (["AutoETS"] if any(
            "AutoETS" in report["models"] for report in pool_reports
        ) else []),
        "pools": pool_reports,
        "notes": notes + [
            "Each origin was fit only on observations at or before that origin.",
            "Gap outcomes were not scored and were not treated as zero.",
            "upper_coverage is the share of realized protected demand <= Q_(1-alpha). "
            "A calibrated quantile sits near its nominal probability. This is not an SLA.",
            "mean_error is actual minus P50. Positive means protected demand was under-predicted.",
            DISCLAIMER,
        ],
    }


def infer_step_from_group(group: pd.DataFrame) -> pd.Timedelta:
    from capacity_forecast.cleaning import infer_step

    return infer_step(group["timestamp"])


def dump_backtest(document: dict, path: str | Path | None) -> None:
    text = json.dumps(document, indent=2) + "\n"
    if path is None:
        sys.stdout.write(text)
        return
    Path(path).write_text(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Rolling-origin backtest of SeasonalNaive vs MSTL on a history file."
    )
    parser.add_argument("--history", required=True)
    parser.add_argument("--output")
    parser.add_argument("--quantile", type=float, default=0.95)
    parser.add_argument("--buffer", type=float, default=None)
    parser.add_argument("--horizon", default="24h")
    parser.add_argument("--step", default=None)
    parser.add_argument("--max-gap-fill", type=int, default=2)
    parser.add_argument("--min-train", default="14d")
    parser.add_argument("--origin-step", default="24h")
    parser.add_argument("--overrides", default=None)
    parser.add_argument(
        "--autoets",
        action="store_true",
        help="Also score AutoETS. Comparison only; the snapshot worker does not publish it.",
    )
    parser.add_argument(
        "--autoets-force",
        action="store_true",
        help=f"Run AutoETS even when the daily season exceeds {AUTOETS_SEASON_LIMIT} steps.",
    )
    args = parser.parse_args(argv)
    try:
        table = FileHistorySource(args.history).load()
        windows, file_buffer = load_windows(args.overrides)
        buffer = args.buffer if args.buffer is not None else (0.0 if file_buffer is None else file_buffer)
        config = ForecastConfig(
            quantile=args.quantile,
            buffer=buffer,
            horizon=parse_duration(args.horizon),
            max_gap_fill=args.max_gap_fill,
            step=None if args.step is None else parse_duration(args.step),
            windows=windows,
            honor_announced_at=True,
        )
        document = run_backtest(
            table,
            config,
            min_train=parse_duration(args.min_train),
            origin_step=parse_duration(args.origin_step),
            include_autoets=args.autoets,
            autoets_force=args.autoets_force,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    dump_backtest(document, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
