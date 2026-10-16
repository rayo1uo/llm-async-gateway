"""Synthetic example history.

The series is generated, not collected from a cluster. It has a UTC daily
cycle, a weekend drop, and a few rectangular bursts. Bursts are left intact
on purpose. A handful of failed scrapes are written as zeros with
``data_quality=scrape_failed`` so a consumer that treats gaps as zero will
get the target wrong.

Do not use this file as a capacity plan.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from capacity_forecast.horizons import parse_duration
from capacity_forecast.schema import EXAMPLE_DATA_ORIGIN
from capacity_forecast.snapshot import isoformat_z

SYNTHETIC_BANNER = (
    "synthetic example data, not a real cluster trace; "
    "daily and weekly seasonality plus random bursts"
)


def generate_synthetic_history(
    *,
    start: str | pd.Timestamp = "2026-01-05T00:00:00Z",
    step: str | pd.Timedelta = "5min",
    days: float = 21,
    seed: int = 0,
    cluster: str = "example-cluster",
    node_pool: str = "example-pool",
    gpu_type: str = "example-gpu",
    resource_policy: str = "example-policy",
    healthy_capacity: float = 256.0,
) -> pd.DataFrame:
    """Build one pool of labeled synthetic history."""
    step_delta = pd.Timedelta(step)
    periods = int(pd.Timedelta(days=days) / step_delta)
    if periods < 3:
        raise ValueError("synthetic history needs at least 3 points")
    index = pd.date_range(pd.Timestamp(start), periods=periods, freq=step_delta)
    if index.tz is None:
        index = index.tz_localize("UTC")
    else:
        index = index.tz_convert("UTC")
    rng = np.random.default_rng(seed)
    hours = index.hour.to_numpy() + index.minute.to_numpy() / 60.0
    weekend = index.dayofweek.to_numpy() >= 5
    # Negative lobes are fine: demand is shifted up and clamped by saturation below.
    daily = 36.0 * np.sin(2.0 * np.pi * (hours - 8.0) / 24.0)
    weekly = np.where(weekend, -28.0, 12.0)
    base = 120.0 + daily + weekly
    bursts = np.zeros(periods, dtype=float)
    burst_count = max(4, periods // 400)
    for _ in range(burst_count):
        at = int(rng.integers(0, max(periods - 2, 1)))
        width = int(rng.integers(3, 8))
        bursts[at : at + width] += float(rng.uniform(45.0, 95.0))
    noise = rng.normal(0.0, 2.0, periods)
    desired = np.maximum(base + bursts + noise, 0.0)

    healthy = np.full(periods, float(healthy_capacity))
    reservations = np.full(periods, 8.0)
    maintenance = np.zeros(periods, dtype=float)
    maintenance_points = max(1, int(pd.Timedelta(hours=6) / step_delta))
    maintenance_at = periods // 2
    maintenance[maintenance_at : maintenance_at + maintenance_points] = 32.0
    effective = np.maximum(healthy - reservations - maintenance, 0.0)
    # When the pool is saturated, observed allocation stops at C and the rest
    # shows up as protected_pending. The target has to add them back.
    pending = np.maximum(desired - effective, 0.0)
    allocated = desired - pending

    serving = 25.0 + 20.0 * np.sin(2.0 * np.pi * hours / 24.0)
    serving = np.clip(serving, 0.0, None)
    reclaimable = np.full(periods, 4.0)
    quality = np.full(periods, "ok", dtype=object)

    # Failed scrapes stored as zeros. These are gaps, not an empty pool.
    for position in (periods // 5, periods // 5 + 1, periods // 3):
        if not 0 <= position < periods:
            continue
        quality[position] = "scrape_failed"
        allocated[position] = 0.0
        pending[position] = 0.0
        serving[position] = 0.0
        healthy[position] = 0.0
        reservations[position] = 0.0
        maintenance[position] = 0.0
        reclaimable[position] = 0.0

    # A negative count with quality ok is a collection error, not negative demand.
    broken = periods // 7
    if quality[broken] == "ok":
        allocated[broken] = -5.0

    return pd.DataFrame(
        {
            "timestamp": index,
            "cluster": cluster,
            "node_pool": node_pool,
            "gpu_type": gpu_type,
            "resource_policy": resource_policy,
            "healthy_capacity": healthy,
            "protected_allocated": allocated,
            "protected_pending": pending,
            "reclaimable_allocated": reclaimable,
            "serving_allocated": serving,
            "known_reservations": reservations,
            "maintenance_capacity": maintenance,
            "data_quality": quality,
            "data_origin": EXAMPLE_DATA_ORIGIN,
        }
    )


def write_history(frame: pd.DataFrame, path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    suffix = destination.suffix.lower()
    if suffix == ".parquet":
        frame.to_parquet(destination, index=False)
        return
    if suffix != ".csv":
        raise ValueError(f"unsupported output {destination.name}; use .csv or .parquet")
    exported = frame.copy()
    exported["timestamp"] = [
        isoformat_z(pd.Timestamp(value)) for value in exported["timestamp"]
    ]
    exported.to_csv(destination, index=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a synthetic protected-demand history file. Not real cluster data."
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--start", default="2026-01-05T00:00:00Z")
    parser.add_argument("--step", default="5min")
    parser.add_argument("--days", type=float, default=21)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--cluster", default="example-cluster")
    parser.add_argument("--node-pool", default="example-pool")
    parser.add_argument("--gpu-type", default="example-gpu")
    parser.add_argument("--resource-policy", default="example-policy")
    args = parser.parse_args(argv)
    try:
        frame = generate_synthetic_history(
            start=args.start,
            step=parse_duration(args.step),
            days=args.days,
            seed=args.seed,
            cluster=args.cluster,
            node_pool=args.node_pool,
            gpu_type=args.gpu_type,
            resource_policy=args.resource_policy,
        )
        write_history(frame, args.output)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"{SYNTHETIC_BANNER}: wrote {len(frame)} rows to {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
