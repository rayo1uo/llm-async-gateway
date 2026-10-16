"""Publish a capacity-forecast snapshot from a history file.

The process is a CPU-only batch job. It does not scale anything, does not
write an admission ledger, and does not serve inference.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from capacity_forecast.horizons import Tier, parse_duration
from capacity_forecast.schema import POOL_KEYS
from capacity_forecast.snapshot import ForecastConfig, build_snapshot_document, isoformat_z
from capacity_forecast.sources import FileHistorySource, load_windows


def _load_tiers(path: str | None) -> list[Tier] | None:
    if path is None:
        return None
    payload = json.loads(Path(path).read_text())
    tiers = []
    for raw in payload["tiers"]:
        tiers.append(Tier(until_seconds=int(raw["until_seconds"]), step_seconds=int(raw["step_seconds"])))
    return tiers


def dump_snapshot(document: dict, path: str | Path | None) -> None:
    text = json.dumps(document, indent=2, ensure_ascii=False) + "\n"
    if path is None:
        sys.stdout.write(text)
        return
    Path(path).write_text(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Forecast protected GPU demand and write a serving-budget snapshot. "
            "Does not scale, admit, or serve."
        )
    )
    parser.add_argument("--history", required=True, help="CSV or Parquet history file")
    parser.add_argument("--output", help="Snapshot JSON path. Defaults to stdout")
    parser.add_argument("--model", choices=("mstl", "seasonal_naive"), default="mstl")
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.95,
        help="Upper quantile of protected demand, 1-alpha. Default 0.95 (P95)",
    )
    parser.add_argument(
        "--buffer",
        type=float,
        default=None,
        help="Platform reserve B in GPUs. Default 0, or buffer_default from --overrides",
    )
    parser.add_argument("--horizon", default="24h", help="Forecast horizon, for example 24h or 48h")
    parser.add_argument("--step", default=None, help="History grid step. Default: inferred")
    parser.add_argument("--max-gap-fill", type=int, default=2)
    parser.add_argument("--as-of", default=None, help="Data cutoff. Default: latest timestamp")
    parser.add_argument("--overrides", default=None, help="JSON file of scheduled C and B windows")
    parser.add_argument("--tiers", default=None, help="JSON file of horizon tiers")
    parser.add_argument("--cluster", default=None)
    parser.add_argument("--node-pool", default=None)
    parser.add_argument("--gpu-type", default=None)
    parser.add_argument("--resource-policy", default=None)
    args = parser.parse_args(argv)
    try:
        table = FileHistorySource(args.history).load()
        windows, file_buffer = load_windows(args.overrides)
        if args.buffer is not None:
            buffer = args.buffer
        elif file_buffer is not None:
            buffer = file_buffer
        else:
            buffer = 0.0
        pool_filter = {
            key: value
            for key, value in (
                ("cluster", args.cluster),
                ("node_pool", args.node_pool),
                ("gpu_type", args.gpu_type),
                ("resource_policy", args.resource_policy),
            )
            if value is not None
        }
        config = ForecastConfig(
            quantile=args.quantile,
            buffer=buffer,
            horizon=parse_duration(args.horizon),
            max_gap_fill=args.max_gap_fill,
            step=None if args.step is None else parse_duration(args.step),
            tiers=_load_tiers(args.tiers),
            model=args.model,
            windows=windows,
            honor_announced_at=False,
        )
        as_of = None if args.as_of is None else pd.Timestamp(args.as_of)
        if as_of is not None and as_of.tzinfo is None:
            as_of = as_of.tz_localize("UTC")
        document = build_snapshot_document(
            table,
            config,
            as_of=as_of,
            pool_filter=pool_filter or None,
        )
    except (ValueError, OSError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    dump_snapshot(document, args.output)
    for pool in document["pools"]:
        scope = pool["scope"]
        label = "/".join(scope[key] for key in POOL_KEYS)
        print(
            f"{label} model={pool['model']['name']} as_of={pool['as_of']} "
            f"points={len(pool['points'])} "
            f"example_data={str(document['example_data']).lower()}",
            file=sys.stderr,
        )
        if document["example_data"]:
            print(
                "input is synthetic example data, not a real cluster trace",
                file=sys.stderr,
            )
        serving = pool["observed"]["serving_allocated"]["value"]
        print(
            f"  latest serving_allocated={serving} (not part of the forecast target); "
            f"generated_at={isoformat_z(pd.Timestamp(document['generated_at']))}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
