"""History inputs.

File CSV/Parquet is the required path. The Prometheus reader is a small
optional adapter behind the same ``HistorySource`` interface: one range
query per field, merged onto the requested grid. It does not scrape, and
a missing sample stays a gap.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from capacity_forecast.budget import CapacityWindow
from capacity_forecast.schema import COUNT_COLUMNS, REQUIRED_HISTORY_COLUMNS


@dataclass
class HistoryTable:
    """One history file, possibly containing several pools."""

    frame: pd.DataFrame
    pending_in_target: bool
    source: str


class HistorySource(Protocol):
    def load(self) -> HistoryTable:
        """Return the history contract, with gaps still marked as gaps."""


def _parse_timestamps(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        maximum = float(series.max())
        if maximum > 10**14:
            unit = "ns"
        elif maximum > 10**11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(series, unit=unit, utc=True)
    return pd.to_datetime(series, utc=True)


def normalize_history(frame: pd.DataFrame, *, source: str) -> HistoryTable:
    """Validate the file contract and coerce types without filling gaps."""
    missing = [column for column in REQUIRED_HISTORY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"history is missing columns {missing}")
    out = frame.copy()
    pending_in_target = "protected_pending" in out.columns
    if "resource_policy" not in out.columns:
        out["resource_policy"] = ""
    if not pending_in_target:
        out["protected_pending"] = math.nan
    out["timestamp"] = _parse_timestamps(out["timestamp"])
    if out["timestamp"].isna().any():
        raise ValueError("history has a timestamp that could not be parsed")
    for column in ("cluster", "node_pool", "gpu_type", "resource_policy", "data_quality"):
        out[column] = out[column].astype(str).str.strip()
    out["resource_policy"] = out["resource_policy"].replace({"nan": "", "None": ""})
    for column in COUNT_COLUMNS:
        # Leave coercion of bad tokens to the cleaner so it can flag them.
        # Here, only blank-like values become NaN when the dtype is already numeric.
        if pd.api.types.is_numeric_dtype(out[column]):
            out[column] = pd.to_numeric(out[column], errors="coerce")
    columns = [
        "timestamp",
        "cluster",
        "node_pool",
        "gpu_type",
        "resource_policy",
        *COUNT_COLUMNS,
        "data_quality",
    ]
    if "data_origin" in out.columns:
        columns.append("data_origin")
    return HistoryTable(
        frame=out.loc[:, columns].reset_index(drop=True),
        pending_in_target=pending_in_target,
        source=source,
    )


class FileHistorySource:
    """CSV or Parquet history. Suffix selects the format."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> HistoryTable:
        suffix = self.path.suffix.lower()
        if suffix == ".csv":
            frame = pd.read_csv(self.path)
        elif suffix == ".parquet":
            frame = pd.read_parquet(self.path)
        else:
            raise ValueError(f"unsupported history file {self.path.name}; use .csv or .parquet")
        return normalize_history(frame, source=str(self.path))


def load_windows(path: str | Path | None) -> tuple[list[CapacityWindow], float | None]:
    """Read scheduled C/B overrides. Returns windows and an optional default buffer."""
    if path is None:
        return [], None
    payload = json.loads(Path(path).read_text())
    buffer_default = payload.get("buffer_default")
    if buffer_default is not None:
        buffer_default = float(buffer_default)
        if buffer_default < 0:
            raise ValueError("buffer_default must be >= 0")
    windows: list[CapacityWindow] = []
    for raw in payload.get("windows", []):
        windows.append(
            CapacityWindow(
                start=_stamp(raw["start"]),
                end=_stamp(raw["end"]),
                healthy_capacity=_optional_float(raw.get("healthy_capacity")),
                known_reservations=_optional_float(raw.get("known_reservations")),
                maintenance_capacity=_optional_float(raw.get("maintenance_capacity")),
                buffer=_optional_float(raw.get("buffer")),
                reason=None if raw.get("reason") is None else str(raw["reason"]),
                announced_at=None if raw.get("announced_at") is None else _stamp(raw["announced_at"]),
            )
        )
    return windows, buffer_default


def _stamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _parse_prom_value(raw: object) -> float:
    if raw is None:
        return math.nan
    text = str(raw).strip()
    if text in {"", "NaN", "nan", "+Inf", "-Inf", "Inf"}:
        return math.nan
    return float(text)


def parse_query_range(payload: dict) -> pd.Series:
    """Parse one Prometheus ``query_range`` JSON body into a UTC series.

    A missing or non-finite sample is absent from the series. Callers reindex
    the full grid and must not fill those holes with zero.
    """
    if payload.get("status") != "success":
        raise ValueError(f"prometheus query failed: {payload.get('error', payload.get('status'))}")
    data = payload.get("data") or {}
    if data.get("resultType") != "matrix":
        raise ValueError(f"expected a matrix result, got {data.get('resultType')}")
    result = data.get("result") or []
    if len(result) == 0:
        return pd.Series(dtype=float)
    if len(result) != 1:
        raise ValueError(
            f"PromQL returned {len(result)} series; aggregate to one pool before forecasting"
        )
    values = result[0].get("values") or []
    index: list[pd.Timestamp] = []
    points: list[float] = []
    for sample in values:
        timestamp = pd.to_datetime(float(sample[0]), unit="s", utc=True)
        value = _parse_prom_value(sample[1])
        if math.isfinite(value):
            index.append(timestamp)
            points.append(value)
    return pd.Series(points, index=pd.DatetimeIndex(index), dtype=float)


class PrometheusHistorySource:
    """Optional PromQL range-query reader.

    ``queries`` maps a history count column to a PromQL expression that
    already selects a single pool. The scope dict is attached to every row;
    this adapter does not discover pools from metric labels.
    """

    def __init__(
        self,
        base_url: str,
        queries: dict[str, str],
        start: pd.Timestamp,
        end: pd.Timestamp,
        step: pd.Timedelta,
        scope: dict[str, str],
        *,
        opener=None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.queries = dict(queries)
        self.start = _stamp(start)
        self.end = _stamp(end)
        self.step = pd.Timedelta(step)
        self.scope = scope
        self.timeout = timeout
        self.opener = opener

    def load(self) -> HistoryTable:
        required = [
            "healthy_capacity",
            "protected_allocated",
            "reclaimable_allocated",
            "serving_allocated",
            "known_reservations",
            "maintenance_capacity",
        ]
        missing = [field for field in required if field not in self.queries]
        if missing:
            raise ValueError(f"prometheus queries missing {missing}")
        for key in ("cluster", "node_pool", "gpu_type"):
            if key not in self.scope:
                raise ValueError(f"prometheus scope missing {key}")
        series = {field: self._query(expr) for field, expr in self.queries.items()}
        grid = pd.date_range(self.start, self.end, freq=self.step)
        frame = pd.DataFrame({"timestamp": grid})
        for field in COUNT_COLUMNS:
            if field in series:
                aligned = series[field].reindex(grid)
                frame[field] = aligned.to_numpy()
            elif field == "protected_pending" and field not in self.queries:
                continue
            else:
                frame[field] = math.nan
        present_fields = [field for field in COUNT_COLUMNS if field in self.queries]
        finite = frame[present_fields].notna()
        quality = pd.Series("gap", index=frame.index, dtype=object)
        quality = quality.mask(finite.all(axis=1), "ok")
        partial = finite.any(axis=1) & ~finite.all(axis=1)
        quality = quality.mask(partial, "partial")
        frame["data_quality"] = quality
        frame["cluster"] = str(self.scope["cluster"])
        frame["node_pool"] = str(self.scope["node_pool"])
        frame["gpu_type"] = str(self.scope["gpu_type"])
        frame["resource_policy"] = str(self.scope.get("resource_policy", ""))
        return normalize_history(frame, source=self.base_url)

    def _query(self, expr: str) -> pd.Series:
        params = urlencode(
            {
                "query": expr,
                "start": f"{self.start.timestamp():.3f}",
                "end": f"{self.end.timestamp():.3f}",
                "step": str(int(self.step.total_seconds())),
            }
        )
        url = f"{self.base_url}/api/v1/query_range?{params}"
        opener = self.opener or self._open
        with opener(url) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return parse_query_range(payload)

    def _open(self, url: str):
        return urlopen(url, timeout=self.timeout)
