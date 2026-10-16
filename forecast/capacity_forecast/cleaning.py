"""Turn raw pool history into a contiguous protected-demand series.

Missing and failed scrapes are gaps. They are never read as zero occupancy.
Short gaps are forward-filled from the last real observation, which keeps a
burst that was actually seen and does not invent one. A gap longer than
``max_gap_fill`` is not filled: training starts after the last such gap.
A long gap at the tail is an error, because carrying a stale demand forward
across an outage would look like a fresh forecast.

Only clear collection errors are removed: a quality flag other than ``ok``,
a non-numeric token, or a negative count. Large positive jumps stay.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from capacity_forecast.budget import CapacityState
from capacity_forecast.schema import COUNT_COLUMNS, USABLE_QUALITY


def _as_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        if pd.isna(value):
            return math.nan
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _is_finite(value: float) -> bool:
    return math.isfinite(value)


@dataclass
class PreparedPool:
    """Contiguous training series ending at ``as_of``, plus the known C."""

    timestamps: pd.DatetimeIndex
    protected_demand: np.ndarray
    imputed_mask: np.ndarray
    as_of: pd.Timestamp
    step: pd.Timedelta
    baseline: CapacityState
    observed_protected: float | None
    observed_protected_at: pd.Timestamp | None
    observed_protected_allocated: float | None
    observed_protected_pending: float | None
    serving_allocated: float | None
    serving_allocated_at: pd.Timestamp | None
    reclaimable_allocated: float | None
    reclaimable_allocated_at: pd.Timestamp | None
    pending_in_target: bool
    report: dict
    notes: list[str] = field(default_factory=list)


def infer_step(timestamps: pd.Series) -> pd.Timedelta:
    """Smallest positive spacing, which is the grid when gaps are multiples of it."""
    ordered = pd.to_datetime(timestamps, utc=True).sort_values().drop_duplicates()
    if len(ordered) < 2:
        raise ValueError("need at least two timestamps to infer the step")
    deltas = ordered.diff().dropna()
    deltas = deltas[deltas > pd.Timedelta(0)]
    if deltas.empty:
        raise ValueError("timestamps do not advance")
    step = deltas.min()
    quotients = deltas / step
    if not np.allclose(quotients.to_numpy(dtype=float), np.round(quotients.to_numpy(dtype=float))):
        raise ValueError(
            "timestamps are not on one regular grid; pass --step explicitly. "
            "This worker does not resample, because averaging would smooth bursts"
        )
    return pd.Timedelta(step)


def _mark_non_numeric(frame: pd.DataFrame) -> pd.DataFrame:
    """Turn non-numeric count tokens into collection errors. Blanks stay missing."""
    out = frame.copy()
    bad = pd.Series(False, index=out.index)
    for column in COUNT_COLUMNS:
        if column not in out.columns:
            continue
        raw = out[column]
        numeric = pd.to_numeric(raw, errors="coerce")
        if raw.dtype == object:
            text = raw.astype(str).str.strip()
            nonempty = raw.notna() & ~text.isin(["", "nan", "NaN", "None", "<NA>"])
            bad = bad | (nonempty & numeric.isna())
        out[column] = numeric
    if bad.any():
        out.loc[bad, "data_quality"] = "collection_error"
        out.loc[bad, list(COUNT_COLUMNS)] = math.nan
    return out


def _collapse_duplicates(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    notes: list[str] = []
    columns = [column for column in list(COUNT_COLUMNS) + ["data_quality"] if column in frame.columns]
    kept: list[pd.Series] = []
    for timestamp, group in frame.groupby("timestamp", sort=True):
        if len(group) == 1:
            kept.append(group.iloc[0])
            continue
        conflicting = False
        for column in columns:
            if group[column].nunique(dropna=False) > 1:
                conflicting = True
                break
        if conflicting:
            notes.append(
                f"conflicting duplicate rows at {timestamp} marked collection_error "
                "and excluded; they were not averaged and not treated as zero"
            )
            row = group.iloc[-1].copy()
            row["data_quality"] = "collection_error"
            for column in COUNT_COLUMNS:
                if column in row.index:
                    row[column] = math.nan
            kept.append(row)
        else:
            kept.append(group.iloc[-1])
    collapsed = pd.DataFrame(kept).reset_index(drop=True)
    return collapsed, notes


def _row_negative(row: pd.Series, pending_in_target: bool) -> bool:
    fields = [
        "healthy_capacity",
        "protected_allocated",
        "reclaimable_allocated",
        "serving_allocated",
        "known_reservations",
        "maintenance_capacity",
    ]
    if pending_in_target:
        fields.append("protected_pending")
    for name in fields:
        value = _as_float(row.get(name))
        if _is_finite(value) and value < 0:
            return True
    return False


def _protected_from_row(row: pd.Series, pending_in_target: bool) -> float:
    """Observed P, or NaN when this row must not enter the target."""
    if str(row.get("data_quality", "")).strip().lower() != USABLE_QUALITY:
        return math.nan
    if _row_negative(row, pending_in_target):
        return math.nan
    allocated = _as_float(row.get("protected_allocated"))
    if not _is_finite(allocated):
        return math.nan
    if not pending_in_target:
        return allocated
    pending = _as_float(row.get("protected_pending"))
    # A blank pending cell is unknown. Adding zero would hide saturation.
    if not _is_finite(pending):
        return math.nan
    return allocated + pending


def _capacity_from_row(row: pd.Series, pending_in_target: bool) -> tuple[float, float, float] | None:
    if str(row.get("data_quality", "")).strip().lower() != USABLE_QUALITY:
        return None
    if _row_negative(row, pending_in_target):
        return None
    healthy = _as_float(row.get("healthy_capacity"))
    reservations = _as_float(row.get("known_reservations"))
    maintenance = _as_float(row.get("maintenance_capacity"))
    if not (_is_finite(healthy) and _is_finite(reservations) and _is_finite(maintenance)):
        return None
    return healthy, reservations, maintenance


def _latest_count(
    indexed: dict[pd.Timestamp, pd.Series],
    grid: pd.DatetimeIndex,
    column: str,
    pending_in_target: bool,
) -> tuple[float | None, pd.Timestamp | None]:
    for timestamp in reversed(grid):
        row = indexed.get(timestamp)
        if row is None:
            continue
        if str(row.get("data_quality", "")).strip().lower() != USABLE_QUALITY:
            continue
        if _row_negative(row, pending_in_target):
            continue
        value = _as_float(row.get(column))
        if _is_finite(value):
            return value, pd.Timestamp(timestamp)
    return None, None


def prepare_pool(
    frame: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    step: pd.Timedelta,
    max_gap_fill: int,
    pending_in_target: bool,
) -> PreparedPool:
    """Build the training series using only rows at or before ``as_of``."""
    if max_gap_fill < 0:
        raise ValueError("max_gap_fill must be >= 0")
    as_of = pd.Timestamp(as_of)
    if as_of.tzinfo is None:
        as_of = as_of.tz_localize("UTC")
    else:
        as_of = as_of.tz_convert("UTC")
    step = pd.Timedelta(step)
    working = frame.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True)
    working = working.loc[working["timestamp"] <= as_of].sort_values("timestamp")
    if working.empty:
        raise ValueError("no history at or before as_of")
    # Count only rows that would have been known at the cutoff. Rows after
    # as_of are not part of this snapshot, so appending a future scrape must
    # not change the document.
    known_rows = int(len(working))
    working = _mark_non_numeric(working)
    working, notes = _collapse_duplicates(working)
    working["data_quality"] = working["data_quality"].astype(str).str.strip().str.lower()

    start = pd.Timestamp(working["timestamp"].min())
    if (as_of - start) % step != pd.Timedelta(0):
        raise ValueError("as_of is not on the history grid; choose a cutoff the step divides")
    off_grid = [
        pd.Timestamp(timestamp)
        for timestamp in working["timestamp"]
        if (pd.Timestamp(timestamp) - start) % step != pd.Timedelta(0)
    ]
    if off_grid:
        raise ValueError(
            "history has timestamps off the step grid "
            f"(first {off_grid[0].isoformat()}). Refusing to bin them; "
            "binning would smooth bursts"
        )
    grid = pd.date_range(start, as_of, freq=step)
    indexed = {
        pd.Timestamp(timestamp): group.iloc[0]
        for timestamp, group in working.groupby("timestamp", sort=True)
    }

    protected = np.full(len(grid), math.nan, dtype=float)
    collection_errors = 0
    rejected_quality = 0
    for index, timestamp in enumerate(grid):
        row = indexed.get(pd.Timestamp(timestamp))
        if row is None:
            continue
        quality = str(row.get("data_quality", "")).strip().lower()
        if quality != USABLE_QUALITY:
            if quality == "collection_error" or _row_negative(row, pending_in_target):
                collection_errors += 1
            else:
                rejected_quality += 1
            continue
        if _row_negative(row, pending_in_target):
            collection_errors += 1
            continue
        protected[index] = _protected_from_row(row, pending_in_target)

    usable = np.isfinite(protected)
    if not usable.any():
        raise ValueError("no usable protected-demand observations at or before as_of")

    last_usable = int(np.where(usable)[0][-1])
    tail = len(protected) - 1 - last_usable
    if tail > max_gap_fill:
        raise ValueError(
            f"the series ends with a gap of {tail} steps, longer than max_gap_fill="
            f"{max_gap_fill}. Refusing to treat that outage as zero occupancy and "
            "refusing to carry demand forward across it. Raise --max-gap-fill only "
            "if a stale carry-forward is an explicit choice, or restore the scrape"
        )

    cut = 0
    cursor = 0
    while cursor <= last_usable:
        if usable[cursor]:
            cursor += 1
            continue
        end = cursor
        while end <= last_usable and not usable[end]:
            end += 1
        run = end - cursor
        has_predecessor = bool(usable[:cursor].any())
        if run > max_gap_fill or not has_predecessor:
            cut = end
            notes.append(
                f"excluded history before grid index {end} because of a gap of {run} "
                f"steps (max_gap_fill={max_gap_fill}); the gap was not filled with zero"
            )
        cursor = end

    segment = protected[cut:].copy()
    segment_usable = usable[cut:].copy()
    imputed = np.zeros(len(segment), dtype=bool)
    last_value: float | None = None
    for index in range(len(segment)):
        if segment_usable[index]:
            last_value = float(segment[index])
            continue
        if last_value is None:
            raise RuntimeError("gap fill reached a point with no predecessor")
        segment[index] = last_value
        imputed[index] = True

    baseline: CapacityState | None = None
    for timestamp in reversed(grid):
        row = indexed.get(pd.Timestamp(timestamp))
        if row is None:
            continue
        components = _capacity_from_row(row, pending_in_target)
        if components is None:
            continue
        healthy, reservations, maintenance = components
        baseline = CapacityState(
            healthy_capacity=healthy,
            known_reservations=reservations,
            maintenance_capacity=maintenance,
            timestamp=pd.Timestamp(timestamp),
        )
        break
    if baseline is None:
        raise ValueError(
            "no usable capacity observation at or before as_of; C is a known input "
            "and is not invented from a failed scrape"
        )

    observed_p, observed_p_at = _latest_finite_protected(indexed, grid, pending_in_target)
    allocated, _ = _latest_count(indexed, grid, "protected_allocated", pending_in_target)
    if pending_in_target:
        pending, _ = _latest_count(indexed, grid, "protected_pending", pending_in_target)
    else:
        pending = None
    serving, serving_at = _latest_count(indexed, grid, "serving_allocated", pending_in_target)
    reclaimable, reclaimable_at = _latest_count(
        indexed, grid, "reclaimable_allocated", pending_in_target
    )

    gap_points = int((~usable).sum())
    report = {
        "input_rows": known_rows,
        "rows_at_or_before_as_of": int(len(working)),
        "grid_points": int(len(grid)),
        "usable_protected_rows": int(usable.sum()),
        "gap_points_before_fill": gap_points,
        "imputed_ffill_points": int(imputed.sum()),
        "collection_error_rows": int(collection_errors),
        "rejected_quality_rows": int(rejected_quality),
        "training_points": int(len(segment)),
        "prefix_points_dropped": int(cut),
        "pending_in_target": bool(pending_in_target),
    }
    return PreparedPool(
        timestamps=pd.DatetimeIndex(grid[cut:]),
        protected_demand=segment,
        imputed_mask=imputed,
        as_of=as_of,
        step=step,
        baseline=baseline,
        observed_protected=observed_p,
        observed_protected_at=observed_p_at,
        observed_protected_allocated=allocated,
        observed_protected_pending=pending,
        serving_allocated=serving,
        serving_allocated_at=serving_at,
        reclaimable_allocated=reclaimable,
        reclaimable_allocated_at=reclaimable_at,
        pending_in_target=pending_in_target,
        report=report,
        notes=notes,
    )


def _latest_finite_protected(
    indexed: dict[pd.Timestamp, pd.Series],
    grid: pd.DatetimeIndex,
    pending_in_target: bool,
) -> tuple[float | None, pd.Timestamp | None]:
    for timestamp in reversed(grid):
        row = indexed.get(pd.Timestamp(timestamp))
        if row is None:
            continue
        value = _protected_from_row(row, pending_in_target)
        if _is_finite(value):
            return float(value), pd.Timestamp(timestamp)
    return None, None


def observed_protected_map(frame: pd.DataFrame, pending_in_target: bool) -> dict[pd.Timestamp, float]:
    """Real P observations, with gaps omitted. Used as backtest labels, never as zeros."""
    working = _mark_non_numeric(frame.copy())
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True)
    working, _ = _collapse_duplicates(working)
    observed: dict[pd.Timestamp, float] = {}
    for _, row in working.iterrows():
        value = _protected_from_row(row, pending_in_target)
        if _is_finite(value):
            observed[pd.Timestamp(row["timestamp"])] = float(value)
    return observed
