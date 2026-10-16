"""Gaps are not zeros, and real bursts are not smoothed away."""

import math

import pandas as pd
import pytest

from capacity_forecast.cleaning import prepare_pool
from tests.support import history_frame


def _hourly(count: int, start: str = "2026-01-05T00:00:00Z"):
    return list(pd.date_range(start, periods=count, freq="h", tz="UTC"))


def _prepare(frame, **kwargs):
    as_of = kwargs.pop("as_of", frame["timestamp"].max())
    return prepare_pool(
        frame,
        as_of=as_of,
        step=kwargs.pop("step", pd.Timedelta(hours=1)),
        max_gap_fill=kwargs.pop("max_gap_fill", 2),
        pending_in_target=kwargs.pop("pending_in_target", True),
    )


def test_scrape_failed_zero_is_not_occupancy():
    stamps = _hourly(3)
    frame = history_frame(
        stamps,
        protected_allocated=[10, 0, 10],
        protected_pending=[0, 0, 0],
        data_quality=["ok", "scrape_failed", "ok"],
    )
    prepared = _prepare(frame)
    assert prepared.protected_demand.tolist() == [10, 10, 10]
    assert prepared.imputed_mask.tolist() == [False, True, False]
    assert 0 not in prepared.protected_demand


def test_missing_timestamp_is_a_gap_and_not_zero():
    stamps = _hourly(4)
    # Drop the third hour entirely. The grid still has that timestamp.
    frame = history_frame(
        [stamps[0], stamps[1], stamps[3]],
        protected_allocated=[12, 12, 12],
        protected_pending=[1, 1, 1],
    )
    prepared = _prepare(frame, as_of=stamps[3])
    assert prepared.protected_demand.tolist() == [13, 13, 13, 13]
    assert prepared.imputed_mask.tolist() == [False, False, True, False]


def test_blank_pending_is_unknown_not_zero():
    stamps = _hourly(3)
    frame = history_frame(
        stamps,
        protected_allocated=[10, 10, 10],
        protected_pending=[1, math.nan, 2],
    )
    prepared = _prepare(frame)
    assert prepared.protected_demand.tolist() == [11, 11, 12]
    assert prepared.imputed_mask.tolist() == [False, True, False]


def test_pending_is_part_of_protected_demand_and_serving_is_not():
    stamps = _hourly(2)
    frame = history_frame(
        stamps,
        protected_allocated=[10, 10],
        protected_pending=[3, 3],
        serving_allocated=[100, 500],
        reclaimable_allocated=[80, 80],
    )
    prepared = _prepare(frame)
    assert prepared.protected_demand.tolist() == [13, 13]
    assert prepared.serving_allocated == 500


def test_absent_pending_column_is_not_invented_as_zero_demand():
    stamps = _hourly(2)
    frame = history_frame(stamps, protected_allocated=[10, 15])
    frame = frame.drop(columns=["protected_pending"])
    prepared = _prepare(frame, pending_in_target=False)
    assert prepared.protected_demand.tolist() == [10, 15]
    assert prepared.observed_protected_pending is None


def test_real_burst_is_kept():
    stamps = _hourly(4)
    frame = history_frame(
        stamps,
        protected_allocated=[10, 10, 80, 10],
        protected_pending=[0, 0, 0, 0],
    )
    prepared = _prepare(frame)
    assert prepared.protected_demand.tolist() == [10, 10, 80, 10]
    assert not prepared.imputed_mask.any()


def test_negative_count_is_a_collection_error_not_negative_demand():
    stamps = _hourly(3)
    frame = history_frame(
        stamps,
        protected_allocated=[20, -5, 20],
        protected_pending=[0, 0, 0],
        data_quality=["ok", "ok", "ok"],
    )
    prepared = _prepare(frame)
    assert prepared.protected_demand.tolist() == [20, 20, 20]
    assert prepared.imputed_mask.tolist() == [False, True, False]
    assert prepared.report["collection_error_rows"] == 1


def test_long_internal_gap_drops_the_prefix_instead_of_filling_zero():
    stamps = _hourly(10)
    quality = ["ok"] * 10
    allocated = [10, 10, 10, 0, 0, 0, 0, 30, 30, 30]
    for index in range(3, 7):
        quality[index] = "scrape_failed"
    frame = history_frame(stamps, protected_allocated=allocated, data_quality=quality)
    prepared = _prepare(frame, max_gap_fill=2)
    assert prepared.protected_demand.tolist() == [30, 30, 30]
    assert pd.Timestamp(prepared.timestamps[0]) == stamps[7]


def test_long_tail_gap_is_an_error():
    stamps = _hourly(6)
    quality = ["ok", "ok", "ok", "scrape_failed", "scrape_failed", "scrape_failed"]
    frame = history_frame(stamps, data_quality=quality, protected_allocated=[5, 5, 5, 0, 0, 0])
    with pytest.raises(ValueError, match="ends with a gap"):
        _prepare(frame, max_gap_fill=2)
