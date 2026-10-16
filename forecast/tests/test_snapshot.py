"""Snapshot schema, budget wiring, and the serving-occupancy exclusion."""

import json

import pandas as pd
import pytest

from capacity_forecast.budget import CapacityWindow, serving_budgets
from capacity_forecast.schema import (
    DISCLAIMER,
    PLACEMENT_NOT_HANDLED,
    SCHEMA_VERSION,
    validate_pool_snapshot,
    validate_snapshot,
)
from capacity_forecast.snapshot import ForecastConfig, build_snapshot_document
from capacity_forecast.sources import normalize_history
from tests.support import history_frame


def _hourly(count: int, start: str = "2026-01-05T00:00:00Z"):
    return list(pd.date_range(start, periods=count, freq="h", tz="UTC"))


def _document(frame, **config):
    table = normalize_history(frame, source="test")
    # The test frames always carry protected_pending, including when the
    # caller wants it ignored. Honor an explicit drop.
    if "protected_pending" not in frame.columns:
        table.pending_in_target = False
        table.frame = table.frame.drop(columns=["protected_pending"])
    settings = dict(
        model="seasonal_naive",
        horizon=pd.Timedelta(hours=6),
        buffer=8,
        step=pd.Timedelta(hours=1),
        quantile=0.95,
    )
    settings.update(config)
    as_of = settings.pop("as_of", None)
    return build_snapshot_document(
        table,
        ForecastConfig(**settings),
        now=pd.Timestamp("2026-02-01T00:00:00Z"),
        as_of=as_of,
    )


def _minimal_pool(budget_median=104, budget_lower=64):
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": {
            "cluster": "c",
            "node_pool": "pool",
            "gpu_type": "gpu",
            "resource_policy": "policy",
        },
        "as_of": "2026-01-01T00:00:00Z",
        "generated_at": "2026-01-01T00:00:01Z",
        "model": {
            "name": "MSTL",
            "season_lengths": [288, 2016],
            "quantile": 0.95,
            "alpha": 0.05,
            "interval_level": 90,
        },
        "horizon": {
            "base_step_seconds": 300,
            "until_seconds": 300,
            "tiers": [{"from_seconds": 0, "until_seconds": 300, "step_seconds": 300}],
        },
        "points": [
            {
                "timestamp": "2026-01-01T00:05:00Z",
                "horizon_seconds": 300,
                "step_seconds": 300,
                "protected_demand_p50": 144,
                "protected_demand_upper": 184,
                "quantile": 0.95,
                "capacity": 256,
                "buffer": 8,
                "budget_median": budget_median,
                "budget_lower": budget_lower,
            }
        ],
        "observed": {},
        "quality": {},
        "semantics": {
            "target": "protected_demand",
            "budget_formula": "A(t)=max(C(t)-P(t)-B(t),0)",
            "budget_lower_formula": "A_lower(t)=max(C(t)-Q_(1-alpha)(P(t))-B(t),0)",
            "budget_median_formula": "A_median(t)=max(C(t)-Q_0.50(P(t))-B(t),0)",
            "total_budget_includes_current_serving": True,
            "serving_allocated_excluded_from_protected_demand": True,
            "statistical_estimate_not_sla": True,
            "placement_not_handled": list(PLACEMENT_NOT_HANDLED),
        },
        "disclaimer": DISCLAIMER,
    }


def test_schema_accepts_the_worked_example_and_rejects_a_wrong_budget():
    validate_pool_snapshot(_minimal_pool())
    with pytest.raises(ValueError, match="budget_median"):
        validate_pool_snapshot(_minimal_pool(budget_median=100, budget_lower=64))
    with pytest.raises(ValueError, match="budget_lower"):
        validate_pool_snapshot(_minimal_pool(budget_median=104, budget_lower=60))


def test_snapshot_schema_and_formula_on_a_fitted_model():
    stamps = _hourly(48)
    serving = [500.0 if pd.Timestamp(stamp).hour == 3 else 10.0 for stamp in stamps]
    frame = history_frame(
        stamps,
        protected_allocated=40,
        protected_pending=0,
        serving_allocated=serving,
        reclaimable_allocated=80,
    )
    # One failed scrape of zeros must not pull the seasonal repeat down to zero.
    frame.loc[10, "data_quality"] = "scrape_failed"
    frame.loc[10, "protected_allocated"] = 0
    frame.loc[10, "protected_pending"] = 0
    frame.loc[10, "serving_allocated"] = 0
    document = _document(frame)
    validate_snapshot(document)
    round_trip = json.loads(json.dumps(document))
    validate_snapshot(round_trip)
    pool = document["pools"][0]
    assert pool["schema_version"] == SCHEMA_VERSION
    assert pool["model"]["name"] == "SeasonalNaive"
    assert pool["model"]["quantile"] == 0.95
    assert pool["model"]["interval_level"] == 90
    assert pool["as_of"] == "2026-01-06T23:00:00Z"
    assert pool["generated_at"] != pool["as_of"]
    assert pool["scope"] == {
        "cluster": "c",
        "node_pool": "pool",
        "gpu_type": "gpu",
        "resource_policy": "policy",
    }
    assert "Not an SLA" in pool["disclaimer"]
    assert pool["semantics"]["placement_not_handled"] == list(PLACEMENT_NOT_HANDLED)
    assert pool["points"]
    for point in pool["points"]:
        median, lower = serving_budgets(
            point["capacity"],
            point["protected_demand_p50"],
            point["protected_demand_upper"],
            point["buffer"],
        )
        assert point["budget_median"] == pytest.approx(median)
        assert point["budget_lower"] == pytest.approx(lower)
        # Protected demand is flat at 40. Serving spikes to 500 and must not
        # appear in the target. Reclaimable (80) must not appear either.
        assert point["protected_demand_p50"] == pytest.approx(40, abs=1e-6)
        assert point["protected_demand_upper"] == pytest.approx(40, abs=1e-6)
        assert point["capacity"] == pytest.approx(256)
        assert point["budget_median"] == pytest.approx(208)
        assert point["budget_lower"] == pytest.approx(208)
    assert pool["observed"]["serving_allocated"]["value"] == pytest.approx(10)


def test_pending_shifts_the_forecast_and_serving_does_not():
    stamps = _hourly(48)
    frame = history_frame(
        stamps,
        protected_allocated=100,
        protected_pending=20,
        serving_allocated=70,
        healthy_capacity=256,
    )
    pool = _document(frame, buffer=8)["pools"][0]
    assert pool["points"][0]["protected_demand_p50"] == pytest.approx(120)
    # max(256 - 120 - 8, 0) = 128. A target that had included serving (70)
    # would have forecast about 190 and a median budget near 58.
    assert pool["points"][0]["budget_median"] == pytest.approx(128)
    assert pool["observed"]["serving_allocated"]["value"] == pytest.approx(70)


def test_budget_clamps_at_zero_when_protected_demand_exceeds_capacity():
    stamps = _hourly(30)
    frame = history_frame(
        stamps,
        protected_allocated=300,
        protected_pending=0,
        healthy_capacity=256,
    )
    pool = _document(frame, buffer=8, horizon=pd.Timedelta(hours=2))["pools"][0]
    assert pool["points"][0]["protected_demand_p50"] == pytest.approx(300)
    assert pool["points"][0]["budget_median"] == 0
    assert pool["points"][0]["budget_lower"] == 0


def test_capacity_uses_reservations_maintenance_and_a_future_window():
    stamps = _hourly(48)
    frame = history_frame(
        stamps,
        protected_allocated=50,
        protected_pending=0,
        healthy_capacity=256,
        known_reservations=8,
        maintenance_capacity=32,
    )
    as_of = stamps[-1]
    window = CapacityWindow(
        start=as_of + pd.Timedelta(hours=2),
        end=as_of + pd.Timedelta(hours=4),
        healthy_capacity=200,
        known_reservations=0,
        maintenance_capacity=0,
        buffer=10,
        reason="drain",
    )
    pool = _document(frame, buffer=8, windows=[window])["pools"][0]
    assert pool["observed"]["capacity"]["value"] == pytest.approx(216)
    assert pool["points"][0]["capacity"] == pytest.approx(216)
    assert pool["points"][0]["budget_median"] == pytest.approx(max(216 - 50 - 8, 0))
    drained = pool["points"][1]
    assert drained["capacity"] == pytest.approx(200)
    assert drained["buffer"] == pytest.approx(10)
    assert drained["override_reason"] == "drain"
    assert drained["budget_median"] == pytest.approx(max(200 - 50 - 10, 0))


def test_future_rows_do_not_change_the_snapshot():
    stamps = _hourly(72)
    frame = history_frame(stamps, protected_allocated=40, protected_pending=0, serving_allocated=10)
    origin = stamps[48]
    before = _document(frame, as_of=origin, horizon=pd.Timedelta(hours=6))
    future = frame.copy()
    future.loc[future["timestamp"] > origin, "protected_allocated"] = 99999
    future.loc[future["timestamp"] > origin, "serving_allocated"] = 99999
    future.loc[future["timestamp"] > origin, "protected_pending"] = 99999
    after = _document(future, as_of=origin, horizon=pd.Timedelta(hours=6))
    assert before["pools"][0]["points"] == after["pools"][0]["points"]
    assert before["pools"][0]["observed"] == after["pools"][0]["observed"]
    assert before["pools"][0]["quality"] == after["pools"][0]["quality"]


def test_mstl_snapshot_validates():
    stamps = _hourly(24 * 10)
    frame = history_frame(stamps, protected_allocated=80, protected_pending=5)
    document = _document(frame, model="mstl", horizon=pd.Timedelta(hours=3), buffer=8)
    validate_snapshot(document)
    pool = document["pools"][0]
    assert pool["model"]["name"] == "MSTL"
    assert 24 in pool["model"]["season_lengths"]
    assert 168 in pool["model"]["season_lengths"]
    assert pool["points"][0]["protected_demand_p50"] == pytest.approx(85, abs=1)
    assert pool["points"][0]["budget_lower"] <= pool["points"][0]["budget_median"]
