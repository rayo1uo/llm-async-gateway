"""Rolling-origin backtest on synthetic example history, with no future leakage."""

import pandas as pd
import pytest

from capacity_forecast.backtest import run_backtest, score_points
from capacity_forecast.schema import DISCLAIMER
from capacity_forecast.snapshot import ForecastConfig
from capacity_forecast.sources import normalize_history
from capacity_forecast.synthetic import SYNTHETIC_BANNER, generate_synthetic_history


def _config(**overrides):
    settings = dict(
        quantile=0.95,
        buffer=8,
        horizon=pd.Timedelta(hours=12),
        step=pd.Timedelta(hours=1),
        max_gap_fill=2,
        honor_announced_at=True,
    )
    settings.update(overrides)
    return ForecastConfig(**settings)


def test_gap_outcomes_are_not_scored_as_zero():
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    future = start + pd.Timedelta(hours=1)
    assert score_points({future: (100.0, 110.0)}, {start: 100.0}) == []

    scored = score_points({start: (10.0, 12.0)}, {start: 0.0})
    assert len(scored) == 1
    assert scored[0][1] == pytest.approx(-10)
    assert scored[0][2] is True


def test_backtest_on_synthetic_history_reports_error_and_coverage():
    frame = generate_synthetic_history(step="1h", days=12, seed=7)
    assert set(frame["data_origin"]) == {"synthetic_example"}
    assert SYNTHETIC_BANNER
    table = normalize_history(frame, source="synthetic-example")
    report = run_backtest(
        table,
        _config(),
        min_train=pd.Timedelta(days=8),
        origin_step=pd.Timedelta(days=1),
    )
    assert report["example_data"] is True
    assert report["schema_version"] == "capacity-forecast-backtest.v1"
    assert report["models"] == ["SeasonalNaive", "MSTL"]
    assert any(DISCLAIMER in note or "not an SLA" in note for note in report["notes"])
    pool = report["pools"][0]
    assert pool["origins_used"] >= 2
    for name in ("SeasonalNaive", "MSTL"):
        overall = pool["models"][name]["overall"]
        assert overall["n"] > 0
        assert overall["mae"] >= 0
        assert overall["rmse"] >= 0
        assert 0 <= overall["upper_coverage"] <= 1
        assert overall["target_coverage"] == pytest.approx(0.95)
        assert pool["models"][name]["per_horizon"]
        for row in pool["models"][name]["per_horizon"]:
            assert row["horizon_seconds"] > 0
            assert row["n"] > 0
            assert 0 <= row["upper_coverage"] <= 1


def test_autoets_is_available_as_a_backtest_comparison_only():
    frame = generate_synthetic_history(step="1h", days=12, seed=3)
    table = normalize_history(frame, source="synthetic-example")
    report = run_backtest(
        table,
        _config(horizon=pd.Timedelta(hours=6)),
        min_train=pd.Timedelta(days=8),
        origin_step=pd.Timedelta(days=3),
        include_autoets=True,
    )
    assert "AutoETS" in report["models"]
    overall = report["pools"][0]["models"]["AutoETS"]["overall"]
    assert overall["n"] > 0
    assert 0 <= overall["upper_coverage"] <= 1


def test_autoets_is_skipped_for_a_five_minute_season_unless_forced():
    from capacity_forecast.modeling import include_autoets_model

    run, note = include_autoets_model(288, enabled=True, force=False)
    assert run is False
    assert note is not None
    assert "288" in note
    assert "not use AutoETS" in note
    run, note = include_autoets_model(288, enabled=True, force=True)
    assert run is True and note is None
    run, note = include_autoets_model(24, enabled=True, force=False)
    assert run is True and note is None
