"""Budget formula, including the spec's worked example and the clamp at zero."""

import pandas as pd
import pytest

from capacity_forecast.budget import (
    CapacityState,
    CapacityWindow,
    resolve_inputs,
    serving_budgets,
)
from capacity_forecast.schema import effective_capacity


def test_worked_example_median_104_conservative_64():
    median, lower = serving_budgets(
        capacity=256,
        protected_p50=144,
        protected_upper=184,
        buffer=8,
    )
    assert median == 104
    assert lower == 64


def test_budget_clamps_at_zero():
    median, lower = serving_budgets(
        capacity=10,
        protected_p50=40,
        protected_upper=50,
        buffer=8,
    )
    assert median == 0
    assert lower == 0

    median, lower = serving_budgets(
        capacity=256,
        protected_p50=144,
        protected_upper=300,
        buffer=8,
    )
    assert median == 104
    assert lower == 0


def test_negative_inputs_are_rejected():
    with pytest.raises(ValueError):
        serving_budgets(capacity=-1, protected_p50=1, protected_upper=1, buffer=0)


def test_effective_capacity_subtracts_reservations_and_maintenance():
    assert effective_capacity(256, 8, 32) == 216
    assert effective_capacity(10, 8, 8) == 0


def test_override_replaces_future_capacity_without_forecasting_it():
    baseline = CapacityState(
        healthy_capacity=256,
        known_reservations=0,
        maintenance_capacity=0,
        timestamp=pd.Timestamp("2026-01-02T00:00:00Z"),
    )
    window = CapacityWindow(
        start=pd.Timestamp("2026-01-02T02:00:00Z"),
        end=pd.Timestamp("2026-01-02T04:00:00Z"),
        healthy_capacity=200,
        buffer=10,
        reason="node drain",
        announced_at=pd.Timestamp("2026-01-01T00:00:00Z"),
    )
    later = CapacityWindow(
        start=pd.Timestamp("2026-01-02T02:00:00Z"),
        end=pd.Timestamp("2026-01-02T03:00:00Z"),
        maintenance_capacity=16,
        announced_at=pd.Timestamp("2026-01-03T00:00:00Z"),
    )
    stamps = [
        pd.Timestamp("2026-01-02T01:00:00Z"),
        pd.Timestamp("2026-01-02T02:00:00Z"),
        pd.Timestamp("2026-01-02T04:00:00Z"),
    ]
    # The second window was announced after the cutoff, so a backtest ignores it.
    honored = resolve_inputs(
        baseline,
        buffer_default=8,
        windows=[window, later],
        timestamps=stamps,
        known_at=pd.Timestamp("2026-01-02T00:00:00Z"),
        honor_announced_at=True,
    )
    assert honored[0].capacity == 256
    assert honored[0].buffer == 8
    assert honored[0].reason is None
    assert honored[1].capacity == 200
    assert honored[1].buffer == 10
    assert honored[1].reason == "node drain"
    assert honored[2].capacity == 256

    # The live worker applies every window in the file the operator just passed.
    live = resolve_inputs(
        baseline,
        buffer_default=8,
        windows=[window, later],
        timestamps=stamps,
        known_at=pd.Timestamp("2026-01-02T00:00:00Z"),
        honor_announced_at=False,
    )
    assert live[1].capacity == max(200 - 0 - 16, 0)
