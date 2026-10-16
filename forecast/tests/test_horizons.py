"""Horizon tiers and the 5-minute season lengths from the spec."""

import pandas as pd

from capacity_forecast.horizons import (
    default_tiers,
    forecast_offsets,
    resolve_tiers,
    season_lengths,
)


def test_five_minute_points_have_daily_288_and_weekly_2016():
    assert season_lengths(pd.Timedelta(minutes=5)) == [288, 2016]


def test_default_tiers_cover_a_day_at_the_suggested_steps():
    tiers, notes = resolve_tiers(default_tiers(), base_step_seconds=300, horizon_seconds=24 * 3600)
    assert notes == []
    offsets = forecast_offsets(tiers)
    assert offsets[0] == (300, 300)
    assert (1800, 300) in offsets
    assert (21600, 300) in offsets
    assert (22500, 900) in offsets
    assert offsets[-1] == (24 * 3600, 900)
    # The week-scale tier is beyond a 24h horizon, so it is not emitted.
    assert all(step != 3600 for _, step in offsets)


def test_finer_tier_than_the_history_grid_is_coarsened():
    tiers, notes = resolve_tiers(default_tiers(), base_step_seconds=3600, horizon_seconds=24 * 3600)
    assert notes
    offsets = forecast_offsets(tiers)
    assert offsets[0] == (3600, 3600)
    assert offsets[-1][0] == 24 * 3600
    assert len(offsets) == 24
    assert all(step == 3600 for _, step in offsets)
