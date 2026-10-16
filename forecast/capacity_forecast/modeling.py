"""Fit SeasonalNaive and MSTL on protected demand and read their intervals.

AutoETS is available for the backtest comparison only. It is not a model
the worker will publish: a seasonal ETS state at length 288 or 2016 is a
different, much heavier fit, and v1 does not need it to produce a snapshot.
"""

from __future__ import annotations

import math

import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoETS, MSTL, SeasonalNaive

from capacity_forecast import __version__ as IMPLEMENTATION_VERSION
from capacity_forecast.schema import interval_level, level_token, quantile_to_alpha

PUBLISHED_MODELS = ("seasonal_naive", "mstl")
MODEL_NAMES = {
    "seasonal_naive": "SeasonalNaive",
    "mstl": "MSTL",
    "autoets": "AutoETS",
}

# Above this daily season length, AutoETS is skipped unless the caller forces it.
AUTOETS_SEASON_LIMIT = 48


def include_autoets_model(daily_season: int, *, enabled: bool, force: bool) -> tuple[bool, str | None]:
    """Decide whether the backtest should fit AutoETS.

    Returns ``(run, note)``. A note is set when the caller asked for AutoETS
    and the season is too long to run without ``force``. The snapshot worker
    never publishes AutoETS.
    """
    if not enabled:
        return False, None
    if daily_season > AUTOETS_SEASON_LIMIT and not force:
        return False, (
            f"AutoETS skipped: daily season_length {daily_season} is above "
            f"{AUTOETS_SEASON_LIMIT}. It is only a backtest comparison; "
            "pass --autoets-force to run it. The published snapshot does not use AutoETS."
        )
    return True, None


def select_seasons(history_length: int, daily: int, weekly: int) -> tuple[list[int], bool]:
    """Keep the weekly season only when the training series covers it."""
    if history_length < daily:
        raise ValueError(
            f"need at least one daily season ({daily} points) to fit, got {history_length}"
        )
    if history_length >= weekly:
        return [daily, weekly], True
    return [daily], False


def frequency_alias(step: pd.Timedelta) -> str:
    seconds = int(round(pd.Timedelta(step).total_seconds()))
    if seconds % 86400 == 0:
        days = seconds // 86400
        return "D" if days == 1 else f"{days}D"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return "h" if hours == 1 else f"{hours}h"
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes}min"
    return f"{seconds}s"


def _build_models(
    model_keys: list[str],
    seasons: list[int],
    *,
    allow_autoets: bool,
) -> list:
    models = []
    naive_season = seasons[-1]
    for key in model_keys:
        if key == "seasonal_naive":
            models.append(SeasonalNaive(season_length=naive_season))
        elif key == "mstl":
            # Default trend forecaster is a non-seasonal AutoETS. The daily
            # and weekly cycles are carried by the MSTL seasonal components,
            # each of which is forecast with SeasonalNaive inside MSTL.
            season_arg: int | list[int] = seasons if len(seasons) > 1 else seasons[0]
            models.append(MSTL(season_length=season_arg))
        elif key == "autoets":
            if not allow_autoets:
                raise ValueError("AutoETS is a backtest comparison only and is not published")
            models.append(AutoETS(season_length=seasons[0]))
        else:
            raise ValueError(f"unknown model {key}")
    return models


def predict_protected(
    timestamps: pd.DatetimeIndex,
    values: list[float] | pd.Series,
    *,
    step: pd.Timedelta,
    horizon_steps: int,
    seasons: list[int],
    model_keys: list[str],
    quantile: float,
    allow_autoets: bool = False,
) -> dict[str, pd.DataFrame]:
    """Forecast protected demand. ``values`` must already be gap-filled.

    Returns, per model name, a frame indexed by UTC timestamp with columns
    ``p50`` and ``upper``. ``p50`` is the model's point forecast, which is the
    center of its native equal-tailed interval and therefore its median under
    that symmetric model. ``upper`` is the ``quantile`` endpoint (P95 when
    quantile is 0.95). Both are clamped at 0; protected demand cannot be negative.
    """
    if horizon_steps < 1:
        raise ValueError("horizon_steps must be >= 1")
    level = interval_level(quantile)
    quantile_to_alpha(quantile)
    token = level_token(level)
    level_arg: int | float = int(round(level)) if token.isdigit() else level
    index = pd.DatetimeIndex(timestamps)
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    frame = pd.DataFrame(
        {
            "unique_id": "pool",
            "ds": index,
            "y": [float(value) for value in values],
        }
    )
    if frame["y"].isna().any():
        raise ValueError("training series still contains gaps; refusing to fill them with zero")
    if not frame["ds"].is_monotonic_increasing or frame["ds"].duplicated().any():
        raise ValueError("training timestamps must be strictly increasing")
    models = _build_models(model_keys, seasons, allow_autoets=allow_autoets)
    forecaster = StatsForecast(models=models, freq=frequency_alias(step), n_jobs=1)
    forecaster.fit(frame)
    predicted = forecaster.predict(h=horizon_steps, level=[level_arg])
    predicted = predicted.copy()
    predicted["ds"] = pd.to_datetime(predicted["ds"], utc=True)
    out: dict[str, pd.DataFrame] = {}
    for key in model_keys:
        name = MODEL_NAMES[key]
        upper_col = f"{name}-hi-{token}"
        if upper_col not in predicted.columns:
            candidates = [column for column in predicted.columns if column.startswith(f"{name}-hi-")]
            raise ValueError(
                f"prediction interval column {upper_col} missing; got {list(predicted.columns)} "
                f"(hi columns: {candidates})"
            )
        p50 = predicted[name].astype(float).clip(lower=0.0)
        upper = predicted[upper_col].astype(float).clip(lower=0.0)
        # A numerical inversion would make the "conservative" budget larger
        # than the median budget. Keep the upper quantile at least at P50.
        upper = upper.where(upper >= p50, p50)
        piece = pd.DataFrame({"p50": p50.to_numpy(), "upper": upper.to_numpy()})
        piece.index = pd.DatetimeIndex(predicted["ds"])
        if not math.isfinite(float(piece["p50"].sum())) or not math.isfinite(float(piece["upper"].sum())):
            raise ValueError(f"{name} produced a non-finite forecast")
        out[name] = piece
    return out


def model_metadata(
    name: str,
    seasons: list[int],
    quantile: float,
    *,
    weekly_included: bool,
) -> dict:
    import statsforecast

    return {
        "name": name,
        "season_lengths": [int(season) for season in seasons],
        "seasonality": "UTC daily and weekly" if weekly_included else "UTC daily",
        "point_forecast": "median_under_symmetric_native_interval",
        "quantile": float(quantile),
        "alpha": quantile_to_alpha(quantile),
        "interval_level": interval_level(quantile),
        "interval": "statsforecast_native_equal_tailed",
        "library": "statsforecast",
        "library_version": statsforecast.__version__,
        "implementation_version": IMPLEMENTATION_VERSION,
        "demand_clamped_at_zero": True,
    }
