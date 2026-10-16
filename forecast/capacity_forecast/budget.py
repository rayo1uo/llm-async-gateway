"""Serving resource budget.

A(t) = max(C(t) - P(t) - B(t), 0)

P is protected demand that cannot be yielded to this serving system.
C is effective healthy capacity, a known input. B is the platform reserve,
also a known input. The forecast applies to P. A is the total budget,
including GPUs this serving system already holds.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from capacity_forecast.schema import effective_capacity


def serving_budgets(
    capacity: float,
    protected_p50: float,
    protected_upper: float,
    buffer: float,
) -> tuple[float, float]:
    """Return ``(budget_median, budget_lower)``.

    budget_median = max(C - P50 - B, 0)
    budget_lower = max(C - Q_(1-alpha)(P) - B, 0)

    The worked example capacity=256, P50=144, P95=184, buffer=8 yields
    median 104 and conservative 64. Both sides clamp at 0.
    """
    values = {
        "capacity": capacity,
        "protected_p50": protected_p50,
        "protected_upper": protected_upper,
        "buffer": buffer,
    }
    for name, value in values.items():
        number = float(value)
        if number < 0:
            raise ValueError(f"{name} must be >= 0, got {number}")
        values[name] = number
    if values["protected_upper"] + 1e-12 < values["protected_p50"]:
        raise ValueError("upper protected-demand quantile is below P50")
    median = max(values["capacity"] - values["protected_p50"] - values["buffer"], 0.0)
    lower = max(values["capacity"] - values["protected_upper"] - values["buffer"], 0.0)
    return median, lower


@dataclass(frozen=True)
class CapacityState:
    """Last known components of C. Not a forecast."""

    healthy_capacity: float
    known_reservations: float
    maintenance_capacity: float
    timestamp: pd.Timestamp

    @property
    def effective(self) -> float:
        return effective_capacity(
            self.healthy_capacity,
            self.known_reservations,
            self.maintenance_capacity,
        )


@dataclass(frozen=True)
class CapacityWindow:
    """A scheduled modification of C or B over ``[start, end)``.

    Fields left as None keep the baseline component. Later windows in the
    list win on overlap. ``announced_at`` is ignored by the live worker
    (the file is an input the operator is supplying now) and honored by the
    backtest so a schedule that was not yet announced cannot leak in.
    """

    start: pd.Timestamp
    end: pd.Timestamp
    healthy_capacity: float | None = None
    known_reservations: float | None = None
    maintenance_capacity: float | None = None
    buffer: float | None = None
    reason: str | None = None
    announced_at: pd.Timestamp | None = None

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("capacity window end must be after start")
        for name in (
            "healthy_capacity",
            "known_reservations",
            "maintenance_capacity",
            "buffer",
        ):
            value = getattr(self, name)
            if value is not None and float(value) < 0:
                raise ValueError(f"capacity window {name} must be >= 0")


@dataclass(frozen=True)
class ResolvedInput:
    """C and B at one forecast timestamp."""

    capacity: float
    buffer: float
    reason: str | None


def resolve_inputs(
    baseline: CapacityState,
    buffer_default: float,
    windows: list[CapacityWindow],
    timestamps: list[pd.Timestamp],
    *,
    known_at: pd.Timestamp | None = None,
    honor_announced_at: bool = False,
) -> list[ResolvedInput]:
    """Project known C and B onto forecast timestamps. Does not forecast them."""
    if buffer_default < 0:
        raise ValueError("buffer must be >= 0")
    resolved: list[ResolvedInput] = []
    for timestamp in timestamps:
        healthy = baseline.healthy_capacity
        reservations = baseline.known_reservations
        maintenance = baseline.maintenance_capacity
        buffer = float(buffer_default)
        reason: str | None = None
        for window in windows:
            if honor_announced_at and window.announced_at is not None:
                if known_at is None or window.announced_at > known_at:
                    continue
            if window.start <= timestamp < window.end:
                if window.healthy_capacity is not None:
                    healthy = window.healthy_capacity
                if window.known_reservations is not None:
                    reservations = window.known_reservations
                if window.maintenance_capacity is not None:
                    maintenance = window.maintenance_capacity
                if window.buffer is not None:
                    buffer = window.buffer
                reason = window.reason
        resolved.append(
            ResolvedInput(
                capacity=effective_capacity(healthy, reservations, maintenance),
                buffer=buffer,
                reason=reason,
            )
        )
    return resolved
