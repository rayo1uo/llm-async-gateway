"""Forecast horizon tiers and UTC season lengths.

The default tiers are an initial suggestion, not a validated operating point:
the first half hour at a 5 minute step, through 6 hours at 5 minutes,
through 24 hours at 15 minutes, and coarser after that. A tier step finer
than the history grid is raised to the grid; this worker does not invent
a finer series than it was given.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Tier:
    until_seconds: int
    step_seconds: int

    def __post_init__(self) -> None:
        if self.until_seconds <= 0 or self.step_seconds <= 0:
            raise ValueError("tier bounds and steps must be positive")


@dataclass(frozen=True)
class ResolvedTier:
    from_seconds: int
    until_seconds: int
    step_seconds: int


def default_tiers() -> list[Tier]:
    return [
        Tier(until_seconds=30 * 60, step_seconds=5 * 60),
        Tier(until_seconds=6 * 3600, step_seconds=5 * 60),
        Tier(until_seconds=24 * 3600, step_seconds=15 * 60),
        Tier(until_seconds=7 * 24 * 3600, step_seconds=3600),
    ]


def season_lengths(step: pd.Timedelta) -> list[int]:
    """Daily then weekly season length, in steps, for a UTC grid.

    Five-minute points yield 288 and 2016. The step must divide one day so
    both seasons land on an integer number of observations.
    """
    step = pd.Timedelta(step)
    if step <= pd.Timedelta(0):
        raise ValueError("step must be positive")
    day = pd.Timedelta(days=1)
    if day % step != pd.Timedelta(0):
        raise ValueError(f"step {step} does not divide one UTC day")
    daily = int(day / step)
    weekly = daily * 7
    return [daily, weekly]


def parse_duration(text: str) -> pd.Timedelta:
    """Parse a duration such as ``5min``, ``15min``, ``1h``, ``24h``, or ``7d``."""
    try:
        value = pd.Timedelta(text)
    except ValueError as exc:
        raise ValueError(f"invalid duration {text!r}") from exc
    if value <= pd.Timedelta(0):
        raise ValueError(f"duration must be positive, got {text!r}")
    return value


def duration_seconds(value: pd.Timedelta) -> int:
    seconds = pd.Timedelta(value).total_seconds()
    if abs(seconds - round(seconds)) > 1e-6:
        raise ValueError(f"duration {value} is not a whole number of seconds")
    return int(round(seconds))


def resolve_tiers(
    tiers: list[Tier],
    base_step_seconds: int,
    horizon_seconds: int,
) -> tuple[list[ResolvedTier], list[str]]:
    """Clip tiers to the horizon and coarsen steps that are finer than the grid."""
    if base_step_seconds <= 0:
        raise ValueError("base step must be positive")
    if horizon_seconds < base_step_seconds:
        raise ValueError("horizon is shorter than one history step")
    if horizon_seconds % base_step_seconds != 0:
        raise ValueError("horizon must be a multiple of the history step")
    ordered = sorted(tiers, key=lambda tier: tier.until_seconds)
    notes: list[str] = []
    resolved: list[ResolvedTier] = []
    previous = 0
    for tier in ordered:
        until = min(tier.until_seconds, horizon_seconds)
        if until <= previous:
            continue
        step = tier.step_seconds
        if step < base_step_seconds:
            notes.append(
                f"tier step {step}s raised to history step {base_step_seconds}s "
                f"through {until}s; this worker does not forecast finer than the input grid"
            )
            step = base_step_seconds
        elif step % base_step_seconds != 0:
            raised = ((step + base_step_seconds - 1) // base_step_seconds) * base_step_seconds
            notes.append(
                f"tier step {step}s raised to {raised}s so it lands on the history grid"
            )
            step = raised
        resolved.append(ResolvedTier(previous, until, step))
        previous = until
    if previous < horizon_seconds:
        if resolved:
            last = resolved[-1]
            resolved[-1] = ResolvedTier(last.from_seconds, horizon_seconds, last.step_seconds)
            notes.append(
                f"last tier extended from {previous}s to the horizon {horizon_seconds}s"
            )
        else:
            resolved.append(ResolvedTier(0, horizon_seconds, base_step_seconds))
            notes.append("no tier covered the horizon; using the history step")
    return resolved, notes


def forecast_offsets(tiers: list[ResolvedTier]) -> list[tuple[int, int]]:
    """Return ``(offset_seconds, step_seconds)`` for each emitted timestamp.

    Offsets are measured from ``as_of`` and are strictly positive. A timestamp
    that sits on a tier boundary is emitted once, by the earlier tier.
    """
    emitted: list[tuple[int, int]] = []
    seen: set[int] = set()
    for tier in tiers:
        offset = ((tier.from_seconds // tier.step_seconds) + 1) * tier.step_seconds
        if offset <= tier.from_seconds:
            offset += tier.step_seconds
        while offset <= tier.until_seconds:
            if offset not in seen:
                emitted.append((offset, tier.step_seconds))
                seen.add(offset)
            offset += tier.step_seconds
    emitted.sort()
    if not emitted:
        raise ValueError("horizon and tiers produced no forecast timestamps")
    return emitted
