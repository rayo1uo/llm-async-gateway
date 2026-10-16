"""Shared constants for the history contract and the snapshot document."""

from __future__ import annotations

import math
from typing import Any

from capacity_forecast import SCHEMA_VERSION

POOL_KEYS = ("cluster", "node_pool", "gpu_type", "resource_policy")

# Counts that describe the pool at one timestamp. None of them are inferred
# by filling a failed scrape with zero.
COUNT_COLUMNS = (
    "healthy_capacity",
    "protected_allocated",
    "protected_pending",
    "reclaimable_allocated",
    "serving_allocated",
    "known_reservations",
    "maintenance_capacity",
)

REQUIRED_HISTORY_COLUMNS = (
    "timestamp",
    "cluster",
    "node_pool",
    "gpu_type",
    "healthy_capacity",
    "protected_allocated",
    "reclaimable_allocated",
    "serving_allocated",
    "known_reservations",
    "maintenance_capacity",
    "data_quality",
)

# Present only when the producer actually has a pending-protected series.
# Omitting the column means "not part of P". An empty cell means "unknown",
# which is a gap, not a zero.
OPTIONAL_HISTORY_COLUMNS = (
    "resource_policy",
    "protected_pending",
    "data_origin",
)

# The only quality value whose numbers may enter the model. Every other
# token (scrape_failed, missing, gap, partial, collection_error, ...) drops
# the row's measurements.
USABLE_QUALITY = "ok"

EXAMPLE_DATA_ORIGIN = "synthetic_example"

BUDGET_FORMULA = "A(t)=max(C(t)-P(t)-B(t),0)"
BUDGET_LOWER_FORMULA = "A_lower(t)=max(C(t)-Q_(1-alpha)(P(t))-B(t),0)"
BUDGET_MEDIAN_FORMULA = "A_median(t)=max(C(t)-Q_0.50(P(t))-B(t),0)"

PLACEMENT_NOT_HANDLED = (
    "gpu_fragmentation_across_nodes",
    "whole_node_topology",
    "reclaim_latency",
    "queue_quota",
)

DISCLAIMER = (
    "Statistical estimate of protected demand and the derived serving resource budget. "
    "Not an SLA and not a deadline-completion guarantee. "
    "Placement feasibility is not handled here and must be checked by the consumer: "
    "GPU fragmentation across nodes, whole-node topology, reclaim latency, and queue quota. "
    "budget_median and budget_lower are the total pool budget, including capacity this serving "
    "system already holds; subtract current serving usage downstream. "
    "serving_allocated is not part of protected demand and was not used as a forecasting target."
)

POINT_FIELDS = (
    "timestamp",
    "horizon_seconds",
    "step_seconds",
    "protected_demand_p50",
    "protected_demand_upper",
    "quantile",
    "capacity",
    "buffer",
    "budget_median",
    "budget_lower",
)


def quantile_to_alpha(quantile: float) -> float:
    """Return alpha for an upper quantile q = 1 - alpha."""
    q = float(quantile)
    if not 0.5 <= q < 1.0:
        raise ValueError("quantile (1-alpha) must be in [0.5, 1)")
    # 1 - 0.95 is not a clean binary float. Round so the snapshot says 0.05.
    return round(1.0 - q, 10)


def interval_level(quantile: float) -> float:
    """Central interval level whose upper endpoint is ``quantile``.

    StatsForecast native intervals are equal-tailed around the point forecast.
    The upper bound of the central level ``100 * (2q - 1)`` is the model's
    ``q`` quantile. For q = 0.95 the level is 90, and the ``hi-90`` column is P95.
    """
    q = float(quantile)
    quantile_to_alpha(q)
    level = round(100.0 * (2.0 * q - 1.0), 10)
    if abs(level - round(level)) < 1e-9:
        return float(round(level))
    return level


def level_token(level: float) -> str:
    """Column suffix StatsForecast uses for a confidence level."""
    if abs(level - round(level)) < 1e-9:
        return str(int(round(level)))
    text = f"{level:.4f}".rstrip("0").rstrip(".")
    return text


def effective_capacity(healthy: float, reservations: float, maintenance: float) -> float:
    """C = max(healthy - reservations - maintenance, 0).

    The three inputs must already describe non-overlapping holds. A node that
    has left ``healthy_capacity`` must not also be counted in
    ``maintenance_capacity``.
    """
    return max(float(healthy) - float(reservations) - float(maintenance), 0.0)


def _close(left: float, right: float, tol: float = 1e-6) -> bool:
    return abs(float(left) - float(right)) <= tol


def validate_pool_snapshot(pool: dict[str, Any]) -> None:
    """Raise ValueError when a per-pool snapshot breaks the v1 contract."""
    required = (
        "schema_version",
        "scope",
        "as_of",
        "generated_at",
        "model",
        "horizon",
        "points",
        "observed",
        "quality",
        "semantics",
        "disclaimer",
    )
    missing = [key for key in required if key not in pool]
    if missing:
        raise ValueError(f"pool snapshot missing {missing}")
    if pool["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
    if pool["disclaimer"] != DISCLAIMER:
        raise ValueError("disclaimer text does not match the v1 contract")
    scope = pool["scope"]
    for key in POOL_KEYS:
        if not isinstance(scope.get(key), str):
            raise ValueError(f"scope.{key} must be a string")
    model = pool["model"]
    for key in ("name", "season_lengths", "quantile", "alpha", "interval_level"):
        if key not in model:
            raise ValueError(f"model.{key} is required")
    if model["name"] not in {"SeasonalNaive", "MSTL"}:
        raise ValueError(f"unsupported published model {model['name']}")
    q = float(model["quantile"])
    if not _close(float(model["alpha"]), quantile_to_alpha(q)):
        raise ValueError("model.alpha is not 1 - quantile")
    if not _close(float(model["interval_level"]), interval_level(q)):
        raise ValueError("model.interval_level does not match the upper quantile")
    horizon = pool["horizon"]
    for key in ("base_step_seconds", "until_seconds", "tiers"):
        if key not in horizon:
            raise ValueError(f"horizon.{key} is required")
    if not pool["points"]:
        raise ValueError("snapshot has no forecast points")
    semantics = pool["semantics"]
    if semantics.get("budget_formula") != BUDGET_FORMULA:
        raise ValueError("budget formula mismatch")
    if semantics.get("budget_lower_formula") != BUDGET_LOWER_FORMULA:
        raise ValueError("budget_lower formula mismatch")
    if semantics.get("budget_median_formula") != BUDGET_MEDIAN_FORMULA:
        raise ValueError("budget_median formula mismatch")
    if semantics.get("statistical_estimate_not_sla") is not True:
        raise ValueError("snapshot must say this is not an SLA")
    if semantics.get("total_budget_includes_current_serving") is not True:
        raise ValueError("budget must be documented as a total budget")
    if semantics.get("serving_allocated_excluded_from_protected_demand") is not True:
        raise ValueError("serving_allocated must be excluded from P")
    if list(semantics.get("placement_not_handled", [])) != list(PLACEMENT_NOT_HANDLED):
        raise ValueError("placement exclusions do not match the v1 contract")
    for point in pool["points"]:
        for key in POINT_FIELDS:
            if key not in point:
                raise ValueError(f"point missing {key}")
        for key in (
            "protected_demand_p50",
            "protected_demand_upper",
            "capacity",
            "buffer",
            "budget_median",
            "budget_lower",
        ):
            if not math.isfinite(float(point[key])) or float(point[key]) < 0:
                raise ValueError(f"point.{key} must be finite and >= 0")
        if float(point["protected_demand_upper"]) + 1e-9 < float(point["protected_demand_p50"]):
            raise ValueError("upper quantile is below P50")
        if not _close(float(point["quantile"]), q):
            raise ValueError("point quantile does not match the model")
        expected_median = max(
            float(point["capacity"]) - float(point["protected_demand_p50"]) - float(point["buffer"]),
            0.0,
        )
        expected_lower = max(
            float(point["capacity"]) - float(point["protected_demand_upper"]) - float(point["buffer"]),
            0.0,
        )
        if not _close(point["budget_median"], expected_median):
            raise ValueError("budget_median does not match A = max(C - P50 - B, 0)")
        if not _close(point["budget_lower"], expected_lower):
            raise ValueError("budget_lower does not match A_lower = max(C - Q - B, 0)")
        if int(point["horizon_seconds"]) <= 0:
            raise ValueError("horizon_seconds must be positive")


def validate_snapshot(document: dict[str, Any]) -> None:
    """Validate an envelope or a single pool snapshot."""
    if "pools" in document:
        if document.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        if document.get("kind") != "capacity_forecast_snapshot_set":
            raise ValueError("envelope kind must be capacity_forecast_snapshot_set")
        if document.get("disclaimer") != DISCLAIMER:
            raise ValueError("envelope disclaimer mismatch")
        if "generated_at" not in document:
            raise ValueError("envelope missing generated_at")
        if not isinstance(document["pools"], list) or not document["pools"]:
            raise ValueError("envelope needs at least one pool")
        for pool in document["pools"]:
            validate_pool_snapshot(pool)
        return
    validate_pool_snapshot(document)
