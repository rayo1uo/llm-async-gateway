"""Protected-demand forecast for a future serving-budget planner.

This package reads historical pool metrics and writes a forecast snapshot.
It does not scale replicas, change admission, or serve inference.
"""

from __future__ import annotations

__version__ = "0.1.0"

SCHEMA_VERSION = "capacity-forecast.v1"
BACKTEST_SCHEMA_VERSION = "capacity-forecast-backtest.v1"
