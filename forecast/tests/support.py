"""Helpers for building history frames in tests."""

from __future__ import annotations

import pandas as pd


def history_frame(timestamps, **columns) -> pd.DataFrame:
    index = pd.to_datetime(list(timestamps), utc=True)
    count = len(index)
    data = {
        "timestamp": index,
        "cluster": "c",
        "node_pool": "pool",
        "gpu_type": "gpu",
        "resource_policy": "policy",
        "healthy_capacity": 256.0,
        "protected_allocated": 100.0,
        "protected_pending": 0.0,
        "reclaimable_allocated": 4.0,
        "serving_allocated": 10.0,
        "known_reservations": 0.0,
        "maintenance_capacity": 0.0,
        "data_quality": "ok",
    }
    for key, value in columns.items():
        if isinstance(value, (list, tuple, pd.Series)):
            data[key] = list(value)
        else:
            data[key] = [value] * count
    return pd.DataFrame(data)
