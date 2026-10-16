"""File contract and the optional Prometheus range-query adapter."""

import json

import pandas as pd
import pytest

from capacity_forecast.schema import EXAMPLE_DATA_ORIGIN
from capacity_forecast.sources import (
    FileHistorySource,
    PrometheusHistorySource,
    normalize_history,
    parse_query_range,
)
from capacity_forecast.synthetic import generate_synthetic_history, write_history
from capacity_forecast.worker import main as worker_main


def test_csv_and_parquet_round_trip(tmp_path):
    frame = generate_synthetic_history(step="1h", days=2, seed=1)
    for name in ("history.csv", "history.parquet"):
        path = tmp_path / name
        write_history(frame, path)
        loaded = FileHistorySource(path).load()
        assert loaded.pending_in_target is True
        assert loaded.frame["data_origin"].eq(EXAMPLE_DATA_ORIGIN).all()
        assert loaded.frame["data_quality"].isin(["ok", "scrape_failed"]).any()
        assert (loaded.frame["data_quality"] == "scrape_failed").any()
        failed = loaded.frame["data_quality"] == "scrape_failed"
        # The file may store a numeric zero on a failed scrape. The loader
        # must not rewrite that quality into ok, and the cleaner (covered
        # elsewhere) must not train on it. Here we only check it survived.
        assert failed.any()
        assert loaded.frame.loc[failed, "protected_allocated"].fillna(0).eq(0).all()


def test_resource_policy_defaults_and_missing_pending_is_not_a_zero_column(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"],
            "cluster": ["c", "c"],
            "node_pool": ["p", "p"],
            "gpu_type": ["g", "g"],
            "healthy_capacity": [256, 256],
            "protected_allocated": [10, 12],
            "reclaimable_allocated": [1, 1],
            "serving_allocated": [4, 4],
            "known_reservations": [0, 0],
            "maintenance_capacity": [0, 0],
            "data_quality": ["ok", "ok"],
        }
    )
    path = tmp_path / "no_policy.csv"
    frame.to_csv(path, index=False)
    loaded = FileHistorySource(path).load()
    assert loaded.pending_in_target is False
    assert loaded.frame["resource_policy"].eq("").all()
    assert loaded.frame["protected_pending"].isna().all()


def test_parse_query_range_keeps_real_zeros_and_drops_nan_samples():
    payload = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"cluster": "c"},
                    "values": [[1700000000, "0"], [1700000300, "NaN"], [1700000600, "12"]],
                }
            ],
        },
    }
    series = parse_query_range(payload)
    assert list(series.to_numpy()) == [0.0, 12.0]
    assert 1700000300 not in set(series.index.astype("int64") // 10**9)


def test_prometheus_adapter_marks_missing_samples_as_gaps_not_zeros():
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    step = pd.Timedelta(minutes=5)
    end = start + 2 * step
    stamps = [start, start + step, end]
    present = {stamps[0], stamps[1]}

    def payload_for(include_zero: bool):
        values = []
        for stamp in stamps:
            if stamp not in present:
                continue
            if stamp == stamps[0] and include_zero:
                values.append([stamp.timestamp(), "0"])
            else:
                values.append([stamp.timestamp(), "10"])
        return {
            "status": "success",
            "data": {"resultType": "matrix", "result": [{"metric": {}, "values": values}]},
        }

    fields = [
        "healthy_capacity",
        "protected_allocated",
        "protected_pending",
        "reclaimable_allocated",
        "serving_allocated",
        "known_reservations",
        "maintenance_capacity",
    ]

    def opener(url):
        query = url.split("query=", 1)[1].split("&", 1)[0]
        from urllib.parse import unquote

        name = unquote(query)
        body = json.dumps(payload_for(include_zero=(name == "serving_allocated"))).encode()

        class _Response:
            def read(self):
                return body

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        return _Response()

    source = PrometheusHistorySource(
        "http://prometheus.example",
        {field: field for field in fields},
        start,
        end,
        step,
        {"cluster": "c", "node_pool": "p", "gpu_type": "g", "resource_policy": "policy"},
        opener=opener,
    )
    table = source.load()
    assert table.pending_in_target is True
    qualities = table.frame["data_quality"].tolist()
    assert qualities == ["ok", "ok", "gap"]
    gap = table.frame.iloc[2]
    assert pd.isna(gap["protected_allocated"])
    assert pd.isna(gap["healthy_capacity"])
    assert gap["serving_allocated"] != 0 or pd.isna(gap["serving_allocated"])
    # The first serving sample is a real zero, and the row is otherwise present.
    assert table.frame.iloc[0]["serving_allocated"] == 0
    assert table.frame.iloc[0]["data_quality"] == "ok"
    assert table.frame.iloc[0]["protected_allocated"] == 10


def test_worker_cli_writes_a_snapshot(tmp_path, capsys):
    history = tmp_path / "history.csv"
    output = tmp_path / "snapshot.json"
    write_history(generate_synthetic_history(step="1h", days=10, seed=2), history)
    code = worker_main(
        [
            "--history",
            str(history),
            "--output",
            str(output),
            "--model",
            "seasonal_naive",
            "--horizon",
            "6h",
            "--step",
            "1h",
            "--buffer",
            "8",
            "--quantile",
            "0.95",
        ]
    )
    assert code == 0
    document = json.loads(output.read_text())
    assert document["example_data"] is True
    assert document["pools"][0]["model"]["name"] == "SeasonalNaive"
    assert document["pools"][0]["points"]
    captured = capsys.readouterr()
    assert "synthetic example data" in captured.err


def test_query_range_rejects_multiple_series():
    payload = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {"metric": {}, "values": [[1, "1"]]},
                {"metric": {}, "values": [[1, "2"]]},
            ],
        },
    }
    with pytest.raises(ValueError, match="one pool"):
        parse_query_range(payload)


def test_normalize_rejects_a_missing_column():
    with pytest.raises(ValueError, match="healthy_capacity"):
        normalize_history(pd.DataFrame({"timestamp": ["2026-01-01T00:00:00Z"]}), source="x")
