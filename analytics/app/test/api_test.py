import asyncio
import copy
import os.path
import time
from collections import OrderedDict
from typing import Optional

import pytest
import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.workload.schemes import TimeSeries
from app.prepare_model import prepare_workload_models
from app.server import start_server

PROFILE_DATA_NAME: str = "profiles2.csv"

app: Optional[FastAPI] = None
client: Optional[TestClient] = None


@pytest.fixture(autouse=True)
def setup():
    global app
    global client

    asyncio.get_event_loop().run_until_complete(prepare_workload_models(["TEST"]))
    app = start_server()
    client = TestClient(app)

    yield

    app = None
    client = None


def test_workload_merge():
    workload1: TimeSeries = TimeSeries.fold([0, 1, 2, 3], [1, 2, 2, 3])
    workload2: TimeSeries = TimeSeries.fold([7, 8, 9, 10], [8, 9, 9, 10])

    for pair in [[workload1, workload2], [workload2, workload1]]:
        merged_workload: TimeSeries = TimeSeries.merge(*pair)
        timestamps, throughput_rates = TimeSeries.unfold(merged_workload)

        assert not np.isnan(throughput_rates).any()
        assert np.array_equiv(np.arange(0, 11, 1), timestamps)
        assert np.array_equiv(np.array([1, 2, 2, 3, 4.25, 5.5, 6.75, 8, 9, 9, 10]), throughput_rates)


def test_common_regression():
    response = client.post("/common/regression", json={
        "workload1": TimeSeries.create_example(10, offset=1).dict(),
        "workload2": TimeSeries.create_example(10, offset=11).dict()
    })

    assert response.status_code == 200
    response_dict: dict = response.json()

    assert len(response_dict) == 2
    for key, value_dict in response_dict.items():
        assert all([k in value_dict for k in ["slope", "intercept"]])


def test_baseline():

    response = client.post("/baselines/twres_training", json={
        "scale_outs": [2, 4, 8],
        "throughput_rates": [0.5, 1, 2],
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    response = client.post("/baselines/twres_prediction", json={
        "workload": TimeSeries.create_example(600).dict(),
        "avg_latency": 1000,
        "max_latency_constraint": 2000,
        "scale_out": 10,
        "time_window_interval": 600,
        "min_scale_out": 4,
        "max_scale_out": 24,
        "job": "TEST"
    })
    assert response.status_code == 200
    assert isinstance(response.json()["scale_out"], int)
    print(response.json())


def test_latency():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), PROFILE_DATA_NAME))
    df = df[["scaleOut", "avgThr", "avgLat", "isBckPres"]]
    df = df[df["isBckPres"] == 0]

    def _flat_list(series: pd.Series):
        return series.values.reshape(-1).tolist()

    response = client.post("/latency/training", json={
        "scale_outs": _flat_list(df["scaleOut"]),
        "throughput_rates": _flat_list(df["avgThr"]),
        "latencies": _flat_list(df["avgLat"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    response = client.post("/latency/prediction", json={
        "min_scale_out": 2,
        "max_scale_out": 24,
        "scale_out": 12,
        "throughput_rate": 50379.1,
        "job": "TEST"
    })

    assert response.status_code == 200
    print(response.json())


def test_recoverytime():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), PROFILE_DATA_NAME))
    df = df[["scaleOut", "avgThr", "avgLat", "isBckPres"]]
    df = df[df["isBckPres"] == 1]

    def _flat_list(series: pd.Series):
        return series.values.reshape(-1).tolist()

    response = client.post("/recoverytime/training", json={
        "scale_outs": _flat_list(df["scaleOut"]),
        "max_throughput_rates": _flat_list(df["avgThr"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    response = client.post("/recoverytime/prediction", json={
        "min_scale_out": 2,
        "max_scale_out": 24,
        "workload": TimeSeries.create_example(600, offset=6001).dict(),
        "scale_out": 4,
        "prediction_period_in_s": 600,
        "downtime": 10.0,
        "last_checkpoint": 90,
        "max_recovery_time": 240,
        "job": "TEST"
    })
    assert response.status_code == 200
    print(response.json())


def test_recoverytime_plus_latency():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), PROFILE_DATA_NAME))
    df = df[["scaleOut", "avgThr", "avgLat", "isBckPres"]]

    def _flat_list(series: pd.Series):
        return series.values.reshape(-1).tolist()

    rt_df = copy.deepcopy(df[df["isBckPres"] == 1])
    response = client.post("/recoverytime/training", json={
        "scale_outs": _flat_list(rt_df["scaleOut"]),
        "max_throughput_rates": _flat_list(rt_df["avgThr"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    lat_df = copy.deepcopy(df[df["isBckPres"] == 0])
    response = client.post("/latency/training", json={
        "scale_outs": _flat_list(lat_df["scaleOut"]),
        "throughput_rates": _flat_list(lat_df["avgThr"]),
        "latencies": _flat_list(lat_df["avgLat"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    response = client.post("/recoverytime/prediction", json={
        "min_scale_out": 2,
        "max_scale_out": 24,
        "workload": TimeSeries.create_example(600, offset=6001).dict(),
        "scale_out": 11,
        "prediction_period_in_s": 600,
        "downtime": 10.0,
        "last_checkpoint": 90,
        "max_recovery_time": 240,
        "job": "TEST"
    })
    assert response.status_code == 200

    response_json = response.json()
    print("rt_response", response_json)
    response_json["job"] = "TEST"

    response = client.post("/latency/evaluation", json=response_json)

    assert response.status_code == 200
    print("latency_response", response.json())


def test_latency_plus_recoverytime():
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), PROFILE_DATA_NAME))
    df = df[["scaleOut", "avgThr", "avgLat", "isBckPres"]]

    def _flat_list(series: pd.Series):
        return series.values.reshape(-1).tolist()

    lat_df = copy.deepcopy(df[df["isBckPres"] == 0])
    response = client.post("/latency/training", json={
        "scale_outs": _flat_list(lat_df["scaleOut"]),
        "throughput_rates": _flat_list(lat_df["avgThr"]),
        "latencies": _flat_list(lat_df["avgLat"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    rt_df = copy.deepcopy(df[df["isBckPres"] == 1])
    response = client.post("/recoverytime/training", json={
        "scale_outs": _flat_list(rt_df["scaleOut"]),
        "max_throughput_rates": _flat_list(rt_df["avgThr"]),
        "job": "TEST"
    })

    assert response.status_code == 200
    task_hash: int = int(response.json()["task_hash"])

    running: bool = True
    while running:
        response = client.get(f"/common/tasks/{task_hash}")
        assert isinstance(response.json(), bool)
        running = bool(response.json())
        time.sleep(1)

    response = client.post("/latency/prediction", json={
        "min_scale_out": 2,
        "max_scale_out": 24,
        "scale_out": 12,
        "throughput_rate": 50379.1,
        "job": "TEST"
    })

    response_json = response.json()
    print("lat_response", response_json)
    response_json["job"] = "TEST"
    response_json["workload"] = TimeSeries.create_example(600, offset=6001).dict()
    response_json["prediction_period_in_s"] = 600
    response_json["downtime"] = 10
    response_json["last_checkpoint"] = 90
    response_json["max_recovery_time"] = 240

    response = client.post("/recoverytime/evaluation", json=response_json)

    assert response.status_code == 200
    print("rt_response", response.json())

