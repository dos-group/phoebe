from typing import List
from pydantic import BaseModel

from app.common.schemes import JobModel
from app.workload.schemes import TimeSeries


class TWRESTrainingRequest(JobModel):
    scale_outs: List[int]
    throughput_rates: List[float]

    class Config:
        schema_extra = {
            "example": {
                "scale_outs": [2, 4, 8],
                "throughput_rates": [124.2, 248.4, 496.8],
                "job": "TEST"
            }
        }


class TWRESPredictionRequest(JobModel):
    avg_latency: float
    max_latency_constraint: float
    scale_out: int
    time_window_interval: int
    workload: TimeSeries
    min_scale_out: int
    max_scale_out: int

    class Config:
        schema_extra = {
            "example": {
                "avg_latency": 1000.0,
                "max_latency_constraint": 1234.0,
                "scale_out": 14,
                "time_window_interval": 300,
                "workload": TimeSeries.create_example(60),
                "min_scale_out": 4,
                "max_scale_out": 24,
                "job": "TEST"
            }
        }


class TWRESPredictionResponse(BaseModel):
    scale_out: int
