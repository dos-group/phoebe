import logging
from functools import lru_cache
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel
from sklearn.cluster import Birch
from sklearn.compose import make_column_transformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import FunctionTransformer, RobustScaler, StandardScaler

from app.common.models import BasePredictionModel, BaseProvider
from app.common.schemes import ResponseFinalizer
from app.latency.schemes import LatencyModelTrainingRequest, LatencyModelPredictionRequest, LatencyInformationModel, \
    LatencyModelEvaluationRequest


class ClusteringResult(BaseModel):
    birch_label: int
    correct_label: Optional[int]
    num: int
    min: float
    max: float


class LatencyModelImpl(BasePredictionModel):

    ESTIMATOR_FACTOR: int = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clustering_results: List[ClusteringResult] = []

        self.previous_data: Optional[pd.DataFrame] = None
        self.previous_stats: Optional[pd.DataFrame] = None

        self.clustering: Pipeline = make_pipeline(
            make_column_transformer(
                (RobustScaler(quantile_range=(0.0, 1.0)), ["latencies"]),
                remainder='drop'
            ),
            FunctionTransformer(LatencyModelImpl.log_transform),
            Birch(n_clusters=2, compute_labels=True))

        # column order: scale_outs, throughput_rates, latencies
        self.regressor: Pipeline = make_pipeline(
            make_column_transformer(
                (StandardScaler(), ["scale_outs", "throughput_rates"]),
                remainder='drop'
            ),
            GradientBoostingRegressor(max_depth=10)
        )

    @staticmethod
    def log_transform(x: np.ndarray):
        return np.log(np.maximum(1, x))

    def _learn_correct_mapping(self, data: pd.DataFrame):
        birch_instance: Birch = self.clustering.steps[-1][-1]
        birch_labels: List[int] = sorted(list(set(birch_instance.labels_)))
        clustering_results: List[ClusteringResult] = []
        for label in birch_labels:
            try:
                selection: np.ndarray = data[["latencies"]].values[data[["birch_labels"]].values.reshape(-1) == label]
                clustering_results.append(ClusteringResult(birch_label=label,
                                                           num=len(selection),
                                                           min=np.amin(selection),
                                                           max=np.amax(selection)))
            except ValueError:
                pass
        log_string: str = f"Fit-Result of Latency-Clustering: labels={birch_labels}"
        for idx, clus_res in enumerate(sorted(clustering_results, key=lambda res: res.min)):
            clus_res.correct_label = idx
            log_string += f", ClusteringResult({clus_res})"
        logging.info(log_string)
        self.clustering_results = sorted(clustering_results, key=lambda res: res.correct_label)
        return self._relabel(data)

    def _relabel(self, data: pd.DataFrame):
        for clus_res in self.clustering_results:
            data.loc[data.birch_labels == clus_res.birch_label, "latency_labels"] = clus_res.correct_label
        return data

    def _fit(self, request: LatencyModelTrainingRequest):
        data: pd.DataFrame = pd.DataFrame.from_dict(request.dict(exclude={'job'}))

        if request.append and self.previous_data is not None:
            data = pd.concat([self.previous_data, data], ignore_index=True)

        # fit clustering algorithm
        self.clustering.fit(data)
        data["birch_labels"] = self.clustering.predict(data)
        data = self._learn_correct_mapping(data)

        # fit regressor
        self.regressor.steps[-1][-1].n_estimators = LatencyModelImpl.ESTIMATOR_FACTOR * len(data)
        self.regressor.fit(data, data[["latencies"]].values.reshape(-1))
        regression_results: str = LatencyModelImpl.get_regression_results(data[['latencies']].values,
                                                                          self.regressor.predict(data))
        logging.info(f"Fit-Performance of Latency-Regressor "
                     f"(n_estimators={self.regressor.steps[-1][-1].n_estimators}): {regression_results}")

        tuple_list: List[tuple] = [("count", str(len(data)))] + \
                                  [(f"cluster_{clus_res.correct_label}", str(clus_res)) for
                                   clus_res in self.clustering_results] + [("regressor", regression_results)]

        stats: pd.DataFrame = pd.DataFrame([dict(tuple_list)])
        if request.append and self.previous_stats is not None:
            stats = pd.concat([self.previous_stats, stats], ignore_index=True)

        self.previous_stats = stats
        self.previous_data = data.drop(columns=['birch_labels', 'latency_labels'])

    def _prepare_response(self, data: pd.DataFrame, current_scale_out: int, previously_valid_scale_outs: List[int]):

        # predict latency with regressor
        data["latencies"] = self.regressor.predict(data)
        # predict latency class with clustering
        data["birch_labels"] = self.clustering.predict(data)
        data = self._relabel(data)
        # extract desired columns
        tuple_list: List[Tuple[int, int, float]] = data[["scale_outs", "latency_labels", "latencies"]].values.tolist()
        # finalize response and return (current, candidates)
        return ResponseFinalizer.run(tuple_list, current_scale_out,
                                     LatencyInformationModel, 1, previously_valid_scale_outs)

    def evaluate(self, request: LatencyModelEvaluationRequest):
        data: pd.DataFrame = pd.DataFrame([c.dict() for c in request.candidates])
        data["throughput_rate"] = request.predicted_throughput_rate
        data.rename(columns={'scale_out': 'scale_outs',
                             'throughput_rate': 'throughput_rates'}, inplace=True)
        # drop potential duplicates
        data.drop_duplicates(subset=['scale_outs'], inplace=True)
        # return prepared response
        previously_valid_scale_outs: List[int] = [c.scale_out for c in request.candidates if c.is_valid]
        return self._prepare_response(data, request.current.scale_out, previously_valid_scale_outs)

    def predict(self, request: LatencyModelPredictionRequest):
        data: pd.DataFrame = pd.DataFrame([request.dict(exclude={'min_scale_out', 'max_scale_out', 'job'})])
        scale_out_range: List[int] = list(range(request.min_scale_out, request.max_scale_out + 1))
        data.rename(columns={'scale_out': 'scale_outs',
                             'throughput_rate': 'throughput_rates'}, inplace=True)
        data = pd.concat([data] * len(scale_out_range))
        data["scale_outs"] = scale_out_range
        # return prepared response
        return self._prepare_response(data, request.scale_out, list(scale_out_range))


class LatencyModelProvider(BaseProvider):
    def __init__(self):
        super().__init__(LatencyModelImpl, "latency")

    @staticmethod
    @lru_cache()
    def get_instance():
        return LatencyModelProvider()
