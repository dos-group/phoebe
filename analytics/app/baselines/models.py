from functools import lru_cache
from typing import Optional
import numpy as np
import scipy as sp
import scipy.optimize
from app.baselines.schemes import *
from app.common.models import BasePredictionModel, BaseProvider
from app.common.configuration import GeneralSettings
from app.workload.models import WorkloadModelImpl
from app.workload.schemes import WorkloadModelPredictionRequest

general_settings: GeneralSettings = GeneralSettings()


class TWRESModelImpl(BasePredictionModel):
    # Time Window Resource Elasticity Scaling Algorithm (TWRES)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mst_coeffs: Optional[np.ndarray] = None

    @staticmethod
    def mst_fmap(x: np.ndarray):
        return 1 / np.c_[np.ones_like(x), 1. / x, x, x ** 2]

    def fit_mst_model(self, request: TWRESTrainingRequest):
        # fit MST (maximum sustainable throughput) model
        x, y = np.array(request.scale_outs).flatten(), np.array(request.throughput_rates).flatten()
        x_arr = TWRESModelImpl.mst_fmap(x)
        coeffs, res = sp.optimize.nnls(x_arr, y)
        self.mst_coeffs = coeffs

    def calculate_mstw(self, scale_outs: List[int], time_interval: float):
        # calculate MSTW (maximum sustainable throughput of time window)
        x = np.array(scale_outs).flatten()
        x_arr = self.mst_fmap(x)
        mst: np.ndarray = np.dot(x_arr, self.mst_coeffs)
        mstw: np.ndarray = mst * time_interval
        return mstw

    def calculate_min_opt_scale_out(self, throughput: float, request: TWRESPredictionRequest):
        test_scale_outs: List[int] = list(range(request.min_scale_out, request.max_scale_out + 1))
        mstw_list: List[float] = self.calculate_mstw(test_scale_outs, request.time_window_interval).reshape(-1).tolist()
        target_idx: int = next((idx for idx, mstw in enumerate(mstw_list) if mstw > throughput), -1)
        min_opt_scale_out: int
        if target_idx >= 0:
            # case: we actually select the minimum scale-out that meets the prospective load
            min_opt_scale_out = test_scale_outs[target_idx]
        else:
            # case: we select the scale-out that is closest to the prospective load
            min_opt_scale_out = test_scale_outs[np.argmax(np.array(mstw_list) - throughput)]
        return min_opt_scale_out

    def _fit(self, request: TWRESTrainingRequest):
        self.fit_mst_model(request)

    def predict(self, request: TWRESPredictionRequest, workload_model: WorkloadModelImpl):
        # predict future load
        horizon_s: int = request.time_window_interval
        pred_workload = workload_model.predict(WorkloadModelPredictionRequest(workload=request.workload,
                                                                              job=request.job,
                                                                              prediction_period_in_s=horizon_s))
        _, pred_throughput_rates = TimeSeries.unfold(pred_workload)
        pred_throughput = sum(list(pred_throughput_rates))
        # determine minimum scale-out that still fulfills SLA
        min_opt_scale_out: int = self.calculate_min_opt_scale_out(pred_throughput, request)
        # calculate mstw
        mstw: float = self.calculate_mstw([request.scale_out], horizon_s).reshape(-1)[0]
        # now run twres algorithm
        new_scale_out: int
        if pred_throughput >= mstw:
            new_scale_out = min_opt_scale_out
        else:
            if request.avg_latency > request.max_latency_constraint:
                new_scale_out = request.scale_out + 1
            else:
                new_scale_out = min_opt_scale_out
        return new_scale_out


class TWRESModelProvider(BaseProvider):
    def __init__(self):
        super().__init__(TWRESModelImpl, "twres")

    @staticmethod
    @lru_cache()
    def get_instance():
        return TWRESModelProvider()
