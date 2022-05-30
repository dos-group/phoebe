import asyncio
import json
import os
from asyncio import Task
from typing import Type, Dict

import numpy as np
from fastapi import Request
import dill
from sklearn.metrics import mean_absolute_error, median_absolute_error, mean_absolute_percentage_error, \
    mean_squared_error

from app.common.configuration import GeneralSettings

general_settings: GeneralSettings = GeneralSettings.get_instance()


def _to_task(future, loop):
    if isinstance(future, Task):
        return future
    return loop.create_task(future)


def force_sync(func):
    if asyncio.iscoroutine(func):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        return loop.run_until_complete(_to_task(func, loop))
    else:
        return func


class BasePredictionModel:

    def __init__(self, target_path: str):
        self.target_path = target_path
        self.is_fitted: bool = False

    @staticmethod
    def get_regression_results(y_true: np.ndarray, y_pred: np.ndarray):
        mean_error: float = mean_absolute_error(y_true, y_pred)
        median_error: float = median_absolute_error(y_true, y_pred)
        map_error: float = mean_absolute_percentage_error(y_true, y_pred)
        mse_error: float = mean_squared_error(y_true, y_pred)
        return f"MeanError={mean_error:.2f}, " \
               f"MedianError={median_error:.2f}, " \
               f"MAPE={map_error*100:.2f}%, " \
               f"RMSE={np.sqrt(mse_error):.2f}, " \
               f"MSE={mse_error:.2f}"

    def fit(self, *args, **kwargs):
        self._fit(*args, **kwargs)
        self.is_fitted = True
        with open(self.target_path, "wb") as file:
            dill.dump(self, file)

    def _fit(self, *args, **kwargs):
        raise NotImplementedError

    def predict(self, *args, **kwargs):
        raise NotImplementedError


class BaseProvider:
    def __init__(self, model_class: Type[BasePredictionModel], search_str: str):
        self.model_class: Type[BasePredictionModel] = model_class
        self.search_str: str = search_str
        self.models: Dict[str, BasePredictionModel] = {}
        self.populate()

    def populate(self):
        for file_name in list(os.listdir(general_settings.models_dir)):
            if not file_name.startswith(f"{general_settings.app_env}_"):
                continue
            if f"_{self.search_str}_model.p" in file_name:
                job: str = file_name.split(f"_{self.search_str}_model.p")[0]
                with open(os.path.join(general_settings.models_dir, file_name), "rb") as file:
                    self.models[job] = dill.load(file)

    def __call__(self, request: Request) -> BasePredictionModel:
        awaited_req = force_sync(request.json())
        request_dict: dict = awaited_req if isinstance(awaited_req, dict) else json.loads(awaited_req)
        job: str = request_dict.get("job", None)
        if job is not None:
            if job in self.models:
                return self.models[job]
            else:
                model = self.model_class(os.path.join(general_settings.models_dir,
                                                      f"{general_settings.app_env}_{job}_{self.search_str}_model.p"))
                self.models[job] = model
                return model



