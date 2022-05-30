import copy
import logging
import math
import os
from typing import Tuple, List
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from app.common.configuration import GeneralSettings
from app.common.models import BasePredictionModel
from app.common.schemes import JobModel
from app.workload.models import WorkloadModelProvider
from app.workload.schemes import WorkloadModelTrainingRequest, TimeSeries, WorkloadModelPredictionRequest

general_settings: GeneralSettings = GeneralSettings()


def _get_test_random_data() -> Tuple[TimeSeries, TimeSeries, TimeSeries]:
    return TimeSeries.create_example(6000, offset=1), \
           TimeSeries.create_example(1200, offset=6001), \
           TimeSeries.create_example(600, offset=7201)


def _get_ads_data(seconds: int) -> Tuple[TimeSeries, TimeSeries, TimeSeries]:
    amplitude: int = 100000
    vertical_phase: int = 100000
    period: int = 7200
    logging.info(f"---> Use parameters: amplitude={amplitude}, vertical_phase={vertical_phase}, period={period}")

    train: int = seconds
    update: int = 600
    test: int = 600

    timestamps = np.arange(train + update + test)
    values = np.sin(np.hstack((np.linspace(0, 2 * np.pi, period),) * math.ceil(len(timestamps) / period)))
    values *= amplitude
    values += vertical_phase
    values = values[:(train + update + test)]
    # add some noise
    np.random.seed(42)
    values = np.abs(values + (values * np.random.normal(0, 0.01, len(values))))

    logging.info(f"---> min={np.amin(values)}, max={np.amax(values)}")

    return TimeSeries.fold(timestamps[:train], values[:train]), \
           TimeSeries.fold(timestamps[train:(train + update)], values[train:(train + update)]), \
           TimeSeries.fold(timestamps[-test:], values[-test:])


def _get_cars_data(seconds: int) -> Tuple[TimeSeries, TimeSeries, TimeSeries]:
    path_to_file: str = os.path.join(general_settings.root_dir, "cars_1D_1S_35K_small.csv")
    logging.info(f"---> Use file: {path_to_file}")
    df = pd.read_csv(path_to_file, sep="|")  # 6 hours
    df["value"] *= 5  # "5 generators simultaniously to give us our throughput rates"

    train: int = seconds
    update: int = 600
    test: int = 600

    concat_df = pd.concat([df] * math.ceil((train + update + test) / len(df)))  # repeat X times to get more data

    timestamps = np.arange(train + update + test)
    values = concat_df["value"].values.reshape(-1)[:(train + update + test)]
    # add some noise
    np.random.seed(42)
    values = np.abs(values + (values * np.random.normal(0, 0.01, len(values))))

    logging.info(f"---> min={np.amin(values)}, max={np.amax(values)}")

    return TimeSeries.fold(timestamps[:train], values[:train]), \
           TimeSeries.fold(timestamps[train:(train + update)], values[train:(train + update)]), \
           TimeSeries.fold(timestamps[-test:], values[-test:])


async def prepare_workload_models(job_names: List[str]):

    for job_name in job_names:
        logging.info(f"Prepare workload-model for '{job_name}' job...")
        workload_model: BasePredictionModel = WorkloadModelProvider.get_instance().__call__(JobModel(job=job_name))

        train_data: TimeSeries
        update_data: TimeSeries
        test_data: TimeSeries

        if job_name == "ADS":
            train_data, update_data, test_data = _get_ads_data(60 * 60 * 24 * 1)
        elif job_name == "CARS":
            train_data, update_data, test_data = _get_cars_data(60 * 60 * 24 * 1)
        elif job_name == "TEST":
            train_data, update_data, test_data = _get_test_random_data()
        else:
            continue

        logging.info(f"---> Start training with {train_data.count} samples...")
        workload_model.fit(WorkloadModelTrainingRequest(**{
            "workload": train_data.dict(),
            "job": job_name
        }))
        logging.info("---> Training finished.")

        if test_data is not None:
            logging.info(f"---> Start testing with {test_data.count} samples (and {update_data.count} update samples)...")
            workload_model_copy: BasePredictionModel = copy.deepcopy(workload_model)
            response: TimeSeries = workload_model_copy.predict(WorkloadModelPredictionRequest(**{
                "workload": update_data.dict(),
                "prediction_period_in_s": test_data.count,
                "job": job_name
            }), save=False)

            plt.figure(figsize=(10, 5))
            plt.title(job_name)

            for name, ts in zip(["update", "test"], [update_data, test_data]):
                orig_timestamps, orig_vals = TimeSeries.unfold(ts)
                pros_timestamps, pros_vals = TimeSeries.unfold(workload_model_copy._process_workload(ts, 30, (51, 3)))

                plt.plot(orig_timestamps, orig_vals, label=name)

                if name == "update":
                    plt.plot(pros_timestamps, pros_vals, label=f"{name}_processed")

                if name == "test":
                    pred_timestamps, pred_vals = TimeSeries.unfold(response)

                    plt.plot(pred_timestamps, pred_vals, label=f"{name}_pred")

                    logging.info("---> Predict-Performance of Workload-Regressor after initial training: " +
                                 workload_model_copy.get_regression_results(orig_vals, pred_vals))

            plt.legend()
            plt.tight_layout()
            plt.show()

            logging.info("---> Testing finished.")
        logging.info(f"Workload-model for '{job_name}' workload prepared.")
