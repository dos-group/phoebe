import logging
from importlib import reload
from typing import Optional

from pydantic import BaseModel

from app.common.models import BasePredictionModel


def log_request(request_model: BaseModel, pred_model: Optional[BasePredictionModel] = None):
    string_one: str = f"job={getattr(request_model, 'job')}, " if hasattr(request_model, "job") else ""
    string_two: str = f"model_path={pred_model.target_path.split('/')[-1]}, " if pred_model is not None else ""
    logging.info(f"{string_one}{string_two}request=[{request_model}]")


def init_logging(logging_level: str, file_name: str):
    """
    initialize logging setting
    :param logging_level:
    :param file_name:
    :return:
    """
    reload(logging)
    logging.basicConfig(
        level=getattr(logging, logging_level.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s [%(module)s.%(funcName)s]__[L%(lineno)d]]",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(file_name)
        ]
    )

    logging.info(f"Successfully initialized logging.")
