import os
from functools import lru_cache
from typing import Optional, List

from pydantic import BaseSettings

# https://fastapi.tiangolo.com/advanced/settings/?h=envir
root_dir: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class GeneralSettings(BaseSettings):
    root_dir: Optional[str] = root_dir
    logging_level: Optional[str] = "INFO"
    num_workers: Optional[int] = 1
    app_env: Optional[str] = "DEV"
    host: Optional[str] = "0.0.0.0"
    port: Optional[int] = 5000
    models_dir: Optional[str] = "artifacts/models"
    log_dir: Optional[str] = "artifacts/logs"
    job_names: List[str] = ["ADS", "CARS"]

    @staticmethod
    @lru_cache()
    def get_instance():
        general_settings: GeneralSettings = GeneralSettings()
        general_settings.models_dir = os.path.join(general_settings.root_dir, general_settings.models_dir)
        general_settings.log_dir = os.path.join(general_settings.root_dir, general_settings.log_dir)

        if not os.path.exists(general_settings.models_dir):
            os.makedirs(general_settings.models_dir)

        if not os.path.exists(general_settings.log_dir):
            os.makedirs(general_settings.log_dir)

        return general_settings
