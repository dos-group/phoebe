import argparse
import asyncio
from datetime import datetime
import logging
import os
import sys

import uvicorn

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["PYTHONPATH"] = parent_dir + ":" + os.environ.get("PYTHONPATH", "")
sys.path.append(parent_dir)

from app.prepare_model import prepare_workload_models
from common.logging_utils import init_logging
from common.configuration import GeneralSettings

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-ll", "--logging-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level to use.", default=None)
    parser.add_argument("-nw", "--num-workers", type=int,
                        help="Number of workers.", default=None)
    args = parser.parse_args()

    general_settings: GeneralSettings = GeneralSettings.get_instance()

    logging_level = general_settings.logging_level
    if args.logging_level is not None:
        logging_level = args.logging_level

    log_file_name: str = f"{general_settings.app_env}_{datetime.now().isoformat()}.log"
    init_logging(logging_level, os.path.join(general_settings.log_dir, log_file_name))

    num_workers = general_settings.num_workers
    if args.num_workers is not None:
        num_workers = args.num_workers

    asyncio.get_event_loop().run_until_complete(prepare_workload_models(general_settings.job_names))

    logging.info("Start uvicorn-server...")
    uvicorn.run("server:app",
                workers=num_workers,
                timeout_keep_alive=30,
                host=general_settings.host,
                port=general_settings.port)
