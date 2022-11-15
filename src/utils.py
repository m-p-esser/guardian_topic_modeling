""" Utility Functions """

import logging
import pathlib
import yaml

import pandas as pd


def parse_config(config_file):
    """ Parse YAML config file """
    with open(config_file, "rb") as f:
        config = yaml.safe_load(f)
    return config


def set_logger(log_path):
    """
    Read more about logging: https://www.machinelearningplus.com/python/python-logging-guide/
    Args:
        log_path [str]: eg: "../log/train.log"
    """
    log_path = pathlib.Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_path, mode="a")
    formatter = logging.Formatter(
        "%(asctime)s : %(levelname)s : %(name)s : %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info(f"Finished logger configuration!")
    return logger


def create_data_range(start_date, end_date):
    """ Create a Data Range List containing strings in format YYYY-MM-DD """
    date_range = pd.date_range(start=start_date, end=end_date)
    return [d.strftime('%Y-%m-%d') for d in date_range]
