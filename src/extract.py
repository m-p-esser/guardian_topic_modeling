# -*- coding: utf-8 -*-

"""
This script is used to extract data from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/extract.py
"""

import dotenv
import click

from utils import set_logger, parse_config, create_data_range
from guardian_api import request_content_api, store_content_text, store_content_metadata, store_api_data


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def extract(config_file):
    """
    ETL function that load raw data and convert to train and test set
    Args:
        config_file [str]: path to config file
    Returns:
        None
    """

    # Configure Logger
    logger = set_logger("./logs/extract.log")

    # Load Environment Variables
    dotenv.load_dotenv()

    # Load Config from Config File
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)
    logger.info(f"config: {config['extract']} parsed")

    # Construct Date range
    date_from = config["extract"]["from_date"]
    date_to = config["extract"]["to_date"]
    date_range = create_data_range(date_from, date_to)

    # Call Guardian API
    for date in date_range:
        page = 1
        response = store_api_data(config, page, date)

        while page < response['pages']:
            page += 1
            response = store_api_data(config, page, date)


if __name__ == "__main__":
    extract()