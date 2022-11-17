# -*- coding: utf-8 -*-

"""
This script is used to extract data from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/extract.py
"""

import dotenv
import click
import time

from utils import set_logger, parse_config, create_data_range
from guardian_api import store_api_data


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def extract(config_file):
    """
    ETL function that loads data from Guardian Content API Endpoint and stores in local file directory
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
        time.sleep(1)

        while page < response["pages"]:
            page += 1
            response = store_api_data(config, page, date)

        logger.info(f"Extracted data for: {date}")


if __name__ == "__main__":
    extract()
