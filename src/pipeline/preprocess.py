# -*- coding: utf-8 -*-

"""
This script will be used to preprocess the Guardian Raw Text Data
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/pipeline/preprocess.py
"""

import click

from utils import set_logger, parse_config
from nlp import HtmlSubdirsCorpus


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def preprocess(config_file):
    """
    Preprocess Guardian Text files
    Args:
        config_file [str]: path to config file
    Returns:
        None
    """

    # Configure Logger
    logger = set_logger("./logs/preprocess.log")

    # Load Config from Config File
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)
    logger.info(f"config: {config['extract']} parsed")

    # Load Corpus lazely
    corpus = HtmlSubdirsCorpus("./data/raw/body")


if __name__ == "__main__":
    preprocess()
