# -*- coding: utf-8 -*-

"""
This script is clean the metadata from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/pipeline/clean.py
"""

import pathlib

import click
import pandas as pd
import pyarrow

from src.utils import set_logger, parse_config
from src.nlp import remove_html


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def clean(config_file):
    """
    Clean Guardian Metadata
    Args:
       config_file [str]: path to config file
    Returns:
       None
    """

    # Configure Logger
    logger = set_logger("./logs/clean.log")

    # Load Config from Config File
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)
    logger.info(f"config: {config['clean']} parsed")

    # Load Metadata
    file_path = pathlib.Path(config["clean"]["interim_file_path"]) / "metadata.parquet"
    df = pd.read_parquet(file_path)

    # Only keep english Articles
    only_articles = df["type"] == "article"
    only_english = df["fields_lang"] == "en"
    df = df[only_english & only_articles]

    # Only keep Articles where Text data is available

    # Remove prefix from column names
    df.columns = [col.replace("fields_", "") for col in df.columns]

    # Remove irrelevant columns
    drop_columns = [
        "sectionId",
        "apiUrl",
        "pillarId",
        "main",
        "bylineHtml",
        "thumbnail",
        "displayHint",
    ]
    keep_columns = [col for col in df.columns if col not in drop_columns]
    df = df.loc[:, keep_columns]

    # Recode variables
    for col in [
        "showAffiliateLinks",
        "isInappropriateForSponsorship",
        "sensitive",
        "commentable",
    ]:
        df[col] = df[col].map({0: False, 1: True})

    # Remove HTML Tags
    for col in ["standfirst", "trailText"]:
        df[col] = df[col].apply(lambda x: remove_html(str(x)))

    # Convert Variable Types

    # Store locally
    output_file_path = (
        pathlib.Path(config["clean"]["cleaned_file_path"]) / "metadata_cleaned.parquet"
    )
    df.to_parquet(output_file_path)
    logger.info(f"Cleaned metadata and stored here: {output_file_path}")


if __name__ == "__main__":
    clean()
