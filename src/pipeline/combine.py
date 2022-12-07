# -*- coding: utf-8 -*-

"""
This script is combine the metadata from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/pipeline/combine.py
"""

import pathlib

import click
import pandas as pd
import dask.dataframe as dd
import pyarrow

from src.utils import set_logger, parse_config


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def combine(config_file):
    """
    Load Metadata / Keywords CSV files, combine them in a Dataframe and store the save this Dataframe as Parquet file
    Args:
        input_dir [str]: input directory
        output_dir [str]: output directory
    Returns:
        None
    """

    # Configure Logger
    logger = set_logger("./logs/combine.log")

    # Load Config from Config File
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)
    logger.info(f"config: {config} parsed")

    ######################## METADATA ##################################################

    # Load as Dask Dataframe, combine and store locally as Parquet file
    file_paths = list(pathlib.Path(config["extract"]["raw_data_metadata_file_path"]).rglob("*.csv"))
    number_files = len(file_paths)
    distinct_years = list(set([int(fp.stem[0:4]) for fp in file_paths]))
    distinct_years_sorted = sorted(distinct_years)

    # Create one Parquet file per Year
    for year in distinct_years_sorted:
        urlpath = pathlib.Path(config["extract"]["raw_data_metadata_file_path"]) / f"{year}-*-*.csv"
        df = dd.read_csv(
            urlpath=urlpath,
            sep=";",
            dtype={
                'fields_commentCloseDate': 'object',
                'fields_newspaperPageNumber': 'float64',
                'fields_thumbnail': 'object',
                'fields_contributorBio': 'object',
                'fields_displayHint': 'object'
            },
        )

        df = df.compute()  # Convert to Pandas Dataframe
        output_file_path = pathlib.Path(config["combine"]["interim_file_path"]) / f"{year}_metadata.parquet"
        df.to_parquet(output_file_path)
        logger.info(
            f"Combined {number_files} of files into one file. Stored the file in following path: {output_file_path}"
        )

    # ######################## KEYWORDS ##################################################

    # Load as Dask Dataframe, combine and store locally as Parquet file
    file_paths = list(pathlib.Path(config["extract"]["raw_data_keywords_file_path"]).rglob("*.csv"))
    number_files = len(file_paths)
    distinct_years = list(set([int(fp.stem[0:4]) for fp in file_paths]))
    distinct_years_sorted = sorted(distinct_years)

    # Create one Parquet file per Year
    for year in distinct_years_sorted:
        urlpath = pathlib.Path(config["extract"]["raw_data_keywords_file_path"]) / f"{year}-*-*.csv"
        df = dd.read_csv(
            urlpath=urlpath,
            sep=";"
        )

        df = df.compute()  # Convert to Pandas Dataframe
        output_file_path = pathlib.Path(config["combine"]["interim_file_path"]) / f"{year}_keywords.parquet"
        df.to_parquet(output_file_path)
        logger.info(
            f"Combined {number_files} of files into one file. Stored the file in following path: {output_file_path}"
        )


if __name__ == "__main__":
    combine()