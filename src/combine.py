# -*- coding: utf-8 -*-

"""
This script is combine the metadata from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/combine.py
"""

import pathlib

import click
import pandas as pd

from utils import set_logger


@click.command()
@click.argument("input_dir", type=str, default="./data/raw/metadata")
@click.argument("output_dir", type=str, default="./data/interim")
def combine(input_dir, output_dir):
    """
    Load Metadata CSV files, combine them in a Dataframe and store the save this Dataframe as Feather file
    Args:
        input_dir [str]: input directory
        output_dir [str]: output directory
    Returns:
        None
    """

    # Configure Logger
    logger = set_logger("./logs/extract.log")

    # Load, combine and store
    file_paths = list(pathlib.Path(input_dir).rglob("*.csv"))
    number_files = len(file_paths)
    df = pd.concat([pd.read_csv(fp, sep=";") for fp in file_paths])
    file_path_out = pathlib.Path(output_dir) / 'metadata.feather'
    df.reset_index().to_feather(file_path_out)
    logger.info(f"Combined {number_files} of files into one file. Stored the file in following path: {file_path_out}")


if __name__ == "__main__":
    combine()
