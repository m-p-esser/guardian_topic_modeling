# -*- coding: utf-8 -*-

"""
These functions are used to extract data from Guardian Content API and store them in a local folder
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/extract.py
"""

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pathlib
import os

import pandas as pd


def store_api_data(config, page, date):
    """
    Wrapper function that stores raw (meta)data for a single day
    Args:
        config [Dict]: Dictionary containing configuration
        page [int]: Page Number
        date [str]: Date String in "%Y-%m-%d" format
    Returns:
        response [Dict]: Dictionary containing Response from API
    """
    response = request_content_api(config, page, date)
    results = response['results']

    # Store the actual Content data
    body_output_dir = pathlib.Path(config["extract"]["raw_data_body_file_path"]) / date
    body_output_dir.mkdir(exist_ok=True)
    for result in results:
        store_content_text(result, body_output_dir)

    # Store the Metadata
    metadata_output_dir = pathlib.Path(config["extract"]["raw_data_metadata_file_path"])
    store_content_metadata(response, metadata_output_dir, date)

    return response


def request_content_api(config, page, date):
    """
    Construct Request Parameter and call API
    Args:
        config [Dict]: Dictionary containing configuration
        page [int]: Page Number
        date [str]: Date String in "%Y-%m-%d" format
    Returns:
        response [Dict]: Dictionary containing Response from API
    """

    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    params = {
        'from-date': date,
        'to-date': date,
        'order-by': config["extract"]["order_by"],
        'show-fields': config["extract"]["show_fields"],
        'page-size': config["extract"]["page_size"],
        'page': page,
        'api-key': os.environ.get('API_KEY')
    }

    payload = requests.get(
        url=config["extract"]["endpoint"],
        params=params
    )

    payload.raise_for_status()
    response = payload.json()['response']

    return response


def store_content_text(result, output_dir):
    """
    Store single Content Text
    Args:
        result [Dict]: Dictionary containing single content item
        output_dir [pathlib.Path]: Pathlib Path
    """
    content_id = result['id'].replace('/', '_')
    content_text = result['fields']['body']

    file_path_out = output_dir / f'{content_id}.html'
    with open(file_path_out, "w", encoding="utf-8") as f:
        f.write(content_text)


def store_content_metadata(response, output_dir, date):
    """
    Store Content Metadata
    Args:
        response [Dict]: Dictionary containing Response from API
        output_dir [pathlib.Path]: Pathlib Path
    """
    number_results = len(response['results'])
    df = pd.json_normalize(
        response['results'],
        sep="_"
    )
    df['content_id'] = df['id'].str.replace('/','_')
    df = df.drop(columns=['id', 'fields_body'])

    if len(df) != number_results:
        raise ValueError(f"while collecting data for {response['date']} an error has been raised. Expected {len(df)} "
                         f"rows, but got {number_results} rows")

    file_path_out = output_dir / f'{date}.csv'
    df.to_csv(file_path_out, index=False, sep=";")
