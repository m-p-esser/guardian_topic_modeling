# -*- coding: utf-8 -*-

"""
This script will be used to preprocess the Guardian Raw Text Data
It is designed to be idempotent [stateless transformation]
Usage:
    python3 ./src/pipeline/preprocess.py
"""

import pathlib
import re

import click
import gensim
from gensim.parsing.preprocessing import (
    remove_stopwords,
    strip_punctuation,
    strip_multiple_whitespaces,
)
from gensim.utils import tokenize, simple_tokenize
from gensim import corpora
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup

from src.utils import parse_config, set_logger


def remove_html(document):
    html_pattern = r"<.*?>"
    document = re.sub(html_pattern, "", document)
    return document


def remove_urls(document):
    url_pattern = r"http\S+"
    document = re.sub(url_pattern, "", document)
    return document


def remove_punctuation(document):
    document = document.translate(str.maketrans("", "", document.punctuation))
    return document


class HtmlSubdirsCorpus(object):
    """
    Iterable: on each iteration, process one document at a time using generators, never load the entire corpus into RAM.
    """

    def __init__(self, corpus_directory, config):
        self.corpus_directory = corpus_directory
        self.config = config
        self.dictionary = corpora.Dictionary(
            self.load_documents(corpus_directory, config)
        )

    def preprocess_document(self, config, document):
        if config["preprocess"]["remove_stop_words"]:
            document = remove_stopwords(document)
        if config["preprocess"]["lemmatize"]:
            lemmatizer = WordNetLemmatizer()
            document = lemmatizer.lemmatize(document)
        if config["preprocess"]["remove_puncutation"]:
            document = strip_punctuation(document)
        if config["preprocess"]["strip_multiple_whitespaces"]:
            document = strip_multiple_whitespaces(document)
        return document

    def load_documents(self, corpus_directory, config):
        file_paths = pathlib.Path(corpus_directory).rglob("*.html")
        for file_path in file_paths:
            with open(file_path, mode="r", encoding="utf-8") as fp:
                soup = BeautifulSoup(fp, "html.parser")
                document = soup.get_text()
                document = self.preprocess_document(config, document)
                tokens = tokenize(document)
                yield tokens  # this creates a generator (= iterator, you can only iterate over them once)

    def __iter__(self):
        for tokens in self.load_documents(self.corpus_directory, self.config):
            dictionary = corpora.Dictionary.doc2bow(self.dictionary, tokens)
            yield dictionary


@click.command()
@click.argument("config_file", type=str, default="src/config.yml")
def preprocess(config_file):
    """
    Preprocess Guardian Text files
    Args:
        config_file [str]: path to config file
    Returns:
        corpus [HtmlSubdirsCorpus]: Streamable Corpus
    """

    # Configure Logger
    logger = set_logger("./logs/preprocess.log")

    # Load Config from Config File
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)
    logger.info(f"config: {config['preprocess']} parsed")

    # Load Corpus lazely
    corpus = HtmlSubdirsCorpus(corpus_directory="./data/raw/body", config=config)
    # for vector in corpus:
    #     print(vector)


if __name__ == "__main__":
    preprocess()
