import os
import sys
from collections import defaultdict
from typing import TextIO

import ndjson
import pandas as pd
import psutil

from constants import BLOCK_SIZE, MEMORY_LIMIT, THRESHOLD
from process_data import lemma_df, tokenize_df

process = psutil.Process(os.getpid())


def build_index(
        df: pd.DataFrame(columns=["Plot"])):
    """
    SPMI algorithm build index in blocks using defaultdict()
    """
    index: defaultdict = defaultdict(list)
    file_num: int = 0
    for docID, terms_list in df.iterrows():  # stream implemented as reading rows
        for term in terms_list.values[0]:
            index[term].append(docID)  # add posting list, check for existing inside
        if psutil.virtual_memory().available < BLOCK_SIZE or (
                sys.getsizeof(index) / 1024) >= MEMORY_LIMIT or process.memory_info()[0] > THRESHOLD:
            index.pop("", None)
            file_num = write_block_to_file(index, file_num)
            index = defaultdict(list)


def write_block_to_file(
        index: defaultdict(list),
        file_num: int,
) -> int:
    """
    Write defaultdict() to json file
    :rtype: file_num for naming
    """
    dict_for_index = sorted(index.items())
    ndjson.dump(dict_for_index, open("index" + str(file_num) + ".json", 'w'))
    file_num = file_num + 1
    return file_num


def create_directories():
    """
    Create packages for data storing.
    """
    for folder in ["data/preprocessed", "data/files_index"]:
        if not os.path.isdir(folder):
            os.makedirs(folder)


def process_data(
        df: pd.DataFrame(columns=["Plot"]),
) -> pd.DataFrame(columns=["Plot"]):
    """
    Tokenizing and lemmatizing input dataFrame.
    """
    df['Plot'] = tokenize_df(df)
    df = lemma_df(df)
    return df


if __name__ == "__main__":
    create_directories()
    prep_file: TextIO
    with open('data/preprocessed/tokenized_dataset.csv', "w") as prep_file:
        for chunk in pd.read_csv("wiki_movie_plots_deduped.csv",
                                 usecols=["Plot"],  # add "Title"
                                 chunksize=100000,
                                 ):
            chunk_processed: pd.DataFrame(columns=["Plot"]
                                          ) = process_data(chunk)
            build_index(chunk_processed)
            # chunk_processed.to_csv(prep_file, header=None, index=False)
