import sys
from collections import Counter
from collections import defaultdict
from typing import TextIO

import ndjson
import pandas as pd
import numpy as np
import psutil

from constants import BLOCK_SIZE, MEMORY_LIMIT, THRESHOLD, BUILDED_INDEX_PATH, DATA_PATH
from helper_funcs import create_directories, process
from merge import multi_merge_sort
from process_data import lemma_df, tokenize_df


def build_index(
        df: pd.DataFrame(columns=["Plot"]),
        number: int,
) -> int:
    """
    SPMI algorithm build index in blocks using defaultdict()
    """
    index: defaultdict = defaultdict(list)
    for docID, terms_list in df.iterrows():  # stream implemented as reading rows
        for term in terms_list.values[0]:
            index[term].append(docID)  # add posting list, check for existing inside
        if psutil.virtual_memory().available < BLOCK_SIZE or (
                sys.getsizeof(index) / 1024) >= MEMORY_LIMIT or process.memory_info()[0] > THRESHOLD:
            number = write_block_to_file(index, number)
            index = defaultdict(list)
    if index:  # tail if not reached memory check, but processed data in index
        number = write_block_to_file(index, number)
    return number


def write_block_to_file(
        index: defaultdict(list),
        number: int,
) -> int:
    """
    Write defaultdict() to json file

    """
    index.pop("", None)
    for key in index.keys():
        index[key] = Counter(index[key])  # reformat data as doc_id: frequency_keyword_in_doc_id_file
    index = sorted(index.items())  # sort posting lists
    ndjson.dump(index, open("index" + str(number) + ".json", 'w'))
    with open(
            BUILDED_INDEX_PATH + "index{}.json".format(str(number)), "w") as file:
        ndjson.dump(index, file)
    number += 1  # store file number , used afterwards for merge
    return number


def process_data(
        df: pd.DataFrame(columns=["Plot"]),
) -> pd.DataFrame(columns=["Plot"]):
    """
    Tokenizing and lemmatizing input dataFrame.
    """
    df = tokenize_df(df)
    df = lemma_df(df)
    return df


if __name__ == "__main__":
    create_directories()
    prep_file: TextIO
    file_num: int = 0
    max_doc_id: int = -1
    with open('data/preprocessed/tokenized_dataset.csv', "w") as prep_file:
        for chunk in pd.read_csv(DATA_PATH,
                                 usecols=["Plot"],  # add "Title"
                                 chunksize=100000,
                                 header=None
                                 ):
            chunk_processed: pd.DataFrame(columns=["Plot"]
                                          ) = process_data(chunk)
            # max_doc_id = chunk.index[-1]
            file_num = build_index(chunk_processed, file_num)
            # chunk_processed.to_csv(prep_file, header=None, index=False)
        max_doc_id = np.max(list(chunk_processed.index))
        with open("tmp_store.py", "w") as temp_out:
            if max_doc_id == -1:
                temp_out.write(" Build wasn't successful")
                temp_out.write("max_doc_id: int = {}\n".format(max_doc_id))
            else:
                temp_out.write("max_doc_id: int = {}\n".format(max_doc_id))
    multi_merge_sort(file_num)
