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
            index.pop("", None)
            number = write_block_to_file(index, number)
            index = defaultdict(list)
    return number


def write_block_to_file(
        index: defaultdict(list),
        number: int,
) -> int:
    """
    Write defaultdict() to json file
    :rtype: file_num for naming
    """
    dict_for_index = sorted(index.items())  # sort posting lists
    ndjson.dump(dict_for_index, open("index" + str(number) + ".json", 'w'))
    number = number + 1  # afterwards for merge
    return number


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
    df = tokenize_df(df)
    df = lemma_df(df)
    return df


def multi_merge_sort(number: int) -> int:
    """

    :Multi merge algorithm
    """
    files = [
        open(
            "index{}.json".format(num),
            "r",
            # buffering=math.floor(const_size_in_bytes / (file_number - 1)),
        )
        for num in range(number)
    ]
    files_to_read: list = files  # pointers to read lines
    temp_dictionary: defaultdict(list) = defaultdict(
        list)  # lines from each file stored, posting lists already combined
    term_file: defaultdict(list) = defaultdict(list)  # storage for files_ro_read assignment
    final_index_dict: defaultdict(list) = defaultdict(list)
    while files:  # check eof
        for file in files_to_read:
            line = file.readline()
            if not line:
                files.remove(file)
                file.close()
                # if os.path.exists(file.name):
                #     os.remove(file.name)  # drop tmp index file
            else:
                term, posting_list = ndjson.loads(line)[0]
                temp_dictionary[term].append(posting_list)
                term_file[term].append(file)
                # after read lines from each file
        if temp_dictionary.keys():
            min_term = min(temp_dictionary.keys())
            min_posting_list = temp_dictionary.pop(min_term)
            files_to_read = term_file.pop(min_term)  # read next line on files where min term was
            final_index_dict[min_term].append(min_posting_list)
            if psutil.virtual_memory().available < BLOCK_SIZE or MEMORY_LIMIT <= (
                    sys.getsizeof(final_index_dict) / 1024) or process.memory_info()[0] > THRESHOLD:
                write_to_file(min_term, final_index_dict)
                final_index_dict = defaultdict(list)  # clean out orm


def write_to_file(file_name: int, index: defaultdict(list)):
    dict_for_index = sorted(index.items())  # sort posting lists
    with open("data/files_index/{}.json".format(file_name), 'w') as file:
        ndjson.dump(dict_for_index, file)
    # with open("data/index/{}.json".format(file_name), "wb") as file:
    #     pickle.dump(dict_for_index, file)


if __name__ == "__main__":
    create_directories()
    prep_file: TextIO
    file_num: int = 0
    with open('data/preprocessed/tokenized_dataset.csv', "w") as prep_file:
        for chunk in pd.read_csv("wiki_movie_plots_deduped.csv",
                                 usecols=["Plot"],  # add "Title"
                                 chunksize=100000,
                                 ):
            chunk_processed: pd.DataFrame(columns=["Plot"]
                                          ) = process_data(chunk)
            file_num = build_index(chunk_processed, file_num)
            # chunk_processed.to_csv(prep_file, header=None, index=False)
    multi_merge_sort(file_num)
