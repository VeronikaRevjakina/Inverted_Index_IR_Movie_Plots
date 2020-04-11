import os
from collections import deque

import ndjson
import pandas as pd

from constants import FINAL_INDEX_PATH, TOP_CUT, DATA_PATH
from process_data import tokenize_query
import numpy as np

keywords: set = set(["and", "or"])
files: list = sorted(os.listdir(FINAL_INDEX_PATH))
file_names: list = [os.path.splitext(file)[0] for file in files]


def get_posting_list(token: str) -> dict:
    """
    Get posting list of token

    """
    posting_list: dict = dict()
    for file in file_names:
        if token <= file:
            with open(FINAL_INDEX_PATH + "{}.json".format(file),
                      "r") as file_r:  # ,buffering=const_size_in_bytes
                while file_r:  # while not eof
                    line = file_r.readline()
                    if line:
                        term, posting_list_r = ndjson.loads(line)[0]
                        if token == term:  # if found token in document
                            return posting_list_r
                        elif token < term:  # optimization, not go through all document, if not found token,
                            # but token already bigger value than term
                            print("term not found")
                            return posting_list


def query_processing(query_in: str
                     ) -> dict:
    """

    Processing whole query with output result in console
    """
    query_list = tokenize_query(query_in)
    operations: deque = deque(query_list[1::2])  # due to assumptions on query structure only even positions
    query_words_deque: deque = deque(query_list[::2])  # same only uneven
    operations_set: set = set(np.unique(operations))

    if operations_set.intersection(keywords) != operations_set:  # check operations only contains keywords
        print("The query doesn't match the format.")
        return {}  # quit processing

    if len(query_words_deque) == 1:  # if only 1 word, ignore latest operation
        post_list = get_posting_list(query_words_deque.popleft())
        print_docs_from_posting_lists(post_list)
        return post_list


def print_docs_from_posting_lists(posting_list: dict):
    """

    Print in console document response on query (plot)
    """
    docs_dict: dict = dict(sorted(posting_list.items(),
                                  key=lambda x: x[1], reverse=True))  # sort posting lists by tf as doc_id:tf
    docs_ids: np.array = np.array(list(posting_list.keys()), dtype=int)
    print("Overall found documents:", len(docs_ids))
    docs_ids = docs_ids[:TOP_CUT]  # leave only top k items for response
    docs_ranks = np.array(list(docs_dict.values()), dtype=int)[:TOP_CUT]
    for docID, rank in zip(docs_ids, docs_ranks):
        text = pd.read_csv(DATA_PATH, skiprows=docID+1, nrows=1, header=None).values[0]
        print("Document id: ", docID)
        print("Times mentioned in plot: ", rank, "\n")
        print(text)  # print all info correspond to query


if __name__ == "__main__":
    query = input('Enter your query:')
    res = query_processing(query)

