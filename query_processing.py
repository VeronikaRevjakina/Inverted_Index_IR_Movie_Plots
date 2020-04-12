import os
from collections import deque

import ndjson
import pandas as pd
from nltk import WordNetLemmatizer

from constants import FINAL_INDEX_PATH, TOP_CUT, DATA_PATH
from operations_posting_lists import union_posting_lists, intersect_posting_lists
from process_data import tokenize_query
import numpy as np

keywords: set = set(["and", "or"])
files: list = sorted(os.listdir(FINAL_INDEX_PATH))
file_names: list = [os.path.splitext(file)[0] for file in files]


def get_posting_list_for_token(token: str) -> dict:
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
                            return dict(sorted(posting_list_r.items()))  # order by doc_ids for operations
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
    if query_words_deque:
        final_postings: dict = get_processed_posting_list_operations(query_words_deque.copy(), operations)
        print_docs_from_posting_lists(final_postings)


def get_processed_posting_list_operations(query_words_deque: deque, operations: deque) -> dict:
    left_word_query: str = query_words_deque.popleft()  # get first input word
    left_word_query = WordNetLemmatizer().lemmatize(left_word_query)
    left_dict_post_list: dict = get_posting_list_for_token(left_word_query)

    if len(query_words_deque) == 0:  # if only 1 word in query, ignore latest operation
        return left_dict_post_list
    else:
        try:
            while query_words_deque:
                right_word_query: str = query_words_deque.popleft()  # get next word
                right_word_query = WordNetLemmatizer().lemmatize(right_word_query)
                right_dict_post_list: dict = get_posting_list_for_token(right_word_query)
                curr_operation: str = operations.popleft()  # current operation match keywords

                if curr_operation == "or":
                    if right_dict_post_list:
                        left_dict_post_list = union_posting_lists(left_dict_post_list,
                                                                  right_dict_post_list)  # update left postings with
                        # united value
                elif curr_operation == "and":
                    if right_dict_post_list:
                        left_dict_post_list = intersect_posting_lists(left_dict_post_list,
                                                                      right_dict_post_list)  # update left postings
                        # with intersect value
                        continue

        except IndexError:
            print("End of query")

    return left_dict_post_list


def print_docs_from_posting_lists(posting_list: dict):
    """

    Print in console document response on query (plot)
    """
    docs_dict: dict = dict(sorted(posting_list.items(),
                                  key=lambda x: x[1], reverse=True))  # sort posting lists by tf as doc_id:tf
    docs_ids: np.array = np.array(list(docs_dict.keys()), dtype=int)
    print("Overall found documents:", len(docs_ids))
    docs_ids = docs_ids[:TOP_CUT]  # leave only top k items for response
    docs_tf = np.array(list(docs_dict.values()), dtype=int)[:TOP_CUT]
    # docs_tf = [docs_dict[str(key)] for key in docs_ids]
    for doc_id, tf in zip(docs_ids, docs_tf):
        print("Document id: ", doc_id)
        print("Times mentioned in plot: ", tf, "\n")
        text = pd.read_csv(DATA_PATH, skiprows=doc_id, nrows=1).values[0]

        print(text)  # print all info correspond to query


if __name__ == "__main__":
    query = input('Enter your query:')
    res = query_processing(query)
