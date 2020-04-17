import os
from collections import deque

import ndjson
import numpy as np
import pandas as pd
from nltk import WordNetLemmatizer

from constants import FINAL_INDEX_PATH, TOP_CUT, DATA_PATH, KEYWORDS
from operations_posting_lists import union_posting_lists, not_postings_list, \
    intersect_many_posting_lists, subtract_from_left_right_posting_lists
from process_data import tokenize_query

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
                            print("Token '{}' is processing".format(token))
                            return dict(sorted(posting_list_r.items()))  # order by doc_ids for operations
                        elif token < term:  # optimization, not go through all document, if not found token,
                            # but token already bigger value than term
                            print("Word '{}' not found across all documents".format(token))
                            return posting_list
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

    if operations_set.intersection(KEYWORDS) != operations_set:  # check operations only contains keywords
        print("The query doesn't match the format.")
        return {}  # quit processing
    if query_words_deque:
        final_postings: dict = get_processed_posting_list_operations(query_words_deque.copy(), operations)
        print_docs_from_posting_lists(final_postings)


def get_processed_posting_list_operations(query_words_deque: deque, operations: deque) -> dict:
    left_word_query: str = query_words_deque.popleft()  # get first input word
    left_word_query = WordNetLemmatizer().lemmatize(left_word_query)
    left_dict_post_list: dict = get_posting_list_for_token(left_word_query)
    multiple_postings: list = list()
    flag_next: bool = False
    while True:
        try:
            right_word_query: str = query_words_deque.popleft()  # get next word
            right_word_query = WordNetLemmatizer().lemmatize(right_word_query)
            right_dict_post_list: dict = get_posting_list_for_token(right_word_query)
            curr_operation: str = operations.popleft()  # current operation match keywords

            if curr_operation != "and":  # and only process by multiple

                if multiple_postings:  # if reached end of and list (next operation for exp: or)
                    left_dict_post_list = intersect_many_posting_lists(multiple_postings)
                    multiple_postings = list()  # clean for future

                if curr_operation == "or":
                    if right_dict_post_list:
                        left_dict_post_list = union_posting_lists(left_dict_post_list,
                                                                  right_dict_post_list)  # update left postings with
                        # with intersect value
                elif curr_operation == "ornot":
                    left_dict_post_list = union_posting_lists(left_dict_post_list,
                                                              not_postings_list(right_dict_post_list))
                elif curr_operation == "andnot":
                    if left_dict_post_list and right_dict_post_list:
                        left_dict_post_list = subtract_from_left_right_posting_lists(left_dict_post_list,
                                                                                     right_dict_post_list)
            else:
                if not flag_next:  # there are no more combined with and tokens
                    if multiple_postings:
                        multiple_postings.append(right_dict_post_list)  # store while not reached end
                    else:
                        multiple_postings = [left_dict_post_list, right_dict_post_list]  # initialization

                if not (left_dict_post_list or right_dict_post_list):  # only empty
                    flag_next = True
                    multiple_postings = list()

        except IndexError:
            # print("End of query")
            # if curr_operation == "not":  # empty right already checked
            #     left_dict_post_list = not_postings_list(left_dict_post_list)  # update left postings
            # with intersect value
            if multiple_postings:
                left_dict_post_list = intersect_many_posting_lists(multiple_postings)  # all clearly and processed here
            break
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
