import os
from collections import deque

import ndjson
import numpy as np
import pandas as pd
import ast
from nltk import WordNetLemmatizer

from sklearn.metrics.pairwise import cosine_similarity
from bert_embeddings import get_model_and_tokenizer, get_embeddings_processed_by_bert
from constants import FINAL_INDEX_PATH, TOP_CUT, DATA_PATH, KEYWORDS, BERT_DATA_PATH
from operations_posting_lists import union_posting_lists, not_postings_list, \
    intersect_many_posting_lists, subtract_from_left_right_posting_lists
from process_data import tokenize_query
from query_spelling_correction import get_spelling_corrected_query

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
                            return dict(sorted(posting_list_r.items(), key=lambda x: int(x[0])))
                        elif token < term:  # optimization, not go through all document, if not found token,
                            # but token already bigger value than term
                            print("Word '{}' not found across all documents".format(token))
                            return posting_list
    return posting_list


def query_processing(query_in: str, bert_flag: bool
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
        if not bert_flag:
            print_docs_from_posting_lists_rank_tf(final_postings)
        else:
            print_docs_from_posting_lists_rank_bert(query_words_deque, final_postings)


def get_processed_posting_list_operations(query_words_deque: deque, operations: deque) -> dict:
    left_word_query: str = query_words_deque.popleft()  # get first input word
    left_word_query = WordNetLemmatizer().lemmatize(left_word_query)
    left_dict_post_list: dict = get_posting_list_for_token(left_word_query)
    multiple_postings = list()
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

        except (IndexError, StopIteration):
            # print("End of query")
            # if curr_operation == "not":  # empty right already checked
            #     left_dict_post_list = not_postings_list(left_dict_post_list)  # update left postings
            # with intersect value
            if multiple_postings:
                left_dict_post_list = intersect_many_posting_lists(multiple_postings)  # all clearly and processed here
            break
    return left_dict_post_list


def print_docs_from_posting_lists_rank_tf(posting_list: dict):
    """

    Print in console document response on query (plot) ranked by tf
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


def print_docs_from_posting_lists_rank_bert(query_words_deque: deque, posting_list: dict):
    """

    Print in console document response on query (plot) ranked by embeddings cosine similarity
    """
    query_words_str: str = " ".join(query_words_deque)
    model, tokenizer = get_model_and_tokenizer()
    tokenized = tokenizer.encode(query_words_str, add_special_tokens=True)  # tokenize for BERT input
    query_embedding = get_embeddings_processed_by_bert(model, np.array(tokenized).reshape(1, -1))  # reshape to get
    # matrix as (1,tokenized), get BERT output embedding
    query_embedding = np.array(query_embedding[0]).reshape(1, -1)  # for input to cosine similarity
    docs_dict: dict = dict(posting_list.items())  # get dict from posting_list with tf
    docs_ids: np.array = np.array(list(docs_dict.keys()), dtype=int)
    print("Overall found documents:", len(docs_ids))

    docs_dict_similarity = dict()  # new dict to store doc_id: similarity
    for doc_id in docs_ids:
        doc_embedding = pd.read_csv(BERT_DATA_PATH + f'result.csv', skiprows=doc_id, nrows=1).values[0][5]
        # 5 is hardcoded 'BERT output'
        doc_embedding = ast.literal_eval(doc_embedding)  # here list 768
        doc_embedding = np.array(doc_embedding).reshape(1, -1)  # for input to cosine similarity
        docs_dict_similarity[doc_id] = cosine_similarity(query_embedding, doc_embedding)[0][0]
    docs_dict_similarity = dict(sorted(docs_dict_similarity.items(),
                                       key=lambda x: x[1],
                                       reverse=True))  # sort docs by similarity as doc_id:similarity
    docs_ids = np.array(list(docs_dict_similarity.keys()), dtype=int)  # get from sorted
    docs_ids = docs_ids[:TOP_CUT]  # leave only top k items for response
    docs_similarity = np.array(list(docs_dict_similarity.values()))[:TOP_CUT]
    for doc_id, doc_similarity in zip(docs_ids, docs_similarity):
        print("Document id: ", doc_id)
        print("Counted cosine similarity with query: ", doc_similarity, "\n")
        text = pd.read_csv(BERT_DATA_PATH + f'result.csv', skiprows=doc_id, nrows=1).values[0]
        print(text[2])  # print title of film
        print(text[3])  # print  plot, 2 is hardcoded for plot


if __name__ == "__main__":
    query = input('Enter your query:')
    query_corrected = get_spelling_corrected_query(query)
    res = query_processing(query_corrected, True)  # True is flag for using embeddings , False for tf
