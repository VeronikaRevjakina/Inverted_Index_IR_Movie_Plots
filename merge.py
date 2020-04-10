import functools
import operator
import sys
from collections import defaultdict, Counter

import ndjson
import psutil

from constants import BLOCK_SIZE, MEMORY_LIMIT, THRESHOLD, FINAL_INDEX_PATH, BUILDED_INDEX_PATH
from helper_funcs import process


def multi_merge_sort(number: int) -> int:
    """

    :Multi merge algorithm
    """
    files = [
        open(
            BUILDED_INDEX_PATH + "index{}.json".format(num),
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
            files_to_read, final_index_dict, temp_dictionary, term_file = main_merge(
                final_index_dict,
                temp_dictionary,
                term_file)
    while temp_dictionary.keys() | final_index_dict.keys():  # for tail, if unprocessed items exist
        if temp_dictionary.keys():
            files_to_read, final_index_dict, temp_dictionary, term_file = main_merge(final_index_dict, temp_dictionary,
                                                                                     term_file)
        else:
            max_term = max(final_index_dict.keys())  # name file as max term for boolean search over files
            write_to_file(max_term, final_index_dict)
            final_index_dict = defaultdict(list)  # clean out orm


def main_merge(
        final_index_dict: defaultdict(list),
        temp_dictionary: defaultdict(list),
        term_file: defaultdict(list)
) -> [list, defaultdict(list), defaultdict(list), defaultdict(list)]:
    """

    Main part of merge multi sort algorithm.
    """
    min_term = min(temp_dictionary.keys())
    min_posting_list = temp_dictionary.pop(min_term)
    files_to_read = term_file.pop(min_term)  # read next line on files where min term was

    min_posting_list: dict = dict(
        functools.reduce(operator.add, map(Counter, min_posting_list))
    )
    line_to_write: dict = {min_term: min_posting_list}  # for easier reading

    if psutil.virtual_memory().available < BLOCK_SIZE or MEMORY_LIMIT <= (
            sys.getsizeof(final_index_dict) / 1024) or process.memory_info()[0] > THRESHOLD:
        write_to_file(min_term, final_index_dict)
        final_index_dict = defaultdict(list)  # clean out orm

    final_index_dict.update(line_to_write)
    return files_to_read, final_index_dict, temp_dictionary, term_file


def write_to_file(file_name: int, index: defaultdict(list)):
    """

    Write index in separated files after merge.
    """
    dict_for_index = sorted(
        index.items())  # sort posting lists
    with open(FINAL_INDEX_PATH + "{}.json".format(file_name), 'w') as file:
        ndjson.dump(dict_for_index, file)
    # with open("data/files_index/{}.txt".format(file_name), "wb") as file:
    #     pickle.dump(dict_for_index, file)
