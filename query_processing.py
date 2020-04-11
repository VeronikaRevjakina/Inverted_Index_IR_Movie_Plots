import os

import ndjson

from constants import FINAL_INDEX_PATH

keywords: set = set(["AND", "OR"])
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
                        term, posting_list_r = ndjson.loads(line)[0] # TODO: make load chunk of data
                        if token == term:  # if found token in document
                            return posting_list_r
                        elif token < term:  # optimization, not go through all document, if not found token,
                            # but token already bigger value than term
                            print("term not found")
                            return posting_list


if __name__ == "__main__":
    query = input('Enter your query:')
    post_list = get_posting_list(query)
    print(post_list)
