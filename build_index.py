import os
import sys
from collections import defaultdict
from typing import TextIO

import ndjson
import pandas as pd
import psutil
from nltk import word_tokenize, wordpunct_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import words

BLOCK_SIZE = 2 * 1024 * 1024  # * 1024
THRESHOLD = 16 * 1024 * 1024 * 1024
memorylimit = 1024 * 3
fileCounter = 0
filename = 'file' + str(fileCounter) + '.json'# change to json file to store the dictionary

process = psutil.Process(os.getpid())


def build_index(
        df: pd.DataFrame(columns=["Plot"])):
    """
    SPMI algorithm build index in blocks using defaultdict()
    """
    file_num = 0
    index = defaultdict(list)
    for docID, terms_list in df.iterrows():
        for term in terms_list.values[0]:
            index[term].append(docID)
        # if psutil.virtual_memory().available < BLOCK_SIZE
        # or process.memory_info()[0] > THRESHOLD:
        if (sys.getsizeof(index) / 1024) >= memorylimit:
            # print(index)
            #     with open('file.txt', 'w') as f:
            #         print(index, file=f)
            # writing
            file_num = write_block_to_file(index, file_num)
            index = defaultdict(list)


def write_block_to_file(
        index: defaultdict(list),
        file_num: int,
) -> int:
    # need to add sort here
    # index=sorted(index.values())
    dict_for_index = sorted(index.items())
    ndjson.dump(dict_for_index, open("index" + str(file_num) + ".json", 'w'))
    #     f.close()
    #     outfile.write('\n')
    file_num1 = file_num + 1
    return file_num1  # or make global


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
    # TODO: implement clean English language
    # df['Plot'] = [" ".join(w for w in wordpunct_tokenize(row)
    #                        if w.lower() in words or not w.isalpha())
    #               for row in df.Plot]

    # df['Plot'].apply(lambda row: [item for item in row if item.lower()
    # in words.words() or not item.isalpha()])
    df['Plot'] = df["Plot"].replace(
        r"(<[^>]*>|\W)", " ", regex=True, inplace=True)
    df['Plot'] = df["Plot"].apply(lambda row: word_tokenize(row))
    df['Plot'] = df["Plot"].apply(
        lambda row: [WordNetLemmatizer().lemmatize(sym)
                     for sym in row if sym != " "])
    return df


if __name__ == "__main__":
    create_directories()
    prep_file: TextIO
    with open('data/preprocessed/tokenized_dataset.csv', "w") as prep_file:
        for chunk in pd.read_csv("wiki_movie_plots_deduped.csv",
                                 usecols=["Plot"],  # add "Title"
                                 chunksize=10,
                                 ):
            chunk_processed: pd.DataFrame(columns=["Plot"]
                                          ) = process_data(chunk)
            build_index(chunk_processed)
            # chunk_processed.to_csv(prep_file, header=None, index=False)


