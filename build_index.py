import pandas as pd
from nltk import word_tokenize
import re
from collections import defaultdict
import os
import psutil
import ndjson
import sys

BLOCK_SIZE = 2 * 1024 * 1024  # * 1024
THRESHOLD = 16 * 1024 * 1024 * 1024
memorylimit = 1024 * 3
fileCounter = 0
filename = 'file' + str(fileCounter) + '.json'  # change to json file to store the dictionary


def punctuation(text):
    clean_data = []
    for x in (text[:][0]):
        new_text = re.sub('<.*?>', '', x)  # remove HTML tags
        new_text = re.sub(r'[^\w\s]', '', new_text)  # remove punc.
        new_text = re.sub(r'\d+', '', new_text)  # remove numbers
        new_text = new_text.lower()  # lower case
        if new_text != '':
            clean_data.append(new_text)
    return clean_data


def tokenizing_w(words):
    w_new = []
    for w in (words[:][0]):
        w_token = word_tokenize(w)
        if w_token != '':
            w_new.append(w_token)
    return w_new


process = psutil.Process(os.getpid())


def build_index(plot):
    file_num = 0
    index = defaultdict(list)
    for docID, terms_list in plot.iterrows():
        for term in terms_list.values[0]:
            index[term].append(docID)
        # print(index)
        # if psutil.virtual_memory().available < BLOCK_SIZE  or process.memory_info()[0] > THRESHOLD:
        if (sys.getsizeof(index) / 1024) >= memorylimit:
            # print(index)
            #     with open('file.txt', 'w') as f:
            #         print(index, file=f)
            # writing
            file_num = write_block_to_file(index, file_num)
            index = defaultdict(list)


def write_block_to_file(index, file_num):
    # need to add sort here
    # index=sorted(index.values())
    dict_for_index = sorted(index.items())
    ndjson.dump(dict_for_index, open("index" + str(file_num) + ".json", 'w'))
    #     f.close()
    #     outfile.write('\n')
    file_num = file_num + 1
    return file_num  # or make global


if __name__ == "__main__":
    data = pd.read_csv("wiki_movie_plots_deduped.csv")
    plot = data[['Plot']]
    # plot.loc[:, 'Plot'] = plot.apply(lambda row: word_tokenize(row['Plot']), axis=1)
    # plot.loc[:, 'Plot']=tokenizing_w(plot.Plot)
    plot.loc[:, 'Plot'] = plot.Plot.str.replace('[^\w\s]', '').str.split(' ')
    build_index(plot)
