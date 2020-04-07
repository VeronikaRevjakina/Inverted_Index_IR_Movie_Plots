from collections import defaultdict
import ndjson
import sys

memorylimit = 1024 * 3


def multi_merge_sort():
    file_number = 3
    files = [
        open(
            "index{}.json".format(num),
            "r",
            # buffering=math.floor(const_size_in_bytes / (file_number - 1)),
        )
        for num in range(file_number)
    ]
    files_to_read = files # pointers to read lines
    temp_dictionary = defaultdict(list)  # lines from each file stored, posting lists already combined
    term_file = defaultdict(list)  # storage for files_ro_read assignment
    final_index_dict = defaultdict(list)
    while files:  # check eof
        for file in files_to_read:
            line = file.readline()
            if not line:
                files.remove(file)
                file.close()
            #                   if os.path.exists(file.name):
            #                         os.remove(file.name)  # drop tmp index file
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
            # if (sys.getsizeof(final_index_dict) / 1024) >= memorylimit:
            write_block_to_file(min_term, final_index_dict)
            final_index_dict = defaultdict(list)  # clean out storage


def write_block_to_file(name, index):
    # need to add sort here
    # index=sorted(index.values())
    dict_for_index = sorted(index.items())
    with open("data/index/{}.json".format(name), 'w') as file:
        ndjson.dump(dict_for_index, file)


#     f.close()
#     outfile.write('\n')

if __name__ == "__main__":
    multi_merge_sort()
