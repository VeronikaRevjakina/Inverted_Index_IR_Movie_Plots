import numpy as np

from tmp_store import max_doc_id


def union_posting_lists(left_post_list: dict, right_post_list: dict) -> dict:
    """

    Computes union between 2 non-empty sorted posting lists as dicts
    """
    iter_left: iter = iter(left_post_list.items())  # iter over doc_ids
    iter_right: iter = iter(right_post_list.items())
    doc_id1, tf1 = next(iter_left)
    doc_id2, tf2 = next(iter_right)

    flag_non_empty1: bool = True
    flag_non_empty2: bool = True

    result_dict: dict = dict()
    while flag_non_empty1 or flag_non_empty2:
        if not flag_non_empty1:
            result_dict[doc_id2] = tf2
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                flag_non_empty2 = False
        elif not flag_non_empty2:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                flag_non_empty1 = False
        elif doc_id1[0] == doc_id2[0]:
            result_dict[doc_id1] = tf1 + tf2  # sum tf for doc_id
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                flag_non_empty1 = False
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                flag_non_empty2 = False
        elif doc_id1[0] < doc_id2[0]:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                flag_non_empty1 = False
        else:
            result_dict[doc_id2] = tf2
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                flag_non_empty2 = False

    return result_dict


def intersect_many_posting_lists(list_of_posting_dicts: list) -> dict:
    """

    Computes intersection between many posting lists
    """
    list_of_posting_dicts.sort(key=len)
    result_dict: dict = list_of_posting_dicts.pop()  # get shortest
    while list_of_posting_dicts and result_dict:
        next_shortest_dict: dict = list_of_posting_dicts.pop()
        result_dict = intersect_two_posting_lists(result_dict, next_shortest_dict)  # shortest with next by len
    return result_dict


def subtract_from_left_right_posting_lists(left_post_list: dict, right_post_list: dict) -> dict:
    """

    Computes subtraction from left posting list right posting list non-empty
    """
    iter_left: iter = iter(left_post_list.items())  # iter over doc_ids
    iter_right: iter = iter(right_post_list.items())
    doc_id1, tf1 = next(iter_left)
    doc_id2, tf2 = next(iter_right)
    flag_tail: bool = False
    result_dict: dict = dict()
    while True:
        if doc_id1 == doc_id2:
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break  # reached end of first list, subtract ends
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                flag_tail = True  # end, but need to add left tail
                break
        elif doc_id1 < doc_id2:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break  # end of lest
        else:
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                flag_tail = True  # end of right, need to add tail
                break
    tail: dict = dict(iter_left)  # automatically gets left over values
    if flag_tail and (doc_id1 not in result_dict):
        result_dict[doc_id1] = tf1  # for one current doc_id1, doc_id2 interrupted
    if tail:
        result_dict.update(tail)

    return result_dict


def intersect_two_posting_lists(left_post_list: dict, right_post_list: dict) -> dict:
    """

    Computes intersection between 2 non-empty sorted posting lists as dicts
    """
    iter_left: iter = iter(left_post_list.items())  # iter over doc_ids
    iter_right: iter = iter(right_post_list.items())
    doc_id1, tf1 = next(iter_left)
    doc_id2, tf2 = next(iter_right)

    result_dict: dict = dict()
    while True:
        if doc_id1 == doc_id2:
            result_dict[doc_id1] = tf1 + tf2  # sum tf for doc_id
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break  # reached end of list, intersection ends
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                break
        elif doc_id1 < doc_id2:
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break
        else:
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                break
    return result_dict


def not_postings_list(left_posting_list: dict) -> dict:
    """
    Computes not operation for sorted posting list
    """
    result_dict: dict = dict()
    prev_key: int = -1
    sorted_keys = np.array(list(left_posting_list.keys()), dtype=int)
    # sorted_keys.sort()
    for current_key in sorted_keys:
        if current_key > prev_key + 1:  # add chunk from prev_key to current_key
            keys_list: np.array(dtype=str) = np.array(  # update every time
                range(prev_key + 1, int(current_key)), dtype=str
            )
            if len(keys_list) > 1:
                result_dict.update(dict.fromkeys(keys_list, 0))  # make tf zero
            else:
                result_dict[keys_list[0]] = 0  # in case no diff between prev_key and current_key
        prev_key = current_key

    if max_doc_id > prev_key + 1:  # left after last key to max
        result_dict.update(dict.fromkeys(
            np.array(range(prev_key + 1, max_doc_id + 1), dtype=str), 0))
    elif max_doc_id == prev_key + 1:  # eof
        result_dict[str(max_doc_id)] = 0
    return dict(sorted(result_dict.items()))
