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


def intersect_posting_lists(left_post_list: dict, right_post_list: dict) -> dict:
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
