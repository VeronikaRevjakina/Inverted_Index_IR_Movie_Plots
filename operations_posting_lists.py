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
        if doc_id1[0] == doc_id2[0]:
            result_dict[doc_id1] = tf1 + tf2  # sum tf for doc_id
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break  # reached end of list, intersection ends
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                break
        elif doc_id1[0] < doc_id2[0]:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                break
        else:
            result_dict[doc_id2] = tf2
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                break
    return result_dict
