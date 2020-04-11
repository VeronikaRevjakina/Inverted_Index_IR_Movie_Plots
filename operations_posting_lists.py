def union_posting_lists(left_post_list, right_post_list) -> dict:
    """

    Computes union between 2 non-empty sorted posting lists
    """
    iter_left: iter = iter(left_post_list.items())  # iter over doc_ids
    iter_right: iter = iter(right_post_list.items())
    doc_id1, tf1 = next(iter_left)
    doc_id2, tf2 = next(iter_right)

    result_dict: dict = dict()
    while doc_id1 is not None or doc_id2 is not None:
        if doc_id1 is None:
            result_dict[doc_id2] = tf2
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                doc_id2 = None
            continue
        elif doc_id2 is None:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                doc_id1 = None
            continue
        elif doc_id1[0] == doc_id2[0]:
            result_dict[doc_id1] = tf1 + tf2  # sum tf for doc_id
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                doc_id1 = None
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                doc_id2 = None
            continue
        elif doc_id1[0] < doc_id2[0]:
            result_dict[doc_id1] = tf1
            try:
                doc_id1, tf1 = next(iter_left)
            except StopIteration:
                doc_id1 = None
            continue
        else:
            result_dict[doc_id2] = tf2
            try:
                doc_id2, tf2 = next(iter_right)
            except StopIteration:
                doc_id2 = None
            continue

    return result_dict



