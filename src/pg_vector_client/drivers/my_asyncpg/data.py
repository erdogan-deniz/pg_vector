""""""

import enum

from enum import Enum


@enum.unique
class SimilarityOperators(Enum):
    """"""

    vector_ip_ops = "<#>"
    vector_l2_ops = "<->"
    vector_cosine_ops = "<=>"
