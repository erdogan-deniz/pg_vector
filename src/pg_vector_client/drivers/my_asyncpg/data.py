""""""

import enum

from enum import Enum


@enum.unique
class VectorIndexType(Enum):
    """"""

    hnsw = "hnsw"
    ivfflat = "ivfflat"


@enum.unique
class VectorSimilarityOperators(Enum):
    """"""

    vector_ip_ops = "<#>"
    vector_l2_ops = "<->"
    vector_cosine_ops = "<=>"
