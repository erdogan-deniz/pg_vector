""""""

import structlog

import numpy as np

from typing import Any
from structlog import BoundLogger
from .data import SimilarityOperators
from asyncpg.connection import Connection
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    InvalidNameError,
    TooManyRowsError,
    CannotCoerceError,
    AdminShutdownError,
    InternalServerError,
    UndefinedColumnError,
    DatatypeMismatchError,
    CollationMismatchError,
    IdleSessionTimeoutError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,
    NumericValueOutOfRangeError,
    InvalidColumnReferenceError,
    InvalidTransactionInitiationError,

    PostgresError,
    PostgresSyntaxError,
    PostgresSystemError,
    UnknownPostgresError,
)

logger: BoundLogger = structlog.get_logger(__name__)


async def add_vector(
    conn: Connection,
    table_name: str,
    vec_field_name: str,
    embedding: np.ndarray
) -> None:
    """"""

    try:
        sql_query: str = f"""
                          INSERT INTO {table_name} ({vec_field_name})
                          VALUES ($1);
                          """

        await conn.execute(
            sql_query,
            embedding
        )
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except TooManyRowsError as many_rows_err:
        await logger.aerror(
            "\nTooManyRowsError: "
            f"{str(many_rows_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except CannotCoerceError as cann_coer_err:
        await logger.aerror(
            f"\nCannotCoerceError: {str(cann_coer_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except UndefinedColumnError as undef_col_err:
        await logger.aerror(
            "\nUndefinedColumnError: "
            f"{str(undef_col_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except DatatypeMismatchError as data_mism_err:
        await logger.aerror(
            f"\nDatatypeMismatchError: {str(data_mism_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except CollationMismatchError as coll_mism_err:
        await logger.aerror(
            f"\nCollationMismatchError: {str(coll_mism_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except NumericValueOutOfRangeError as num_val_rang_err:
        await logger.aerror(
            "\nNumericValueOutOfRangeError: "
            f"{str(num_val_rang_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSyntaxError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
    else:
        await logger.ainfo("The vector has been added.\n")


async def get_neighbor_vectors(
    conn: Connection,
    table_name: str,
    embedding: np.ndarray,
    vec_field_name: str = "embedding",
    neighb_count: int = 5,
    idx_type: SimilarityOperators = SimilarityOperators.vector_cosine_ops
) -> Any | None:
    """"""

    try:
        sql_query: str = f"""
                          SELECT *
                          FROM {table_name}
                          ORDER BY {vec_field_name} {idx_type.value} $1
                          LIMIT {neighb_count};
                          """

        res: Any = await conn.execute(
            sql_query,
            embedding
        )
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except TooManyRowsError as many_rows_err:
        await logger.aerror(
            f"\nTooManyRowsError: {str(many_rows_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except CollationMismatchError as coll_mism_err:
        await logger.aerror(
            f"\nCollationMismatchError: {str(coll_mism_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except ActiveSQLTransactionError as active_sql_trans_err:
        await logger.aerror(
            "\nActiveSQLTransactionError: "
            f"{str(active_sql_trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except InvalidColumnReferenceError as inv_col_ref_err:
        await logger.aerror(
            "\nInvalidColumnReferenceError: "
            f"{str(inv_col_ref_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSyntaxError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {get_neighbor_vectors.__name__}.\n"
        )
    else:
        return res
