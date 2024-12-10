""""""

import structlog

import psycopg

from structlog import BoundLogger
from asyncpg.connection import Connection
from .data import (
    VectorIndexType,
    VectorSimilarityOperators,
)
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    InvalidNameError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    UndefinedTableError,
    UndefinedColumnError,
    DuplicateObjectError,
    DatatypeMismatchError,
    IdleSessionTimeoutError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,
    InvalidColumnReferenceError,
    DependentObjectsStillExistError,
    InvalidTransactionInitiationError,

    PostgresError,
    PostgresSyntaxError,
    PostgresSystemError,
    UnknownPostgresError,
)

logger: BoundLogger = structlog.get_logger(__name__)


async def drop_table(
    conn: Connection,
    table_name: str
) -> None:
    """"""

    try:
        sql_query: str = f"DROP TABLE IF EXISTS {table_name};"

        await conn.execute(sql_query)
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedTableError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nTabledNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except (
        InternalClientError,
        InternalServerError,
    ) as intern_err:
        await logger.aerror(
            f"\nInternalError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(insuff_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except DependentObjectsStillExistError as dep_oojs_err:
        await logger.aerror(
            "\nDependentObjectsStillExistError: "
            f"{str(dep_oojs_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    else:
        await logger.ainfo(f"The {table_name} table has been dropped.\n")


async def create_vector_index(
    conn: Connection,
    table_name: str,
    vec_col_name: str = "vectors",
    vec_idx_type: VectorIndexType = VectorIndexType.hnsw,
    vec_sim_oper_type: VectorSimilarityOperators =
    VectorSimilarityOperators.vector_cosine_ops
) -> None:
    """"""

    try:
        sql_query: str = f"""
                          CREATE INDEX ON {table_name}
                          USING {vec_idx_type.value} ({vec_col_name}
                           {vec_sim_oper_type.name});
                          """

        await conn.execute(sql_query)
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except DuplicateObjectError as dublic_err:
        await logger.aerror(
            f"\nDuplicateObjectError: {str(dublic_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except DatatypeMismatchError as data_mismatch_err:
        await logger.aerror(
            "\nDatatypeMismatchError: "
            f"{str(data_mismatch_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except InvalidColumnReferenceError as inv_col_ref_err:
        await logger.aerror(
            "\nInvalidColumnReferenceError: "
            f"{str(inv_col_ref_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
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
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_vector_index.__name__}.\n"
        )
    else:
        await logger.ainfo(f"The {vec_idx_type.value} index has been created.\n")
