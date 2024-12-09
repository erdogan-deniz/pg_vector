""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    InvalidNameError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    UndefinedTableError,
    UndefinedObjectError,
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
        UndefinedObjectError,
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
        await logger.ainfo(f"The {table_name} table has been deleted.\n")


async def create_hnsw_index(
    conn: Connection,
    table_name: str,
    col_name: str
) -> None:
    """"""

    try:
        sql_query: str = f"""
                          CREATE INDEX ON {table_name}
                          USING hnsw ({col_name} vector_l2_ops);
                          """

        await conn.execute(sql_query)
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except DuplicateObjectError as dublic_err:
        await logger.aerror(
            f"\nDuplicateObjectError: {str(dublic_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except DatatypeMismatchError as data_mismatch_err:
        await logger.aerror(
            "\nDatatypeMismatchError: "
            f"{str(data_mismatch_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except InvalidColumnReferenceError as inv_col_ref_err:
        await logger.aerror(
            "\nInvalidColumnReferenceError: "
            f"{str(inv_col_ref_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
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
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_hnsw_index.__name__}.\n"
        )
    else:
        await logger.ainfo("The hnsw index has been created.\n")


async def create_ivfflat_index(
    conn: Connection,
    table_name: str,
    col_name: str,
    clust_count: int
) -> None:
    """"""

    try:
        sql_query: str = f"""
                          CREATE INDEX ON {table_name}
                          USING ivfflat ({col_name} vector_l2_ops)
                          WITH (lists = {clust_count});
                          """

        await conn.execute(sql_query)
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except DuplicateObjectError as dublic_err:
        await logger.aerror(
            f"\nDuplicateObjectError: {str(dublic_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except DatatypeMismatchError as data_mismatch_err:
        await logger.aerror(
            "\nDatatypeMismatchError: "
            f"{str(data_mismatch_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except InvalidColumnReferenceError as inv_col_ref_err:
        await logger.aerror(
            "\nInvalidColumnReferenceError: "
            f"{str(inv_col_ref_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
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
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_ivfflat_index.__name__}.\n"
        )
    else:
        await logger.ainfo("The ivfflat index has been created.\n")
