""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    InterfaceError,
    InvalidNameError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    UndefinedTableError,
    UndefinedObjectError,
    IdleSessionTimeoutError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,
    DependentObjectsStillExistError,
    InvalidTransactionInitiationError,

    PostgresError,
    PostgresSystemError,
    UnknownPostgresError,
)

logger: BoundLogger = structlog.get_logger()


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
        await logger.ainfo(f"\nThe {table_name} table has been deleted.")
