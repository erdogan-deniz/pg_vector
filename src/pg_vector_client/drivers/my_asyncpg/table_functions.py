""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    DataError,
    AdminShutdownError,
    SyntaxOrAccessError,
    DatabaseDroppedError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,

    PostgresError,
    UnknownPostgresError
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

        await logger.ainfo(f"\nThe {table_name} table has been deleted.")
    except DataError as data_err:
        await logger.aerror(
            f"\nDataError: {str(data_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except DatabaseDroppedError as db_drop_err:
        await logger.aerror(
            f"\nDatabaseDroppedError: {str(db_drop_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except ActiveSQLTransactionError as sql_trans_err:
        await logger.aerror(
            "\nActiveSQLTransactionError: "
            f"{str(sql_trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_priv_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_priv_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except SyntaxOrAccessError as synt_or_access_err:
        await logger.aerror(
            "\nSyntaxOrAccessError: "
            f"{str(synt_or_access_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except UnknownPostgresError as unkn_postgr_err:
        await logger.aerror(
            "\nUnknownPostgresError: "
            f"{str(unkn_postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {drop_table.__name__}.\n"
        )
