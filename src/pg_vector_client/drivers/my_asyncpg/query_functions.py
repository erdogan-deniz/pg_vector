""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    DataError,
    DatabaseError,
    InternalError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
    DeadlockDetected,
    NotSupportedError,
    UndefinedTableError,
    UndefinedColumnError,
    UniqueViolationError,
    InsufficientPrivilegeError,
    InsufficientResourcesError,
    
    
    PostgresError
)

logger: BoundLogger = structlog.get_logger()


async def execute_query(
    conn: Connection,
    sql_query: str
) -> None:
    """"""

    try:
        await conn.execute(sql_query)

        await logger.ainfo(f"\nThe request '{sql_query}' was completed.")
    except DataError as data_err:
        await logger.aerror(
            f"\nDataError: {str(data_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except DatabaseError as db_err:
        await logger.aerror(
            f"\nDatabaseError: {str(db_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except DataError as data_err:
        await logger.aerror(
            f"\nDataError: {str(data_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except DataError as data_err:
        await logger.aerror(
            f"\nDataError: {str(data_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
