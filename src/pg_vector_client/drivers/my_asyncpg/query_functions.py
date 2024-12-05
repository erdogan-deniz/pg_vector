""""""

import structlog

import numpy as np

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    DataError,
    PostgresSyntaxError,
    UndefinedTableError,
    UndefinedColumnError,
    UniqueViolationError,
    NotNullViolationError,
    ForeignKeyViolationError,
    InsufficientPrivilegeError,
    InsufficientResourcesError,
    InvalidSQLStatementNameError,

    DataError
    InternalError
    PostgresError
    IntegrityError
    ProgrammingError
    OperationalError
    CheckViolationError
    UniqueViolationError
    UniqueViolationError
    NotNullViolationError
    ForeignKeyViolationError
    ForeignKeyViolationError
    InvalidTextRepresentationError

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
    except PostgresSyntaxError as synt_err:
        await logger.aerror(
            f"\nPostgresSyntaxError: {str(synt_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except UndefinedTableError as undef_table_err:
        await logger.aerror(
            f"\nUndefinedTableError: {str(undef_table_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except UndefinedColumnError as undef_col_err:
        await logger.aerror(
            f"\nUndefinedColumnError: {str(undef_col_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except UniqueViolationError as uniq_violat_err:
        await logger.aerror(
            f"\nUniqueViolationError: {str(uniq_violat_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except NotNullViolationError as n_null_err:
        await logger.aerror(
            f"\nNotNullViolationError: {str(n_null_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except ForeignKeyViolationError as for_key_err:
        await logger.aerror(
            f"\nForeignKeyViolationError: {str(for_key_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(insuff_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InsufficientResourcesError as insuff_res_err:
        await logger.aerror(
            "\nInsufficientResourcesError: "
            f"{str(insuff_res_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InvalidSQLStatementNameError as inv_sql_err:
        await logger.aerror(
            "\nInvalidSQLStatementNameError: "
            f"{str(inv_sql_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
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


async def add_vector(
    conn: Connection,
    sql_query: str,
    embedding: np.ndarray
) -> None:
    """"""

    try:
        await conn.execute(
            sql_query,
            embedding
        )

        await logger.ainfo("\nThe vector has been added.")
    except UniqueViolationError
    except DataError
    except ForeignKeyViolationError
    except InvalidTextRepresentationError
    except PostgresError
    except UniqueViolationError
    except ForeignKeyViolationError
    except NotNullViolationError
    except CheckViolationError
    except DataError
    except IntegrityError
    except InternalError
    except ProgrammingError
    except OperationalError
    except PostgresError
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {add_vector.__name__}.\n"
        )
