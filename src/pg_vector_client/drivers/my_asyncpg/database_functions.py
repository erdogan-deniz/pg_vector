""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    AdminShutdownError,
    SyntaxOrAccessError,
    DatabaseDroppedError,
    FeatureNotSupportedError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,
    InvalidAuthorizationSpecificationError,

    PostgresError,
    UnknownPostgresError
)

logger: BoundLogger = structlog.get_logger()


async def enable_extention(conn: Connection) -> None:
    """"""

    try:
        sql_query: str = "CREATE EXTENSION IF NOT EXISTS vector;"

        await conn.execute(sql_query)

        await logger.ainfo("\nPG_Vector extension has been enabled.")
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except DatabaseDroppedError as db_drop_err:
        await logger.aerror(
            f"\nDatabaseDroppedError: {str(db_drop_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except FeatureNotSupportedError as feat_n_supp_err:
        await logger.aerror(
            "\nFeatureNotSupportedError: "
            f"{str(feat_n_supp_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except ActiveSQLTransactionError as sql_trans_err:
        await logger.aerror(
            "\nActiveSQLTransactionError: "
            f"{str(sql_trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_priv_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_priv_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except InvalidAuthorizationSpecificationError as inv_auth_err:
        await logger.aerror(
            "\nInvalidAuthorizationSpecificationError: "
            f"{str(inv_auth_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except SyntaxOrAccessError as synt_or_access_err:
        await logger.aerror(
            "\nSyntaxOrAccessError: "
            f"{str(synt_or_access_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except UnknownPostgresError as unkn_postgr_err:
        await logger.aerror(
            "\nUnknownPostgresError: "
            f"{str(unkn_postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
