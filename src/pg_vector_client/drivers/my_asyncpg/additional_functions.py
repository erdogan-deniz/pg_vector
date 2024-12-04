""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from pgvector.asyncpg import register_vector
from asyncpg import (
    AdminShutdownError,
    DatabaseDroppedError,
    FeatureNotSupportedError,
    ActiveSQLTransactionError,
    InsufficientPrivilegeError,
    InvalidAuthorizationSpecificationError,

    UndefinedObjectError,
    UndefinedFunctionError,

    SyntaxOrAccessError,
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


async def registr_vector_type(conn: Connection) -> None:
    """"""

    try:
        await register_vector(conn)
    except UndefinedFunctionError as undef_func_err:
        await logger.aerror(
            "\nUndefinedFunctionError: "
            f"{str(undef_func_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except UndefinedObjectError as undef_obj_err:
        await logger.aerror(
            "\nUndefinedObjectError: "
            f"{str(undef_obj_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except UnknownPostgresError as unkn_postgr_err:
        await logger.aerror(
            "\nUnknownPostgresError: "
            f"{str(unkn_postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )


async def prepare_vector_connector(conn: Connection) -> None:
    """"""

    try:
        await enable_extention(conn)
        await registr_vector_type(conn)
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {prepare_vector_connector.__name__}.\n"
        )
