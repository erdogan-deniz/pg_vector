""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    IdleSessionTimeoutError,
    FeatureNotSupportedError,
    InsufficientPrivilegeError,
    UnsupportedClientFeatureError,
    UnsupportedServerFeatureError,
    InvalidTransactionInitiationError,

    PostgresError,
    PostgresSystemError,
    UnknownPostgresError,
)

logger: BoundLogger = structlog.get_logger(__name__)


async def enable_extention(conn: Connection) -> None:
    """"""

    try:
        sql_query: str = "CREATE EXTENSION IF NOT EXISTS vector;"

        await conn.execute(sql_query)
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except AdminShutdownError as admin_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(admin_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except (
        InternalClientError,
        InternalServerError,
    ) as intern_err:
        await logger.aerror(
            f"\nInternalError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except (
        FeatureNotSupportedError,
        UnsupportedServerFeatureError,
        UnsupportedClientFeatureError,
    ) as feat_err:
        await logger.aerror(
            f"\nFeatureError: {str(feat_err).capitalize()}.\n"
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
    except InvalidTransactionInitiationError as inv_trans_err:
        await logger.aerror(
            "\nInvalidTransactionInitiationError: "
            f"{str(inv_trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {enable_extention.__name__}.\n"
        )
    else:
        await logger.ainfo(
            "An extension has been enabled "
            "for the database.\n"
        )
