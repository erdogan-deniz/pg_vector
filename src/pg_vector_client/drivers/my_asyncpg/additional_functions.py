""""""

import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from pgvector.asyncpg import register_vector
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    NoDataFoundError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    IdleSessionTimeoutError,
    FeatureNotSupportedError,
    InsufficientPrivilegeError,
    UnsupportedClientFeatureError,
    UnsupportedServerFeatureError,
    InvalidDatabaseDefinitionError,
    InvalidTransactionInitiationError,
    InvalidAuthorizationSpecificationError,

    PostgresError,
    PostgresSystemError,
    UnknownPostgresError,
    PostgresConnectionError
)

logger: BoundLogger = structlog.get_logger()


async def registr_vector_type(conn: Connection) -> None:
    """"""

    try:
        await register_vector(conn)
    except InterfaceError as interf_err:
        await logger.aerror(
            "\nInterfaceError: "
            f"{str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            "\nSQLRoutineError: "
            f"{str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except NoDataFoundError as data_found_err:
        await logger.aerror(
            "\nNoDataFoundError: "
            f"{str(data_found_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except AdminShutdownError as admin_shutd_err:
        await logger.aerror(
            "\nAdminShutdownError: "
            f"{str(admin_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except (
        InternalClientError,
        InternalServerError,
    ) as intern_err:
        await logger.aerror(
            "\nInternalError: "
            f"{str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_session_timeout_err:
        await logger.aerror(
            "\nIdleSessionTimeoutError: "
            f"{str(idle_session_timeout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_privil_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_privil_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except (
        FeatureNotSupportedError,
        UnsupportedClientFeatureError,
        UnsupportedServerFeatureError,
    )as unsupp_feat_err:
        await logger.aerror(
            "\nUnsupportedFeatureError: "
            f"{str(unsupp_feat_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except InvalidDatabaseDefinitionError as inv_db_def_err:
        await logger.aerror(
            "\nInvalidDatabaseDefinitionError: "
            f"{str(inv_db_def_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except InvalidTransactionInitiationError as inv_trans_init_err:
        await logger.aerror(
            "\nInvalidTransactionInitiationError: "
            f"{str(inv_trans_init_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except InvalidAuthorizationSpecificationError as inv_auth_spec_err:
        await logger.aerror(
            "\nInvalidAuthorizationSpecificationError: "
            f"{str(inv_auth_spec_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
        PostgresConnectionError,
    ) as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {registr_vector_type.__name__}.\n"
        )
    else:
        await logger.ainfo(
            "\nThe vector type has been "
            "registered for the connector."
        )
