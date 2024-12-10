""""""

import asyncpg
import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from pgvector.asyncpg import register_vector
from asyncpg import (
    InterfaceError,
    SQLRoutineError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    CannotConnectNowError,
    ConnectionFailureError,
    TooManyConnectionsError,
    IdleSessionTimeoutError,
    ClientCannotConnectError,
    ClientConfigurationError,
    ConnectionRejectionError,
    FeatureNotSupportedError,
    ProgramLimitExceededError,
    InsufficientPrivilegeError,
    UnsupportedClientFeatureError,
    UnsupportedServerFeatureError,
    ConfigurationLimitExceededError,
    InvalidTransactionInitiationError,
    InvalidAuthorizationSpecificationError,

    PostgresError,
    PostgresSystemError,
    UnknownPostgresError,
    PostgresConnectionError,
)

logger: BoundLogger = structlog.get_logger(__name__)


async def create_connection(
    host: str,
    port: int,
    db: str,
    user: str,
    password: str
) -> Connection | None:
    """"""

    try:
        conn: Connection = await asyncpg.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=password
        )
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except (
        InternalClientError,
        InternalServerError,
    ) as intern_err:
        await logger.aerror(
            f"\nInternalError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except (
        CannotConnectNowError,
        ConnectionFailureError,
        ConnectionRejectionError,
        ClientCannotConnectError,
    ) as conn_err:
        await logger.aerror(
            f"\nConnectionError: {str(conn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except TooManyConnectionsError as many_conn_err:
        await logger.aerror(
            f"\nTooManyConnectionsError: {str(many_conn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ClientConfigurationError as client_conf_err:
        await logger.aerror(
            "\nClientConfigurationError: "
            f"{str(client_conf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ProgramLimitExceededError as prog_limit_err:
        await logger.aerror(
            "\nProgramLimitExceededError: "
            f"{str(prog_limit_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_privil_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_privil_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ConfigurationLimitExceededError as conf_limit_err:
        await logger.aerror(
            "\nConfigurationLimitExceededError: "
            f"{str(conf_limit_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InvalidAuthorizationSpecificationError as inv_auth_err:
        await logger.aerror(
            "\nInvalidAuthorizationSpecificationError: "
            f"{str(inv_auth_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
        PostgresConnectionError,
    ) as postrgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postrgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    else:
        await logger.ainfo("Connection to database has been created.\n")

        return conn


async def register_vector_type(conn: Connection) -> None:
    """"""

    try:
        await register_vector(conn)
    except InterfaceError as interf_err:
        await logger.aerror(
            "\nInterfaceError: "
            f"{str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            "\nSQLRoutineError: "
            f"{str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except AdminShutdownError as admin_shutd_err:
        await logger.aerror(
            "\nAdminShutdownError: "
            f"{str(admin_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except (
        InternalClientError,
        InternalServerError,
    ) as intern_err:
        await logger.aerror(
            "\nInternalError: "
            f"{str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_session_timeout_err:
        await logger.aerror(
            "\nIdleSessionTimeoutError: "
            f"{str(idle_session_timeout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_privil_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_privil_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
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
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except InvalidTransactionInitiationError as inv_trans_init_err:
        await logger.aerror(
            "\nInvalidTransactionInitiationError: "
            f"{str(inv_trans_init_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {register_vector_type.__name__}.\n"
        )
    else:
        await logger.ainfo(
            "The vector type has been "
            "registered for the connection.\n"
        )


async def create_prepared_connection(
    host: str,
    port: int,
    db: str,
    user: str,
    password: str
) -> Connection | None:
    """"""

    try:
        conn: Connection | None = await create_connection(
            host=host,
            port=port,
            db=db,
            user=user,
            password=password
        )

        await register_vector_type(conn)
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_prepared_connection.__name__}.\n"
        )
    else:
        return conn


async def delete_connection(conn: Connection) -> None:
    """"""

    try:
        await conn.close()
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            "\nAdminShutdownError: "
            f"{str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {delete_connection.__name__}.\n"
        )
    except InternalClientError as intern_err:
        await logger.aerror(
            "\nInternalClientError: "
            f"{str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {delete_connection.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSystemError,
        UnknownPostgresError,
        PostgresConnectionError,
    ) as postgres_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgres_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {delete_connection.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {delete_connection.__name__}.\n"
        )
    else:
        await logger.ainfo("Connection to database has been deleted.\n")
