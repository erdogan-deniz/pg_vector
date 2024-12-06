""""""

import asyncpg
import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from .additional_functions import registr_vector_type
from asyncpg import (
    InterfaceError,
    AdminShutdownError,
    InternalClientError,
    InternalServerError,
    CannotConnectNowError,
    ConnectionFailureError,
    TooManyConnectionsError,
    UndefinedParameterError,
    ClientCannotConnectError,
    ClientConfigurationError,
    ConnectionRejectionError,
    ProgramLimitExceededError,
    InsufficientPrivilegeError,
    ConfigurationLimitExceededError,
    InvalidAuthorizationSpecificationError,

    PostgresError,
    PostgresSystemError,
    UnknownPostgresError,
    PostgresConnectionError
)

logger: BoundLogger = structlog.get_logger()


async def create_connection(
    host: str,
    port: int,
    db: str,
    user: str,
    pass_: str
) -> Connection | None:
    """"""

    try:
        conn: Connection = await asyncpg.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=pass_
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
        await logger.ainfo("\nConnection to database has been created.")

        return conn


async def create_worked_connection(
    host: str,
    port: int,
    db: str,
    user: str,
    pass_: str
) -> Connection | None:
    """"""

    try:
        conn: Connection | None = await create_connection(
            host=host,
            port=port,
            database=db,
            user=user,
            password=pass_
        )

        await registr_vector_type(conn)
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_worked_connection.__name__}.\n"
        )
    else:
        await logger.ainfo("\nA prepared connection has been created.")

        return conn


async def remove_connection(conn: Connection) -> None:
    """"""

    try:
        await conn.close()
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            "\nAdminShutdownError: "
            f"{str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except InternalClientError as intern_err:
        await logger.aerror(
            "\nInternalClientError: "
            f"{str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except UndefinedParameterError as undef_par_err:
        await logger.aerror(
            "\nUndefinedParameterError: "
            f"{str(undef_par_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
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
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    else:
        await logger.ainfo("\nConnection to database has been closed.")
