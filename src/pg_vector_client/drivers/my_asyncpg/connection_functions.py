""""""

import asyncpg
import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    InvalidPasswordError,
    CannotConnectNowError,
    ConnectionFailureError,
    PostgresConnectionError,
    TooManyConnectionsError,
    ConnectionRejectionError,
    ClientCannotConnectError,
    ClientConfigurationError,
    InsufficientPrivilegeError,
    InvalidAuthorizationSpecificationError,

    InterfaceError,
    DataCorruptedError,
    CrashShutdownError,
    InternalClientError,
    ConnectionDoesNotExistError,

    PostgresError,
    UnknownPostgresError
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

        await logger.ainfo("\nConnection to database has been created.")

        return conn
    except InvalidPasswordError as inv_pass_err:
        await logger.aerror(
            f"\nInvalidPasswordError: {str(inv_pass_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except CannotConnectNowError as conn_now_err:
        await logger.aerror(
            f"\nCannotConnectNowError: {str(conn_now_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ConnectionFailureError as conn_fail_err:
        await logger.aerror(
            f"\nConnectionFailureError: {str(conn_fail_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except TooManyConnectionsError as many_conn_err:
        await logger.aerror(
            f"\nTooManyConnectionsError: {str(many_conn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ConnectionRejectionError as conn_rej_err:
        await logger.aerror(
            f"\nConnectionRejectionError: {str(conn_rej_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ClientCannotConnectError as client_conn_err:
        await logger.aerror(
            "\nClientCannotConnectError: "
            f"{str(client_conn_err).capitalize()}.\n"
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
    except InsufficientPrivilegeError as insuff_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_err).capitalize()}.\n"
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
    except PostgresConnectionError as postgr_conn_err:
        await logger.aerror(
            "\nPostgresConnectionError: "
            f"{str(postgr_conn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except UnknownPostgresError as unkn_postgr_err:
        await logger.aerror(
            "\nUnknownPostgresError: "
            f"{str(unkn_postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )


async def remove_connection(conn: Connection) -> None:
    """"""

    try:
        await conn.close()

        await logger.ainfo("\nConnection to database has been closed.")
    except InterfaceError as interf_err:
        await logger.aerror(
            "\nInterfaceError: "
            f"{str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except DataCorruptedError as data_corr_err:
        await logger.aerror(
            "\nDataCorruptedError: "
            f"{str(data_corr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except CrashShutdownError as crash_err:
        await logger.aerror(
            "\nCrashShutdownError: "
            f"{str(crash_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except InternalClientError as intern_client_err:
        await logger.aerror(
            "\nInternalClientError: "
            f"{str(intern_client_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except ConnectionFailureError as conn_fail_err:
        await logger.aerror(
            "\nConnectionFailureError: "
            f"{str(conn_fail_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except InsufficientPrivilegeError as insuff_priv_err:
        await logger.aerror(
            "\nInsufficientPrivilegeError: "
            f"{str(insuff_priv_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except ConnectionDoesNotExistError as conn_d_exist_err:
        await logger.aerror(
            "\nConnectionDoesNotExistError: "
            f"{str(conn_d_exist_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except PostgresError as postgr_err:
        await logger.aerror(
            "\nPostgresError: "
            f"{str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except UnknownPostgresError as unkn_postgr_err:
        await logger.aerror(
            "\nUnknownPostgresError: "
            f"{str(unkn_postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )
    except Exception as unkn_err:
        await logger.awarning(
            "\nException: "
            f"{str(unkn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {remove_connection.__name__}.\n"
        )


async def create_worked conn