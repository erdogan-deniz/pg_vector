""""""

import asyncpg
import structlog

from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    InterfaceError,
    InvalidPasswordError,
    CannotConnectNowError,
    InvalidCatalogNameError,
    ConnectionDoesNotExistError,
    InvalidAuthorizationSpecificationError
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
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InvalidPasswordError as inv_pass_err:
        await logger.aerror(
            f"\nInvalidPasswordError: {str(inv_pass_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except CannotConnectNowError as cann_conn_err:
        await logger.aerror(
            f"\nCannotConnectNowError: {str(cann_conn_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InvalidCatalogNameError as inv_name_err:
        await logger.aerror(
            f"\nInvalidCatalogNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except ConnectionDoesNotExistError as conn_n_exist_err:
        await logger.aerror(
            "\nConnectionDoesNotExistError: "
            f"{str(conn_n_exist_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InvalidAuthorizationSpecificationError as inv_auth_err:
        await logger.aerror(
            "\nInvalidAuthorizationSpecificationError: "
            f"{str(inv_auth_err).capitalize()}\n."
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except Exception as unk_err:
        await logger.awarning(
            "\nException: "
            f"{str(unk_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
