""""""

import asyncio
import psycopg
import structlog

from structlog import BoundLogger
from psycopg import AsyncConnection
from psycopg import (
    InternalError,
    InterfaceError,
    OperationalError
)

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger: BoundLogger = structlog.get_logger()


async def create_connection(
    host: str,
    port: int,
    db: str,
    user: str,
    pass_: str
) -> AsyncConnection | None:
    """"""

    try:
        conn: AsyncConnection = await psycopg.AsyncConnection.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=pass_
        )

        await logger.ainfo("\nConnection to database has been created.")

        return conn
    except InternalError as intern_err:
        await logger.aerror(
            f"\nInternalError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except InterfaceError as inter_err:
        await logger.aerror(
            f"\nInterfaceError: {str(inter_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {create_connection.__name__}.\n"
        )
    except OperationalError as oper_err:
        await logger.aerror(
            f"\nOperationalError: {str(oper_err).capitalize()}.\n"
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
