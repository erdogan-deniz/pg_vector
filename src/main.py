""""""

import asyncio
import structlog

import numpy as np

from time import sleep
from asyncpg import Connection
from structlog import BoundLogger
from pg_vector_client.drivers import my_asyncpg


logger: BoundLogger = structlog.get_logger(__name__)


async def main() -> None:
    """"""

    try:
        conn: Connection = await my_asyncpg.create_prepared_connection(
            host="localhost",
            port=5432,
            db="postgres",
            user="postgres",
            password="196910"
        )

        sleep(3)

        await my_asyncpg.enable_extention(conn)

        sleep(3)

        await my_asyncpg.drop_table(
            conn,
            "vectors"
        )

        sleep(3)

        await my_asyncpg.execute_query(
            conn,
            "CREATE TABLE vectors (id bigserial PRIMARY KEY, "
            "vectors vector(3))"
        )

        sleep(3)

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([1, 2, 3])
        )

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([3, 2, 1])
        )

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([1, 1, 1])
        )

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([2, 2, 2])
        )

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([3, 3, 3])
        )

        await my_asyncpg.add_vector(
            conn,
            "vectors",
            "vectors",
            np.array([2, 2, 3])
        )

        sleep(3)

        await my_asyncpg.create_vector_index(
            conn,
            "vectors"
        )

        sleep(3)

        res = await my_asyncpg.get_similar_vectors(
            conn,
            "vectors",
            np.array([1, 1, 1])
        )

        [print(i["vectors"]) for i in res]

        sleep(3)

        await my_asyncpg.delete_connection(conn)
    except Exception:
        await logger.ainfo("The program ends.\n")


if __name__ == "__main__":
    asyncio.run(main())
