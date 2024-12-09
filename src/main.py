""""""

import asyncio
import structlog

import numpy as np

from time import sleep
from asyncpg import Connection
from structlog import BoundLogger
from pg_vector_client import drivers

logger: BoundLogger = structlog.get_logger(__name__)

async def main() -> None:
    """"""

    try:
        conn: Connection = await my_asyncpg.create_worked_connection(
            host="localhost",
            port=5432,
            db="postgres",
            user="postgres",
            pass_="196910"
        )
        
        sleep(2)

        await my_asyncpg.drop_table(
            conn,
            "items"
        )
        
        sleep(2)
        
        await my_asyncpg.execute_query(
            conn,
            "CREATE TABLE items (id bigserial PRIMARY KEY, embedding vector(3))"
        )
        
        sleep(2)
        
        await my_asyncpg.add_vector(
            conn,
            "items",
            "embedding",
            np.array([1, 2, 3])
        )
        
        await my_asyncpg.add_vector(
            conn,
            "items",
            "embedding",
            np.array([2, 2, 3])
        )
        
        await my_asyncpg.add_vector(
            conn,
            "items",
            "embedding",
            np.array([1, 5, 3])
        )
        
        await my_asyncpg.add_vector(
            conn,
            "items",
            "embedding",
            np.array([4, 2, 3])
        )
        
        await my_asyncpg.create_hnsw_index(
            conn,
            "items",
        )
        
        await my_asyncpg.get_neighbor_vectors(
            conn,
            "items",
            np.array([1, 1, 1]),
        )

        await my_asyncpg.remove_connection(conn)
    except Exception:
        await logger.ainfo("The program ends.\n")


if __name__ == "__main__":
    asyncio.run(main())
