""""""

import asyncio

import psycopg3
import my_asyncpg


async def main():
    conn = await my_asyncpg.create_connection(
            host="localhost",
            port=5432,
            db="postgres",
            user="postgres",
            pass_="196910"
        )

    await my_asyncpg.prepare_vector_connector(conn)
    await my_asyncpg.execute_query(
        conn,
        "CREATE TABLE items (id bigserial PRIMARY KEY, embedding vector(3))"
    )
    await my_asyncpg.remove_connection(conn)

asyncio.run(main())
