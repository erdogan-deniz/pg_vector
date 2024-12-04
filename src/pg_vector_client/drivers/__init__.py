""""""

import asyncio

import psycopg3
import my_asyncpg


async def main():
    conn = await psycopg3.create_connection(
            host="localhost",
            port=5432,
            db="postgres",
            user="postgres",
            pass_="196910"
        )

asyncio.run(main())
