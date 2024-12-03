
import asyncio
import asyncpg

async def connect_to_db():
    conn = await asyncpg.connect(
        user='postgres',        # Замените на ваше имя пользователя
        password='196910',    # Замените на ваш пароль
        database='postgres',    # Замените на имя вашей базы данных
        port='5432'                  # Порт PostgreSQL по умолчанию
    )
    print(conn)

asyncio.run(connect_to_db()))
