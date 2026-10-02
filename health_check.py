import asyncio
import os
import sys
import psycopg

from redis.asyncio import Redis

from utils.rs_env import rs_settings

rs_settings.load_env_file()


async def check_services():
    # 1. Настройка Redis
    redis_client = Redis(
        host=os.getenv("REDIS_HOST", 'localhost'),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        socket_timeout=5
    )

    pg_params = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", 5432)),
        "dbname": os.getenv("PG_DB_NAME"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "connect_timeout": 5
    }

    try:
        await redis_client.ping()
        await redis_client.aclose()

        async with await psycopg.AsyncConnection.connect(**pg_params) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")


        sys.exit(0)

    except Exception as e:
        # Пишем в stderr, чтобы Docker увидел текст ошибки в `docker inspect`
        print(f"Healthcheck db_agent failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Фикс для корректной работы psycopg3 при локальном запуске на Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(check_services())
    except KeyboardInterrupt:
        sys.exit(1)