import asyncio
import os
import sys

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from utils.rs_env import rs_settings

rs_settings.load_env_file()


async def check_services():
    db_url = URL.create(
        drivername="postgresql+asyncpg",
        username=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST", 'localhost'),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB_NAME")
    )
    engine = create_async_engine(db_url, connect_args={"timeout": 5})

    redis_client = Redis(
        host=os.getenv("REDIS_HOST", 'localhost'),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD"),
        socket_timeout=5
    )

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        await redis_client.ping()

        await engine.dispose()
        await redis_client.aclose()
        sys.exit(0)

    except Exception as e:
        print(f"Healthcheck failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(check_services())
