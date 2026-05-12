"""
RPUSH task_queue '{"A":"A", "id": "f286645d-21ab-4a73-8b42-6cebbf866658", "payload": {"a": 1}}'
"""

import asyncio
import datetime
import json
import os
import random
from typing import Any

import aioboto3
import redis.asyncio as redis
from redis.exceptions import ConnectionError, TimeoutError, RedisError
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from utils.rs_env import rs_settings
from utils.rs_i18n import _
from utils.rs_logger import get_logger

rs_settings.load_env_file()

logger = get_logger("RSAgentDB")

REDIS_URL: str | None = os.getenv("REDIS_URL")
QUEUE_NAME: str | None = os.getenv("QUEUE_NAME")
MAX_TASKS: int = int(os.getenv("MAX_CONCURRENT_TASKS", "10"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
RECONNECT_DELAY: int = int(os.getenv("RECONNECT_DELAY", "5"))
REDIS_SUBSCRIBE_BPROP_TIMEOUT: int = int(os.getenv("REDIS_SUBSCRIBE_BPROP_TIMEOUT", 2))
RETRY_COUNT_DB_ID: str = "retry_count_db_id"
RETRY_SLEEP: int = int(os.getenv("RETRY_SLEEP", 3))

async_semaphore = asyncio.Semaphore(MAX_TASKS)

s3_session = aioboto3.Session()

db_url = URL.create(
    drivername="postgresql+asyncpg",
    username=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST", 'localhost'),
    port=int(os.getenv("PG_PORT", 5432)),
    database=os.getenv("PG_DB_NAME")
)

db_engine = create_async_engine(db_url,
                                pool_size=int(os.getenv("PG_POOL_SIZE", 20)),
                                pool_pre_ping=True,
                                pool_recycle=3600,
                                max_overflow=int(os.getenv("PG_POOL_MAX_OVERFLOW", 20)),
                                pool_timeout=int(os.getenv("PG_POOL_TIMEOUT", 30)),
                                connect_args={"server_settings": {"search_path": os.getenv("PG_SEARCH_PATH", 'public')}})

redis_engine_client = redis.Redis(
    host=os.getenv("REDIS_HOST", 'localhost'),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=int(os.getenv("REDIS_DB_PUSH", 2)),
    max_connections=int(os.getenv("REDIS_MAX_CONNECTION", 20)),
    socket_timeout=None,
    socket_connect_timeout=5.0,
    socket_keepalive=True,
    health_check_interval=30,
    retry_on_timeout=True,
    decode_responses=True
)


async def save_task_log(task_id: str, message: str):
    """Отдельная функция для записи логов в БД"""
    try:
        async with db_engine.connect() as conn:
            await conn.execute(
                text("INSERT INTO rs.task_logs (task_id, error_message) VALUES (:tid, :err)"),
                {"tid": task_id, "err": message[:1000]}
            )
            await conn.commit()
    except Exception as log_e:
        logger.error(f"Ошибка записи лога в БД: {log_e}")


async def db_process_task(task: dict[str, Any]):
    task_id = task.get('id')
    if not task_id:
        logger.error(f"Получена задача без ID [{str(task)}]")
        return

    async with async_semaphore:
        r = redis_engine_client
        retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
        attempt = int(await r.get(f"{RETRY_COUNT_DB_ID}:{task_id}") or 1)

        try:
            async with db_engine.connect() as conn:
                result = await conn.execute(
                    text("select rs.f_get_test(:json_in)"),
                    {
                        "json_in": json.dumps(task.get('payload', {}))
                    }
                )
                summary = result.scalar()
                await conn.commit()

                print(summary)

            await r.set(f"status:{task_id}", "completed", ex=3600)
            await r.delete(retry_key)

            logger.info(f"{logger.name} {task_id} done")
            await save_task_log(task_id, f"{logger.name} {task_id} done")

        except (RedisError, SQLAlchemyError, Exception) as e:
            if attempt < MAX_RETRIES:
                # 1. Считаем базовую экспоненту: 2, 4, 8, 16...
                base_backoff = RETRY_SLEEP * (2 ** (attempt - 1))

                # 2. Ограничиваем сверху (Cap), чтобы не ждать вечность
                capped_backoff = min(base_backoff, MAX_RETRIES)

                # 3. Добавляем Jitter (от 0% до 30% от текущей задержки)
                jitter = capped_backoff * 0.3 * random.random()

                final_delay = capped_backoff + jitter

                logger.warning(
                    f"Task {task_id} failed (attempt {attempt}). "
                    f"Retrying in {final_delay:.2f}s (backoff: {base_backoff:.2f} + jitter: {jitter:.2f}). "
                    f"Error: {e}"
                )

                await r.set(f"{RETRY_COUNT_DB_ID}:{task_id}", attempt + 1, ex=3600)
                await asyncio.sleep(base_backoff)
                await r.lpush(QUEUE_NAME, json.dumps(task))
            else:
                await r.set(f"status:{task_id}", "failed", ex=3600)
                if not isinstance(e, SQLAlchemyError):
                    await save_task_log(task_id, e)
        finally:

            if os.path.exists('local_filename'):
                os.remove('local_filename')


async def main():
    logger.info(f"{_("The agent is running in", lang="ru")} [{datetime.datetime.now()}]...")
    r = redis_engine_client
    try:
        while True:
            try:
                task_data = await r.brpop(QUEUE_NAME, timeout=REDIS_SUBSCRIBE_BPROP_TIMEOUT)
                if task_data:
                    i, message_json = task_data
                    task = json.loads(message_json)

                    action = task.get('type')

                    if action == 'send':
                        asyncio.create_task(db_process_task(task))



            except (ConnectionError, TimeoutError, RedisError) as e:
                logger.error(f"Ошибка связи с Redis: {e}. Спим {RECONNECT_DELAY}с...")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception as e:
                logger.critical(f"Критическая ошибка цикла: {e}")
                await asyncio.sleep(RECONNECT_DELAY)
    except asyncio.CancelledError:
        logger.info("Завершаем работу...")
    finally:
        print(f"{logger.name} {_("connections are closing")} [db_engine, redis_client]...")
        await db_engine.dispose()
        await r.aclose()
        print(f"{logger.name} {_("all connections are closed")}...")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Завершаем работу...")
