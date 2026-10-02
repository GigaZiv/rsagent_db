"""
RPUSH task_queue '{"A":"A", "id": "f286645d-21ab-4a73-8b42-6cebbf866658", "payload": {"a": 1}}'
"""

import asyncio
import datetime
import json
import os
import random
import signal
import sys
import xml.etree.ElementTree as ET
from typing import Any

import redis.asyncio as redis
from psycopg import DatabaseError
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.exceptions import ConnectionError, TimeoutError, RedisError

from convertors.rs_cnv_format import parse_rosreestr_xml
from utils.rs_env import rs_settings
from utils.rs_i18n import _
from utils.rs_logger import get_logger

rs_settings.load_env_file()

logger = get_logger("RSAgentDB")

stop_event = asyncio.Event()

QUEUE_NAME: str = os.getenv("QUEUE_NAME") or ''
MAX_TASKS: int = int(os.getenv("MAX_CONCURRENT_TASKS", "10"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
RECONNECT_DELAY: int = int(os.getenv("RECONNECT_DELAY", "5"))

REDIS_SUBSCRIBE_BPROP_TIMEOUT: int = int(os.getenv("REDIS_SUBSCRIBE_BPROP_TIMEOUT", 2))
RETRY_COUNT_DB_ID: str = "rs:agent:taskdb:retry"
RETRY_SLEEP: int = int(os.getenv("RETRY_SLEEP", 3))

async_semaphore = asyncio.Semaphore(MAX_TASKS)

db_pool = AsyncConnectionPool(
    "",
    min_size=1,
    max_size=int(os.getenv("PG_POOL_SIZE", 20)),
    timeout=int(os.getenv("PG_POOL_TIMEOUT", 30)),
    open=False,
    kwargs={
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", 5432)),
        "dbname": os.getenv("PG_DB_NAME"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "options": f"-c search_path={os.getenv('PG_SEARCH_PATH', 'public')}",
        "row_factory": dict_row
    }
)

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


async def save_task_log(payload):
    """
    Отдельная функция для записи логов в БД
    result_payload = {
        k: v for k, v in {
            "id": payload.get('id'),
            "link_user_id": payload.get('link_user_id'),
            "task_status": payload.get('task_status'),
            "task_progress": payload.get('task_progress'),
            "task_message": payload.get('task_message'),
            "task_error_message": payload.get('task_error_message')
        }.items() if v is not None
    }
    """
    task_status = payload.get('task_status')
    task_progress = payload.get('task_progress')
    task_message = payload.get('task_message')
    task_error_message = payload.get('task_error_message')

    await executeDbFunction('rs.set_taskdb_log', {
        "payload": {
            "id": payload.get('id'),
            "task_user_id": payload.get('link_user_id'),
            **({"task_status": task_status} if task_status is not None else {}),
            **({"task_progress": task_progress} if task_progress is not None else {}),
            **({"task_message": task_message} if task_message is not None else {}),
            **({"task_error_message": task_error_message} if task_error_message is not None else {})
        }
    })


def get_root_tag(task_file):
    """Быстро достает имя корневого тега без парсинга всего файла"""
    try:
        context = ET.iterparse(task_file, events=('start',))
        i, elem = next(context)
        return elem.tag.split('}')[-1]
    except Exception as e:
        logger.error(f"Ошибка чтения XML [{str(e)}]!")
        return None


async def executeDbFunction(db_function: str | None, query: dict[str, Any] | None) -> dict[str, Any] | str | None:
    try:
        if not db_function:
            return None

        if not query:
            return None

        if "." in db_function:
            schema, func_name = db_function.split(".", 1)
            safe_func = sql.Identifier(schema, func_name)
        else:
            safe_func = sql.Identifier(db_function)
        safe_query = sql.SQL("SELECT {}(%s::jsonb);").format(safe_func)
        params = [json.dumps(query)]

        async with db_pool.connection() as conn:
            cursor = await conn.execute(safe_query, params)
            row = await cursor.fetchone()

            if row:
                if isinstance(row, dict):
                    result = list(row.values())[0]
                else:
                    result = row[0]

                if isinstance(result, str):
                    try:
                        return json.loads(result)
                    except json.JSONDecodeError:
                        return result

                return result

            return None
    except DatabaseError as db_error:
        pg_exception = db_error.__cause__
        pg_diag = getattr(pg_exception, 'diag', None)

        if pg_diag:
            err_msg = pg_diag.message_primary or str(db_error)
            err_state = pg_diag.sqlstate or "UNKNOWN"
            err_detail = pg_diag.message_detail or ""
            err_hint = pg_diag.message_hint or ""
        else:
            err_msg = str(db_error)
            err_state = "UNKNOWN"
            err_detail = ""
            err_hint = ""

        error_result = (f"Ошибка функции call_pg_function_async: {err_msg}, "
                        f"RETURNED_SQLSTATE: {err_state}, "
                        f"PG_EXCEPTION_DETAIL: {err_detail}, "
                        f"PG_EXCEPTION_HINT: {err_hint}")

        logger.error(error_result, exc_info=True)


async def db_process_task_convert_type_01(task: dict[str, Any]):
    """
    Задача для конвертации выписок РОСРЕЕСТРА по состоянию на 2026 года
    :param task:
    :return:
    """
    task_id: str | None = task.get('payload', {}).get('id')
    if not task_id:
        logger.error(f"Получена задача без ID [{str(task)}]")
        return

    link_user_id: str | None = task.get('payload', {}).get('link_user_id')
    if not link_user_id:
        logger.error(f"Получена задача без User ID [{str(task)}]")
        return

    task_prc: str | None = task.get('payload', {}).get("task_prc")
    if not task_prc:
        logger.error(f"Не определена процедура обработки для [{str(task)}]!")
        return

    batch_task_id: str | None = task.get('payload', {}).get("batch_task_id")
    batch_task_prc: str | None = task.get('payload', {}).get("batch_task_prc")
    batch_task_query: dict[str, Any] | None = task.get('payload', {}).get("batch_task_query")


    if batch_task_id:
        if batch_task_prc is None or batch_task_query is None:
            logger.error(f"Получена задача без конечной процедуры или параметров с ID счетчика [{str(batch_task_id)}]")
            return

    payload: dict[str, Any] = task.get('payload', {})

    try:
        xml_file_path: str | None = payload.get('task_file', {}).get('task_file_path')
        if not xml_file_path:
            return

        xml_file_name: str | None = payload.get('task_file', {}).get('task_file_name')
        if not xml_file_name:
            pass

        tag = await asyncio.to_thread(get_root_tag, xml_file_path)

        ALLOWED_TAGS = [
            'extract_base_params_land',
            'extract_base_params_build',
            'extract_base_params_room',
            'extract_base_params_construction'
        ]

        if tag in ALLOWED_TAGS:
            logger.info(f"Обработка {os.path.basename(xml_file_path)} (Тип: {tag})...")
            parsed_items = await asyncio.to_thread(parse_rosreestr_xml, xml_file_path, tag)

            payload['parsed_data'] = parsed_items
            payload['xml_type'] = tag
        else:
            payload['parsed_data'] = {}
            payload['xml_type'] = 'Unknow'


    except Exception as e:
        logger.error(f"Parsing error for task {task_id}: {e}")
        await save_task_log({
            "id": task_id,
            "link_user_id": link_user_id,
            "task_status": "FAILED",
            "task_error_message": str(e)
        })
        return

    async with async_semaphore:
        r = redis_engine_client
        retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
        attempt = int(await r.get(f"{RETRY_COUNT_DB_ID}:{task_id}") or 1)

        try:
            logger.info(f"Процедура {task_prc} с ID {task_id} для {os.path.basename(xml_file_path)} запущена!")

            result = await executeDbFunction(task_prc, payload)
            if result:
                logger.info(
                    f"Результат процедуры {task_prc} с ID {task_id} для {os.path.basename(xml_file_path)} записан в Лог!")

                # await save_task_log({
                #     "id": task_id,
                #     "link_user_id": link_user_id,
                #     "task_status": "COMPLETED",
                #     "task_message": 'ok'
                # })

            await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'COMPLETED'}), ex=3600)
            await r.delete(retry_key)

            logger.info(f"Процедура {task_prc} с ID {task_id} завершена!")


        except (RedisError, DatabaseError, Exception) as e:
            if attempt < MAX_RETRIES:
                base_backoff = RETRY_SLEEP * (2 ** (attempt - 1))
                capped_backoff = min(base_backoff, MAX_RETRIES)
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
                await r.set(f"rs:agent:taskdb:{task_id}", "failed", ex=3600)
                if not isinstance(e, DatabaseError):
                    await save_task_log({
                        "id": task_id,
                        "link_user_id": link_user_id,
                        "task_status": "FAILED",
                        "task_error_message": str(e)
                    })
        finally:
            if batch_task_id:
                remaining = await r.decr(f"rs:agent:taskdb:tasks:batch:{batch_task_id}:pending")
                if remaining == 0:
                    logger.info(f"Финальная процедура {batch_task_prc} id {batch_task_id} запущена!")
                    await r.delete(f"rs:agent:taskdb:tasks:batch:{batch_task_id}:pending")

                    result = await executeDbFunction(batch_task_prc, batch_task_query)
                    if result:
                        if isinstance(result, dict):
                            if result.get('error', 1) == 0:
                                if result.get('payload'):
                                    await r.xadd(name=f"rs:push:user_{link_user_id}",
                                                 fields={'payload': json.dumps(result.get('payload'))},
                                                 maxlen=50,
                                                 approximate=True)


                    logger.info(f"Финальная процедура {batch_task_prc} id {batch_task_id} завершена!")
            if os.path.exists('local_filename'):
                os.remove('local_filename')


async def db_process_task(task: dict[str, Any]):
    """
    Задачи базы данных
    :param task:
    :return:
    """
    task_id: str | None = task.get('id')
    if not task_id:
        logger.error(f"Получена задача без ID [{str(task)}]")
        return

    link_user_id: str | None = task.get('payload', {}).get('link_user_id')
    if not link_user_id:
        logger.error(f"Получена задача без User [{str(task)}]")
        return

    payload = task.get('payload', {})

    async with async_semaphore:
        r = redis_engine_client
        retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
        attempt = int(await r.get(f"{RETRY_COUNT_DB_ID}:{task_id}") or 1)

        try:
            result = await executeDbFunction('rs.set_task_data_flow', payload)

            await r.set(f"rs:agent:taskdb:{task_id}", json.dumps({'status': 'COMPLETED'}), ex=3600)
            await r.delete(retry_key)

            logger.info(f"{logger.name} {task_id} done")
            await save_task_log({
                "id": task_id,
                "link_user_id": link_user_id,
                "task_status": "COMPLETED",
                "task_message": result
            })

        except (RedisError, DatabaseError, Exception) as e:
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
                if not isinstance(e, DatabaseError):
                    await save_task_log({
                        "id": task_id,
                        "link_user_id": link_user_id,
                        "task_status": "FAILED",
                        "task_error_message": str(e)
                    })
        finally:
            if os.path.exists('local_filename'):
                os.remove('local_filename')


def trigger_stop():
    """Вспомогательная функция для безопасной установки флага остановки."""
    try:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        stop_event.set()


def windows_shutdown_handler(signum, frame):
    logger.info(f"Получен сигнал ОС {signum}. Инициируем плавное завершение...")
    trigger_stop()


def linux_shutdown_handler():
    logger.info("Получен сигнал от Docker. Инициируем плавное завершение...")
    trigger_stop()


async def main():
    """
    Запуск агентов
    f"{_("The agent is running in", lang="ru")
    :return:
    """
    if sys.platform == "win32":
        signal.signal(signal.SIGINT, windows_shutdown_handler)
        signal.signal(signal.SIGTERM, windows_shutdown_handler)
    else:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, linux_shutdown_handler)

    logger.info(f"{_("The agent is running in", lang="ru")} [{datetime.datetime.now()}]...")

    await db_pool.open()
    logger.info("Подключение к БД установлено.")

    r = redis_engine_client
    try:
        while not stop_event.is_set():
            try:
                task_data = await r.brpop(QUEUE_NAME, timeout=REDIS_SUBSCRIBE_BPROP_TIMEOUT)
                if task_data:
                    i, message_json = task_data
                    task = json.loads(message_json)

                    action = task.get('payload', {}).get('action')

                    if not action: continue

                    if action == 'send':
                        """
                        Прочая задача
                        """
                        asyncio.create_task(db_process_task(task))
                    elif action == 'convert_type_01':
                        """
                        Задача конвертации
                        """
                        asyncio.create_task(db_process_task_convert_type_01(task))

            except (ConnectionError, TimeoutError, RedisError) as e:
                logger.error(f"Ошибка связи с Redis: {e}. Спим {RECONNECT_DELAY}с...")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception as e:
                logger.critical(f"Критическая ошибка цикла: {e}")
                await asyncio.sleep(RECONNECT_DELAY)
    except asyncio.CancelledError:
        logger.info("Завершаем работу...")
    finally:
        logger.info(_("Ожидаем завершения запущенных фоновых задач..."))
        current_task = asyncio.current_task()

        pending_tasks = [t for t in asyncio.all_tasks() if t is not current_task]

        if pending_tasks:
            await asyncio.wait(pending_tasks, timeout=60)
            logger.info(_("Все фоновые задачи обработаны..."))

        print(f"{logger.name} {_("connections are closing")} [db, redis]...")

        await db_pool.close()
        await r.aclose()

        print(f"{logger.name} {_("all connections are closed")}...")


if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Завершаем работу...")
