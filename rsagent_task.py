"""
    Версия 2.5.0

    Copyright (c) 2026 ООО "РС-Автоматизация" ИНН 7203457349. Все права защищены.

    Данный исходный код является конфиденциальным и принадлежит ООО "РС-Автоматизация".
    Любое копирование, распространение, модификация или использование
    данного кода, полностью или частично, строго запрещено без
    предварительного письменного разрешения правообладателя.
"""
import asyncio
import json
import os
import random
import signal
import sys
import xml.etree.ElementTree as ET
from typing import Any

import redis.asyncio as redis
from psycopg import DatabaseError, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from redis.exceptions import ConnectionError, RedisError, TimeoutError

from ai.rs_ai_yolo import process_image_yolo
from convertors.rs_cnv_format import RS_PARSERS
from utils.rs_env import rs_settings
from utils.rs_i18n import _
from utils.rs_logger import get_logger

rs_settings.load_env_file()

logger = get_logger("RSAgentTask")

stop_event = asyncio.Event()

QUEUE_NAME: str = os.getenv("QUEUE_NAME") or ''
MAX_TASKS: int = int(os.getenv("MAX_CONCURRENT_TASKS", "10"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
RECONNECT_DELAY: int = int(os.getenv("RECONNECT_DELAY", "5"))

DEBUG: bool = rs_settings.get_bool("DEBUG", False)

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
        "row_factory": dict_row,
        "autocommit": True
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


async def save_task_log(payload: dict[str, Any]):
    """Отдельная безопасная функция для записи логов в БД"""
    try:
        await execute_db_function('rs.set_taskdb_log', payload)
    except Exception as e:
        logger.error(f"Не удалось записать лог задачи в БД: {e}")


def get_root_tag(task_file: str) -> str | None:
    """Быстро достает имя корневого тега и гарантированно закрывает файл"""
    try:
        with open(task_file, 'rb') as f:
            context = ET.iterparse(f, events=('start',))
            i, elem = next(context)
            return elem.tag.split('}')[-1]
    except Exception as e:
        logger.error(f"Ошибка чтения XML [{str(e)}]!")
        return None


async def delayed_requeue(task: dict[str, Any], delay: float, retry_key: str, attempt: int):
    """Фоновый возврат задачи в очередь без блокировки семафора"""
    try:
        await redis_engine_client.set(retry_key, attempt + 1, ex=3600)
        await asyncio.sleep(delay)
        await redis_engine_client.lpush(QUEUE_NAME, json.dumps(task))
    except Exception as e:
        logger.error(f"Ошибка при отложенном возврате задачи в очередь: {e}")


async def execute_db_function(db_function: str | None, payload: dict[str, Any] | None) -> dict[str, Any] | str | None:
    try:
        if not db_function or not payload:
            raise ValueError(_("Database function name or parameters not specified!"))

        if "." in db_function:
            schema, func_name = db_function.split(".", 1)
            safe_func = sql.Identifier(schema, func_name)
        else:
            safe_func = sql.Identifier(db_function)

        safe_query = sql.SQL("SELECT {}(%s);").format(safe_func)
        params = [Jsonb(payload)]

        async with db_pool.connection() as conn:
            cursor = await conn.execute(safe_query, params)
            row = await cursor.fetchone()

            if row:
                result = list(row.values())[0] if isinstance(row, dict) else row[0]

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

        error_result = (
            _("Function error call_pg_function_async: {err_msg}, RETURNED_SQLSTATE: {err_state}, "
              "PG_EXCEPTION_DETAIL: {err_detail}, PG_EXCEPTION_HINT: {err_hint}")
            .format(err_msg=err_msg, err_state=err_state, err_detail=err_detail, err_hint=err_hint)
        )

        logger.error(error_result, exc_info=True)
        raise


async def finalize_batch_and_cleanup(
        batch_task_id: str | None,
        batch_task_prc: str | None,
        batch_task_query: dict[str, Any] | None,
        batch_task_after: dict[str, Any] | None,
        link_user_id: str,
        xml_file_path: str | None,
        task_id: str | None = None,
        task_result: dict[str, Any] | str | None = None,
        task_status: str = "SUCCESS"
):
    """
    Отправляет SSE-прогресс по каждому файлу, финализирует батч и удаляет временный файл.
    {
      "event": "batch_file_progress",
      "batch_task_id": "batch_991",
      "task_id": "f47ac10b-58cc...",
      "status": "SUCCESS",
      "remaining": 3,
      "completed": 7,
      "total": 10,
      "percent": 70.0
    }
    """
    """Отправляет SSE-прогресс по каждому файлу, финализирует батч и удаляет временный файл."""
    r = redis_engine_client
    stream_name: str = f"rs:push:user_{link_user_id}"
    resolved_file_name: str = (os.path.basename(xml_file_path) if xml_file_path else "unknown")

    try:
        if batch_task_id:
            batch_key: str = f"rs:agent:taskdb:tasks:batch:{batch_task_id}:pending"
            remaining: int = await r.decr(batch_key)
            safe_remaining = max(remaining, 0)

            progress_payload = {
                **({"action": batch_task_after.get('action')} if batch_task_after.get('action') is not None else {}),
                "event": "batch_file_progress",
                "batch_task_id": batch_task_id,
                "task_id": task_id,
                "file_name": resolved_file_name,
                "task_result": task_result,
                "status": task_status,
                "remaining": safe_remaining
            }

            await r.xadd(
                name=stream_name,
                fields={"payload": json.dumps(progress_payload, ensure_ascii=False)},
                maxlen=100,
                approximate=True
            )

            if remaining <= 0:
                await r.delete(batch_key)
                if batch_task_prc is not None and batch_task_query is not None:
                    logger.info(f"Финальная процедура {batch_task_prc} id {batch_task_id} запущена!")
                    result = await execute_db_function(batch_task_prc, batch_task_query)
                    if isinstance(result, dict) and result.get('error', 1) == 0 and result.get('result'):
                        result_out: dict[str, Any] = {
                            **({"action": batch_task_after.get('action')} if batch_task_after.get(
                                'action') is not None else {}),
                            "event": "batch_result_files_progress",
                            'result': result.get('result')
                        }
                        await r.xadd(
                            name=stream_name,
                            fields={"payload": json.dumps(result_out, ensure_ascii=False)},
                            maxlen=100,
                            approximate=True
                        )
                    logger.info(f"Финальная процедура {batch_task_prc} id {batch_task_id} завершена!")

                elif batch_task_prc is None and batch_task_query is None and batch_task_after is not None:
                    result_out: dict[str, Any] = {**({"action": batch_task_after.get('action')} if batch_task_after.get(
                        'action') is not None else {}),
                                                  "event": "batch_files_progress",
                                                  "batch_task_after": batch_task_after
                                                  }
                    await r.xadd(
                        name=stream_name,
                        fields={"payload": json.dumps(result_out, ensure_ascii=False)},
                        maxlen=100,
                        approximate=True
                    )
                    logger.info(f"Финальная обработка id {batch_task_id} завершена!")
        else:
            # Одиночный файл (без batch_task_id)
            single_payload = {
                "event": "single_file_completed",
                "task_id": task_id,
                "file_name": resolved_file_name,
                "status": task_status
            }
            await r.xadd(
                name=stream_name,
                fields={"payload": json.dumps(single_payload, ensure_ascii=False)},
                maxlen=100,
                approximate=True
            )

    except Exception as e:
        logger.error(f"Ошибка при финализации батча {batch_task_id}: {e}")
    finally:
        if xml_file_path and os.path.exists(xml_file_path):
            try:
                os.remove(xml_file_path)
            except OSError as e:
                logger.warning(f"Не удалось удалить файл {xml_file_path}: {e}")


async def db_process_task_ai_01(task: dict[str, Any]):
    """
    Задача распознавания построек на снимках с помощью YOLO (DJI Mini).
    """
    payload: dict[str, Any] = task.get('payload', {})
    task_id: str | None = payload.get('id')
    link_user_id: str | None = payload.get('link_user_id')
    task_prc: str | None = payload.get('task_prc')  # Например, 'rs.set_task_ai_result'

    batch_task_id: str | None = payload.get("batch_task_id")
    batch_task_prc: str | None = payload.get("batch_task_prc")
    batch_task_query: dict[str, Any] | None = payload.get("batch_task_query")
    batch_task_after: dict[str, Any] | None = payload.get("batch_task_after")

    task_meta: dict[str, Any] = payload.get("task_meta") or task.get("task_meta") or {}

    task_file_info: dict[str, Any] = payload.get('task_file', {})
    image_file_path: str | None = task_file_info.get('task_file_path')
    image_file_name: str | None = task_file_info.get('task_file_name')
    delete_source: bool = bool(task_meta.get('delete-source', False))

    if not task_id or not link_user_id or not image_file_path:
        logger.error(f"AI-задача пропущена: отсутствуют обязательные поля [{str(task)}]")
        if link_user_id:
            await finalize_batch_and_cleanup(
                batch_task_id, batch_task_prc, batch_task_query, batch_task_after,
                link_user_id, image_file_path if delete_source else None,
                task_id, None, "ERROR"
            )
        return

    r = redis_engine_client
    await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'PROCESSING'}), ex=3600)

    # 1. Запуск распознавания YOLO в отдельном потоке
    try:
        logger.info(f"YOLO распознавание запущено для {os.path.basename(image_file_path)} (ID: {task_id})...")
        ai_report = await asyncio.to_thread(process_image_yolo, image_file_path, task_meta)

        payload['ai_result'] = ai_report
        total_found = ai_report['summary']['total_objects']
        result_img_path = ai_report['image_info']['result_path']

        logger.info(
            f"YOLO завершил обработку {os.path.basename(image_file_path)}: "
            f"найдено объектов: {total_found}, сохранено в: {result_img_path}"
        )

    except Exception as e:
        logger.error(f"Ошибка YOLO для задачи {task_id}: {e}")
        await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'ERROR'}), ex=3600)
        await save_task_log({
            "id": task_id,
            "link_user_id": link_user_id,
            "task_status": "ERROR",
            "task_log": {"error": str(e), "task_meta": task_meta},
            "task_error_message": str(e)
        })
        await finalize_batch_and_cleanup(
            batch_task_id, batch_task_prc, batch_task_query, batch_task_after,
            link_user_id, image_file_path if delete_source else None,
            task_id, image_file_name, "ERROR"
        )
        return

    retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
    attempt = int(await r.get(retry_key) or 1)
    is_retrying = False
    task_status = "ERROR"

    try:
        logger.info(f"Процедура {task_prc} с ID {task_id} для {os.path.basename(image_file_path)} запущена!")

        result_db_task = await execute_db_function(task_prc, payload)
        if result_db_task:
            logger.info(f"Результат процедуры {task_prc} с ID {task_id} получен!")

            task_status = "SUCCESS" if isinstance(result_db_task, dict) and result_db_task.get('error',
                                                                                               1) == 0 else 'ERROR'
            await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': task_status}), ex=3600)

        await r.delete(retry_key)
        logger.info(f"Процедура {task_prc} с ID {task_id} завершена!")

    except (RedisError, DatabaseError, Exception) as e:
        if attempt < MAX_RETRIES:
            is_retrying = True
            base_backoff = RETRY_SLEEP * (2 ** (attempt - 1))
            capped_backoff = min(base_backoff, 60)
            jitter = capped_backoff * 0.3 * random.random()
            final_delay = capped_backoff + jitter

            logger.warning(
                f"AI Task {task_id} failed DB save (attempt {attempt}). "
                f"Retrying in {final_delay:.2f}s. Error: {e}"
            )

            payload.pop('ai_result', None)
            asyncio.create_task(delayed_requeue(task, final_delay, retry_key, attempt))
        else:
            await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'ERROR'}), ex=3600)
            await r.delete(retry_key)
            if not isinstance(e, DatabaseError):
                await save_task_log({
                    "id": task_id,
                    "link_user_id": link_user_id,
                    "task_status": "ERROR",
                    "task_error_message": str(e)
                })
    finally:
        if not is_retrying:
            await finalize_batch_and_cleanup(
                batch_task_id, batch_task_prc, batch_task_query, batch_task_after,
                link_user_id, image_file_path if delete_source else None,
                task_id, image_file_name, task_status
            )


async def db_process_task_convert_type_01(task: dict[str, Any]):
    """Задача для конвертации выписок РОСРЕЕСТРА по состоянию на 2026 год"""
    payload: dict[str, Any] = task.get('payload', {})
    task_id: str | None = payload.get('id')
    link_user_id: str | None = payload.get('link_user_id')
    task_prc: str | None = payload.get("task_prc")

    batch_task_id: str | None = payload.get("batch_task_id")
    batch_task_prc: str | None = payload.get("batch_task_prc")
    batch_task_query: dict[str, Any] | None = payload.get("batch_task_query")
    batch_task_after: dict[str, Any] | None = payload.get("batch_task_after")
    xml_file_path: str | None = payload.get('task_file', {}).get('file_path')

    if not task_id or not link_user_id or not task_prc or not xml_file_path:
        logger.error(f"Задача пропущена: отсутствуют обязательные поля [{str(task)}]")
        if link_user_id:
            await finalize_batch_and_cleanup(
                batch_task_id, batch_task_prc, batch_task_query, batch_task_after,
                link_user_id, xml_file_path, task_id, None, "ERROR"
            )
        return

    stream_name: str = f"rs:push:user_{link_user_id}"

    result_db_task: dict[str, Any] | str | None = {}

    r = redis_engine_client
    await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'PROCESSING'}), ex=3600)

    await asyncio.sleep(10)

    progress_payload = {
        **({"action": batch_task_after.get('action')} if batch_task_after.get('action') is not None else {}),
        "event": "batch_file_prcessing",
        "batch_task_id": batch_task_id,
        "task_id": task_id,
        "file_name": xml_file_path,
        "status": 'PROCESSING'
    }

    await r.xadd(
        name=stream_name,
        fields={"payload": json.dumps(progress_payload, ensure_ascii=False)},
        maxlen=100,
        approximate=True
    )

    await asyncio.sleep(10)

    try:
        tag = await asyncio.to_thread(get_root_tag, xml_file_path)

        ALLOWED_TAGS = {
            'extract_base_params_land',
            'extract_base_params_build',
            'extract_base_params_room',
            'extract_base_params_construction',
            'extract_cadastral_plan_territory'
        }

        if tag in ALLOWED_TAGS:
            logger.info(f"Обработка {os.path.basename(xml_file_path)} (Тип: {tag})...")
            parsed_items = await asyncio.to_thread(RS_PARSERS['parse_rosreestr'], xml_file_path, tag)
            payload['parsed_data'] = parsed_items
            payload['xml_type'] = tag
        else:
            payload['parsed_data'] = {}
            payload['xml_type'] = 'Unknown'

    except Exception as e:
        logger.error(f"Parsing error for task {task_id}: {e}")
        await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'ERROR'}), ex=3600)
        await save_task_log({
            "id": task_id,
            "link_user_id": link_user_id,
            "task_status": "ERROR",
            "task_error_message": str(e)
        })
        await finalize_batch_and_cleanup(
            batch_task_id, batch_task_prc, batch_task_query, batch_task_after,
            link_user_id, xml_file_path, task_id, None, "ERROR"
        )
        return

    retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
    attempt = int(await r.get(retry_key) or 1)
    is_retrying = False
    task_status = "ERROR"

    try:
        logger.info(f"Процедура {task_prc} с ID {task_id} для {os.path.basename(xml_file_path)} запущена!")

        result_db_task = await execute_db_function(task_prc, payload)
        if result_db_task:
            logger.info(f"Результат процедуры {task_prc} с ID {task_id} получен!")

            task_status = "SUCCESS" if isinstance(result_db_task, dict) and result_db_task.get('error',
                                                                                               1) == 0 else 'ERROR'
            await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': task_status}), ex=3600)

        await r.delete(retry_key)
        logger.info(f"Процедура {task_prc} с ID {task_id} завершена!")

    except (RedisError, DatabaseError, Exception) as e:
        if attempt < MAX_RETRIES:
            is_retrying = True
            base_backoff = RETRY_SLEEP * (2 ** (attempt - 1))
            capped_backoff = min(base_backoff, 60)
            jitter = capped_backoff * 0.3 * random.random()
            final_delay = capped_backoff + jitter

            logger.warning(
                f"Task {task_id} failed (attempt {attempt}). "
                f"Retrying in {final_delay:.2f}s (backoff: {base_backoff:.2f} + jitter: {jitter:.2f}). "
                f"Error: {e}"
            )

            # Очищаем распарсенные данные перед возвратом в очередь, чтобы не гонять мегабайты в Redis
            payload.pop('parsed_data', None)
            asyncio.create_task(delayed_requeue(task, final_delay, retry_key, attempt))
        else:
            await r.set(f"rs:agent:taskdb:tasks:task_{task_id}", json.dumps({'status': 'ERROR'}), ex=3600)
            await r.delete(retry_key)
            if not isinstance(e, DatabaseError):
                await save_task_log({
                    "id": task_id,
                    "link_user_id": link_user_id,
                    "task_status": "ERROR",
                    "task_error_message": str(e)
                })
    finally:
        if not is_retrying:
            await finalize_batch_and_cleanup(
                batch_task_id, batch_task_prc, batch_task_query, batch_task_after, link_user_id, xml_file_path, task_id,
                result_db_task, task_status
            )


async def db_process_task(task: dict[str, Any]):
    """Задачи базы данных"""
    task_id: str | None = task.get('id')
    if not task_id:
        logger.error(f"Получена задача без ID [{str(task)}]")
        return

    payload = task.get('payload', {})
    link_user_id: str | None = payload.get('link_user_id')
    if not link_user_id:
        logger.error(f"Получена задача без User [{str(task)}]")
        return

    r = redis_engine_client
    retry_key = f"{RETRY_COUNT_DB_ID}:{task_id}"
    attempt = int(await r.get(retry_key) or 1)

    try:
        result = await execute_db_function('rs.set_task_data_flow', payload)

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
            base_backoff = RETRY_SLEEP * (2 ** (attempt - 1))
            capped_backoff = min(base_backoff, 60)
            jitter = capped_backoff * 0.3 * random.random()
            final_delay = capped_backoff + jitter

            logger.warning(
                f"Task {task_id} failed (attempt {attempt}). "
                f"Retrying in {final_delay:.2f}s (backoff: {base_backoff:.2f} + jitter: {jitter:.2f}). "
                f"Error: {e}"
            )

            asyncio.create_task(delayed_requeue(task, final_delay, retry_key, attempt))
        else:
            await r.set(f"status:{task_id}", "failed", ex=3600)
            await r.delete(retry_key)
            if not isinstance(e, DatabaseError):
                await save_task_log({
                    "id": task_id,
                    "link_user_id": link_user_id,
                    "task_status": "FAILED",
                    "task_error_message": str(e)
                })


def trigger_stop():
    """Вспомогательная функция для безопасной установки флага остановки."""
    try:
        loop_trigger = asyncio.get_running_loop()
        loop_trigger.call_soon_threadsafe(stop_event.set)
    except RuntimeError:
        stop_event.set()


def windows_shutdown_handler(signum, frame):
    logger.info(f"Получен сигнал ОС {signum}. Инициируем плавное завершение...")
    trigger_stop()


def linux_shutdown_handler():
    logger.info("Получен сигнал от Docker. Инициируем плавное завершение...")
    trigger_stop()


async def main():
    if sys.platform == "win32":
        signal.signal(signal.SIGINT, windows_shutdown_handler)
        signal.signal(signal.SIGTERM, windows_shutdown_handler)
    else:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, linux_shutdown_handler)

    # logger.info(f"{_('The agent is running in', lang='ru')} [{datetime.datetime.now()}]...")

    await db_pool.open()
    logger.info("Подключение к БД установлено.")

    r = redis_engine_client
    try:
        while not stop_event.is_set():
            try:
                await async_semaphore.acquire()

                task_data = await r.brpop(QUEUE_NAME, timeout=REDIS_SUBSCRIBE_BPROP_TIMEOUT)
                if task_data:
                    i, message_json = task_data
                    task = json.loads(message_json)
                    action = task.get('payload', {}).get('action')

                    if action == 'send':
                        task_future = asyncio.create_task(db_process_task(task))
                        task_future.add_done_callback(lambda i: async_semaphore.release())

                    elif action == 'convert_type_01':
                        task_future = asyncio.create_task(db_process_task_convert_type_01(task))
                        task_future.add_done_callback(lambda i: async_semaphore.release())

                    elif action == 'ai_01':
                        task_future = asyncio.create_task(db_process_task_ai_01(task))
                        task_future.add_done_callback(lambda i: async_semaphore.release())
                    else:
                        logger.warning(f"Unknown action: {action}")
                        async_semaphore.release()
                else:
                    async_semaphore.release()

            except (ConnectionError, TimeoutError, RedisError) as e:
                async_semaphore.release()
                logger.error(f"Ошибка связи с Redis: {e}. Спим {RECONNECT_DELAY}с...")
                await asyncio.sleep(RECONNECT_DELAY)

            except Exception as e:
                async_semaphore.release()
                logger.critical(f"Критическая ошибка цикла: {e}")
                await asyncio.sleep(RECONNECT_DELAY)

    except asyncio.CancelledError:
        pass
    finally:
        logger.info(_("Waiting for running background tasks to complete..."))
        current_task = asyncio.current_task()
        pending_tasks = [t for t in asyncio.all_tasks() if t is not current_task]

        if pending_tasks:
            await asyncio.wait(pending_tasks, timeout=60)
            logger.info(_("All background tasks have been processed..."))

        print(f"{logger.name} {_('connections are closing')} [db, redis]...")

        await db_pool.close()
        await r.aclose()

        logger.info(f"{logger.name} {_('all connections are closed')}...")


if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(_("Stopping due to KeyboardInterrupt..."))
