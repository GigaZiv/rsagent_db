"""
    Версия 2.6
"""
import logging
import os
import socket
from logging.handlers import RotatingFileHandler
from pathlib import Path

CONTAINER_ID: str = socket.gethostname()


def get_logger(name="AgentLogger"):
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    formatter = logging.Formatter(f'%(asctime)s [%(levelname)s] (id:{CONTAINER_ID}) %(name)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Надежный путь: читаем из ENV, либо берем путь относительно текущего файла
    # Замените .parent на .parent.parent при необходимости, чтобы выйти в корень проекта
    default_log_dir = Path(__file__).resolve().parent / "logs"
    log_dir_env = os.getenv("LOG_DIR", str(default_log_dir))
    log_dir = Path(log_dir_env)

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "rlog.log"

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    except PermissionError:
        logger.warning(f"Нет прав на запись в {log_dir}. Файловый лог отключен.")
    except Exception as e:
        logger.warning(f"Ошибка при создании файл-хендлера: {e}")

    return logger