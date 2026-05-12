"""
    async def test():
        s3 = S3Service(max_concurrent_tasks=3)

        # 1. Проверяем бакет
        await s3.ensure_bucket_exists()

        # 2. Массовая загрузка (например, 10 файлов)
        # Создадим фиктивные файлы для теста
        for i in range(5):
            with open(f"test_{i}.txt", "w") as f: f.write("hello")

        tasks = [(f"test_{i}.txt", f"folder/test_{i}.txt") for i in range(5)]
        results = await s3.upload_many(tasks, remove_after=True)
        print(f"Загружено файлов: {sum(results)}")

    if __name__ == "__main__":
        asyncio.run(test())

    # Использование внутри класса Агента
    class MyAgent:
        def __init__(self, s3_service: S3Service):
            self.s3 = s3_service

        async def process_task(self, task):
            # Скачиваем входные данные
            await self.s3.download(task.s3_input_key, "input.tmp")

            # ... тут какая-то логика обработки ...

            # Загружаем результат
            await self.s3.upload("output.tmp", task.s3_output_key)

    # Запуск
    s3_service = S3Service()
    agent = MyAgent(s3_service)
"""
import asyncio
import os

import aioboto3
from botocore.exceptions import ClientError

from utils.rs_env import rs_settings
from utils.rs_logger import get_logger

# Настройка логирования
logger = get_logger("RSS3Service")

rs_settings.load_env_file()


def _ensure_local_dir(local_path: str):
    """
    Создает локальные директории, если они отсутствуют

    # Папки 'archive' и 'reports' создадутся сами в процессе скачивания
    await s3.download(
        s3_key="data/report.pdf",
        local_path="archive/2024/reports/report.pdf"
    )
    """
    directory = os.path.dirname(local_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Создана локальная директория: {directory}")


class S3Service:
    def __init__(self, max_concurrent_tasks: int = 5, retries: int = 3):
        """
        :param max_concurrent_tasks: Максимальное кол-во одновременных операций с S3
        """
        self.session = aioboto3.Session()
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.retries = retries

        # Загрузка конфигов из окружения
        self.config = {
            "endpoint_url": os.getenv("AWS_S3_ENDPOINT_URL"),
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        }
        self.default_bucket = os.getenv("AWS_STORAGE_BUCKET_NAME")
        self.region = os.getenv("AWS_S3_REGION", "us-east-1")

    def _get_client(self):
        """Создает контекстный менеджер клиента S3"""
        return self.session.client("s3", **self.config)

    async def ensure_bucket_exists(self, bucket: str = None):
        """Проверяет наличие бакета и создает его, если нужно"""
        target_bucket = bucket or self.default_bucket
        async with self._get_client() as s3:
            try:
                await s3.head_bucket(Bucket=target_bucket)
                return True
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') == '404':
                    logger.info(f"Бакет {target_bucket} не найден. Создаю...")
                    conf = {'LocationConstraint': self.region} if self.region != 'us-east-1' else {}
                    await s3.create_bucket(Bucket=target_bucket, CreateBucketConfiguration=conf)
                    return True
                logger.error(f"Ошибка доступа к бакету: {e}")
                return False

    async def upload(self, local_path: str, s3_key: str, bucket: str = None, remove_after: bool = False):
        """Загрузка одного файла с опцией удаления оригинала"""
        if not os.path.exists(local_path):
            logger.error(f"Файл {local_path} не найден")
            return False

        target_bucket = bucket or self.default_bucket

        for attempt in range(1, self.retries + 1):
            try:
                async with self.semaphore:
                    async with self._get_client() as s3:
                        await s3.upload_file(local_path, target_bucket, s3_key)
                        logger.info(f"Успех: {s3_key} загружен в {target_bucket}")
                        if remove_after:
                            os.remove(local_path)
                            logger.debug(f"Локальный файл {local_path} удален")
                        return True
            except Exception as e:
                wait_time = 2 ** attempt  # Экспоненциальная пауза: 2с, 4с, 8с...
                if attempt < self.retries:
                    logger.warning(f"Попытка {attempt} не удалась: {e}. Повтор через {wait_time}с...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Все {self.retries} попыток загрузки {s3_key} провалены: {e}")
        return False

    async def download(self, s3_key: str, local_path: str, bucket: str = None):
        """Скачивание файла из S3"""

        _ensure_local_dir(local_path)

        target_bucket = bucket or self.default_bucket
        async with self.semaphore:
            for attempt in range(1, self.retries + 1):
                try:
                    async with self._get_client() as s3:
                        await s3.download_file(target_bucket, s3_key, local_path)
                        logger.info(f"Файл {s3_key} скачан в {local_path}")
                        return True
                except Exception as e:
                    wait_time = 2 ** attempt  # Экспоненциальная пауза: 2с, 4с, 8с...
                    if attempt < self.retries:
                        logger.warning(f"Попытка {attempt} не удалась: {e}. Повтор через {wait_time}с...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Все {self.retries} попыток загрузки {s3_key} провалены: {e}")
            return False

    async def get_url(self, s3_key: str, expires_in: int = 3600, bucket: str = None):
        """Генерация временной ссылки (Presigned URL)"""
        target_bucket = bucket or self.default_bucket
        async with self._get_client() as s3:
            return await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': target_bucket, 'Key': s3_key},
                ExpiresIn=expires_in
            )

    async def delete(self, s3_key: str, bucket: str = None):
        """Удаление объекта из S3"""
        target_bucket = bucket or self.default_bucket
        async with self._get_client() as s3:
            try:
                await s3.delete_object(Bucket=target_bucket, Key=s3_key)
                return True
            except Exception as e:
                logger.error(f"Ошибка удаления {s3_key}: {e}")
                return False

    async def upload_many(self, tasks: list, remove_after: bool = False):
        """
        Массовая загрузка. 
        tasks: [("local_path", "s3_key"), ...]
        """
        # Сначала убедимся, что бакет на месте
        await self.ensure_bucket_exists()

        jobs = [self.upload(local, key, remove_after=remove_after) for local, key in tasks]
        return await asyncio.gather(*jobs)

    async def download_many(self, tasks: list):
        """
        Массовое скачивание файлов из S3.
        :param tasks: список кортежей [(s3_key, local_path), ...]
        :return: список результатов [True, False, True, ...]
        """
        # Создаем список корутин для скачивания
        download_jobs = [
            self.download(s3_key, local_path)
            for s3_key, local_path in tasks
        ]

        # Запускаем всё параллельно.
        # Семафор внутри self.download сам проконтролирует нагрузку.
        results = await asyncio.gather(*download_jobs)

        logger.info(f"Массовое скачивание завершено: {sum(results)} из {len(tasks)} успешно.")
        return results
