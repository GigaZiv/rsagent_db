"""
    Версия 2.5.2
    - Исправленная архитектура ThreadPoolExecutor
    async def generate_yolo_upload_url_view(request):
        if request.method == 'POST':
            unique_filename = f"uploads/yolo_queue/image_{uuid.uuid4().hex}.jpg"

            # Формируем настройки для YOLO
            yolo_settings = {
                'target-folder': 'processed/results/', # Куда положить итог
                'model-version': 'v8-nano',            # Какую модель использовать
                'confidence-threshold': '0.75',        # Порог уверенности
                'callback-id': 'task-9988'             # ID задачи для webhook'а
            }

            manager = BotoCoreManager()
            presigned_data = await manager.get_presigned_upload_object_url(
                object_name=unique_filename,
                metadata=yolo_settings
            )

            presigned_data['object_key'] = unique_filename
            return JsonResponse(presigned_data)

        import boto3

        s3_client = boto3.client('s3')

        def handler(event, context):
            # Достаем инфу о загруженном файле из события
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']

            # Запрашиваем метаданные файла
            response = s3_client.head_object(Bucket=bucket, Key=key)
            metadata = response.get('Metadata', {})

            # Boto3 автоматически отрезает префикс 'x-amz-meta-',
            # поэтому ключи будут именно теми, что мы задавали в Django!
            target_folder = metadata.get('target-folder', 'default/')
            model_version = metadata.get('model-version', 'v8-default')
            confidence = float(metadata.get('confidence-threshold', '0.5'))
            task_id = metadata.get('callback-id')

            print(f"Запускаю YOLO {model_version} с порогом {confidence}...")

            # 1. Скачиваем файл...
            # 2. Прогоняем через нейросеть...
            # 3. Сохраняем в target_folder...
            # 4. Шлем Webhook в Django, передавая task_id...

            return {"status": "ok"}
"""
import asyncio
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncIterable

from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError
from botocore.session import get_session

from rs_env import rs_settings

rs_settings.load_env_file()

botocore_config = Config(
    max_pool_connections=rs_settings.get_int('AWS_BOTOCORE_MAX_WORKERS', 10),
    connect_timeout=rs_settings.get_int('AWS_BOTOCORE_CONNECTION_TIMEOUT', 5),
    read_timeout=rs_settings.get_int('AWS_BOTOCORE_READ_TIMEOUT', 60),
    retries={'max_attempts': rs_settings.get_int('AWS_BOTOCORE_RETRIES', 10)}
)

# Хранилище для обеспечения потокобезопасности botocore клиентов
_thread_local = threading.local()


def _read_chunk_from_stream(body_stream, chunk_size: int):
    return body_stream.read(chunk_size)


def _read_file_chunk(file_descriptor, chunk_size: int) -> bytes:
    return file_descriptor.read(chunk_size)


def _write_chunk_to_disk(file_descriptor, chunk: bytes):
    file_descriptor.write(chunk)


def _cleanup_failed_file(local_path: str):
    try:
        if os.path.exists(local_path):
            os.remove(local_path)
    except Exception:
        pass


class BotoCoreManager:
    """
        Менеджер S3 Storage Object
    """

    # Пул потоков вынесен на уровень класса для переиспользования всеми инстансами
    _executor = ThreadPoolExecutor(
        max_workers=rs_settings.get_int('AWS_BOTOCORE_MAX_WORKERS', 10),
        thread_name_prefix="RSBotocoreIO"
    )

    def __init__(self):
        self.bucket_name = rs_settings.get_str('AWS_STORAGE_BUCKET_NAME')
        self.region_name = rs_settings.get_str('AWS_S3_REGION')
        self.aws_secret_access_key = rs_settings.get_str('AWS_SECRET_ACCESS_KEY')
        self.aws_access_key_id = rs_settings.get_str('AWS_ACCESS_KEY_ID')
        self.endpoint_url = rs_settings.get_str('AWS_S3_ENDPOINT_URL')

    def _get_client(self):
        """
        Гарантирует, что каждый рабочий поток имеет свой собственный,
        изолированный инстанс клиента botocore.
        """
        if not hasattr(_thread_local, 'client'):
            session = get_session()
            _thread_local.client = session.create_client(
                's3',
                config=botocore_config,
                region_name=self.region_name,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_access_key_id=self.aws_access_key_id,
                endpoint_url=self.endpoint_url,
            )
        return _thread_local.client

    def _sync_execute(self, operation_name: str, kwargs: dict) -> dict:
        client = self._get_client()
        return client._make_api_call(operation_name, kwargs)

    async def _execute(self, operation_name: str, kwargs: dict) -> dict:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_execute,
            operation_name,
            kwargs
        )

    async def get_presigned_object_url(self, object_name: str, expiration_seconds: int = 600) -> str | None:
        loop = asyncio.get_running_loop()
        params = {'Bucket': self.bucket_name, 'Key': object_name}

        def _sync_generate():
            client = self._get_client()
            return client.generate_presigned_url('get_object', Params=params, ExpiresIn=expiration_seconds)

        try:
            return await loop.run_in_executor(self._executor, _sync_generate)
        except Exception as e:
            print(e)
            return None

    async def get_presigned_upload_object_url(self, object_name: str, expiration_seconds: int = 600,
                                              metadata: dict = None) -> dict | None:
        loop = asyncio.get_running_loop()

        # Базовые условия безопасности
        conditions = [
            ["content-length-range", 1, 100 * 1024 * 1024],
        ]
        fields = {}

        # 1. Инжектируем метаданные, если они переданы
        if metadata:
            for key, value in metadata.items():
                # S3 требует префикс x-amz-meta- для кастомных полей
                meta_key = f"x-amz-meta-{key}"
                # Все значения должны быть строками
                meta_value = str(value)

                fields[meta_key] = meta_value
                # Строго привязываем значение в Conditions (защита от подмены на фронтенде)
                conditions.append({meta_key: meta_value})

        def _sync_generate_post():
            client = self._get_client()
            return client.generate_presigned_post(
                self.bucket_name,
                object_name,
                Fields=fields if fields else None,
                Conditions=conditions,
                ExpiresIn=expiration_seconds
            )

        try:
            return await loop.run_in_executor(self._executor, _sync_generate_post)
        except Exception as e:
            # print(f"Ошибка генерации presigned URL: {e}")
            return None

    async def stream_file_chunks(self, object_name: str, chunk_size: int = 5 * 1024 * 1024):
        kwargs = {'Bucket': self.bucket_name, 'Key': object_name}
        response = await self._execute('GetObject', kwargs)
        body_stream = response['Body']

        try:
            loop = asyncio.get_running_loop()
            while True:
                chunk = await loop.run_in_executor(
                    self._executor,
                    _read_chunk_from_stream,
                    body_stream,
                    chunk_size
                )
                if not chunk:
                    break
                yield chunk
        except ClientError as err_boto_client_exception:
            error_code = err_boto_client_exception.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise FileNotFoundError(f"Файл {object_name} не найден на сервере")
            elif error_code == 'AccessDenied':
                raise PermissionError("Нет прав для чтения этого файла")
            else:
                raise
        except BotoCoreError as err_boto_core_exception:
            raise ConnectionError("Сервер хранения временно недоступен")
        finally:
            body_stream.close()

    async def upload_files_from_stream(self, async_stream: AsyncIterable[bytes], object_name: str,
                                       chunk_size: int = 5 * 1024 * 1024) -> bool:
        """Минимальный chunk_size изменен на 5 МБ согласно спецификации S3."""
        upload_id = None
        try:
            create_kwargs = {'Bucket': self.bucket_name, 'Key': object_name}
            mp_init = await self._execute('CreateMultipartUpload', create_kwargs)
            upload_id = mp_init['UploadId']

            parts, part_number, buffer = [], 1, bytearray()

            async for chunk in async_stream:
                if not chunk:
                    continue
                buffer.extend(chunk)

                if len(buffer) >= chunk_size:
                    part_response = await self._execute('UploadPart', {
                        'Bucket': self.bucket_name, 'Key': object_name,
                        'UploadId': upload_id, 'PartNumber': part_number, 'Body': bytes(buffer)
                    })
                    parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                    part_number += 1
                    buffer.clear()

            if buffer:
                part_response = await self._execute('UploadPart', {
                    'Bucket': self.bucket_name, 'Key': object_name,
                    'UploadId': upload_id, 'PartNumber': part_number, 'Body': bytes(buffer)
                })
                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})

            # Если файл был пустым (0 байт), Multipart Upload завершится ошибкой.
            if not parts:
                await self._abort_multipart(object_name, upload_id)
                await self._execute('PutObject', {'Bucket': self.bucket_name, 'Key': object_name, 'Body': b''})
                return True

            await self._execute('CompleteMultipartUpload', {
                'Bucket': self.bucket_name, 'Key': object_name,
                'UploadId': upload_id, 'MultipartUpload': {'Parts': parts}
            })
            return True

        except asyncio.CancelledError:
            # Перехват отмены запроса от AbortController на клиенте
            await self._abort_multipart(object_name, upload_id)
            raise
        except (ClientError, BotoCoreError):
            await self._abort_multipart(object_name, upload_id)
            return False
        except Exception:
            await self._abort_multipart(object_name, upload_id)
            return False

    async def upload_file_from_disk(self, local_path: str, object_name: str, chunk_size: int = 5 * 1024 * 1024) -> bool:
        if not os.path.exists(local_path):
            return False

        loop = asyncio.get_running_loop()
        upload_id = None

        try:
            create_kwargs = {'Bucket': self.bucket_name, 'Key': object_name}
            mp_init = await self._execute('CreateMultipartUpload', create_kwargs)
            upload_id = mp_init['UploadId']

            parts, part_number = [], 1

            with open(local_path, 'rb') as local_file:
                while True:
                    chunk = await loop.run_in_executor(
                        self._executor,
                        _read_file_chunk,
                        local_file,
                        chunk_size
                    )
                    if not chunk:
                        break

                    part_response = await self._execute('UploadPart', {
                        'Bucket': self.bucket_name, 'Key': object_name,
                        'UploadId': upload_id, 'PartNumber': part_number, 'Body': chunk
                    })
                    parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                    part_number += 1

            if not parts:
                await self._abort_multipart(object_name, upload_id)
                await self._execute('PutObject', {'Bucket': self.bucket_name, 'Key': object_name, 'Body': b''})
                return True

            await self._execute('CompleteMultipartUpload', {
                'Bucket': self.bucket_name, 'Key': object_name,
                'UploadId': upload_id, 'MultipartUpload': {'Parts': parts}
            })
            return True

        except asyncio.CancelledError:
            await self._abort_multipart(object_name, upload_id)
            raise
        except (ClientError, BotoCoreError, OSError, Exception):
            await self._abort_multipart(object_name, upload_id)
            return False

    async def upload_django_file(self, django_file, object_name: str, chunk_size: int = 5 * 1024 * 1024) -> bool:
        loop = asyncio.get_running_loop()
        upload_id = None
        try:
            mp_init = await self._execute('CreateMultipartUpload', {'Bucket': self.bucket_name, 'Key': object_name})
            upload_id = mp_init['UploadId']
            parts, part_number, buffer = [], 1, bytearray()

            django_chunks = await loop.run_in_executor(self._executor, list, django_file.chunks())

            for chunk in django_chunks:
                buffer.extend(chunk)
                if len(buffer) >= chunk_size:
                    part_response = await self._execute('UploadPart', {
                        'Bucket': self.bucket_name, 'Key': object_name,
                        'UploadId': upload_id, 'PartNumber': part_number, 'Body': bytes(buffer)
                    })
                    parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                    part_number += 1
                    buffer.clear()

            if buffer:
                part_response = await self._execute('UploadPart', {
                    'Bucket': self.bucket_name, 'Key': object_name,
                    'UploadId': upload_id, 'PartNumber': part_number, 'Body': bytes(buffer)
                })
                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})

            if not parts:
                await self._abort_multipart(object_name, upload_id)
                await self._execute('PutObject', {'Bucket': self.bucket_name, 'Key': object_name, 'Body': b''})
                return True

            await self._execute('CompleteMultipartUpload', {
                'Bucket': self.bucket_name, 'Key': object_name,
                'UploadId': upload_id, 'MultipartUpload': {'Parts': parts}
            })
            return True

        except asyncio.CancelledError:
            await self._abort_multipart(object_name, upload_id)
            raise
        except Exception:
            await self._abort_multipart(object_name, upload_id)
            return False

    async def _abort_multipart(self, object_name: str, upload_id: str | None):
        if not upload_id:
            return
        try:
            await self._execute('AbortMultipartUpload', {
                'Bucket': self.bucket_name, 'Key': object_name, 'UploadId': upload_id
            })
        except Exception:
            pass

    async def download_file(self, object_name: str) -> dict | None:
        try:
            return await self._execute('GetObject', {'Bucket': self.bucket_name, 'Key': object_name})
        except ClientError as err_boto_client_exception:
            error_code = err_boto_client_exception.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise FileNotFoundError(f"Файл {object_name} не найден на сервере")
            elif error_code == 'AccessDenied':
                raise PermissionError("Нет прав для чтения этого файла")
            else:
                raise
        except BotoCoreError:
            raise ConnectionError("Сервер хранения временно недоступен")

    async def download_file_to_disk(self, object_name: str, local_path) -> bool:
        loop = asyncio.get_running_loop()
        try:
            with open(local_path, 'wb') as local_file:
                async for chunk in self.stream_file_chunks(object_name):
                    await loop.run_in_executor(
                        self._executor,
                        _write_chunk_to_disk,
                        local_file,
                        chunk
                    )
            return True
        except (ClientError, BotoCoreError, OSError, Exception):
            _cleanup_failed_file(local_path)
            return False

    async def delete_file(self, object_name: str) -> dict | None:
        try:
            return await self._execute('DeleteObject', {'Bucket': self.bucket_name, 'Key': object_name})
        except ClientError as err_boto_client_exception:
            error_code = err_boto_client_exception.response['Error']['Code']
            if error_code == 'NoSuchKey':
                pass
            elif error_code == 'AccessDenied':
                raise PermissionError("Нет прав для удаления этого файла")
            else:
                raise
        except BotoCoreError:
            raise ConnectionError("Сервер хранения временно недоступен")

    async def delete_multiple_files(self, object_names: list[str]) -> dict:
        try:
            if not object_names:
                return {}
            kwargs = {
                'Bucket': self.bucket_name,
                'Delete': {
                    'Objects': [{'Key': name} for name in object_names],
                    'Quiet': True
                }
            }
            return await self._execute('DeleteObjects', kwargs)
        except ClientError as err_boto_client_exception:
            error_code = err_boto_client_exception.response['Error']['Code']
            if error_code == 'AccessDenied':
                raise PermissionError("Нет прав для удаления файлов")
            else:
                raise
        except BotoCoreError:
            raise ConnectionError("Сервер хранения временно недоступен")

    async def list_files_page(self, prefix: str = "", max_keys: int = 100,
                              continuation_token: str | None = None) -> dict:
        kwargs = {'Bucket': self.bucket_name, 'Prefix': prefix, 'MaxKeys': int(max_keys)}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        try:
            response = await self._execute('ListObjectsV2', kwargs)
            return {
                'files': [
                    {
                        'key': item['Key'],
                        'size': item['Size'],
                        'last_modified': item['LastModified'].isoformat()
                    }
                    for item in response.get('Contents', [])
                ],
                'next_token': response.get('NextContinuationToken', None)
            }
        except Exception:
            return {'files': [], 'next_token': None}

    async def list_all_files_generator(self, prefix: str = "", chunk_size: int = 1000):
        continuation_token = None
        while True:
            page_data = await self.list_files_page(
                prefix=prefix,
                max_keys=chunk_size,
                continuation_token=continuation_token
            )
            for file_info in page_data['files']:
                yield file_info

            continuation_token = page_data['next_token']
            if not continuation_token:
                break

    async def list_directory_and_files(self, prefix: str = "", max_keys: int = 100,
                                       continuation_token: str | None = None) -> dict:
        if prefix and not prefix.endswith('/'):
            prefix = f"{prefix}/"

        kwargs = {
            'Bucket': self.bucket_name, 'Prefix': prefix,
            'Delimiter': '/', 'MaxKeys': int(max_keys)
        }
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        try:
            response = await self._execute('ListObjectsV2', kwargs)

            folders = [
                {'name': p['Prefix'][len(prefix):].rstrip('/'), 'full_path': p['Prefix']}
                for p in response.get('CommonPrefixes', [])
            ]

            files = [
                {
                    'name': item['Key'][len(prefix):],
                    'full_path': item['Key'],
                    'size': item['Size'],
                    'last_modified': item['LastModified'].isoformat()
                }
                for item in response.get('Contents', []) if item['Key'] != prefix
            ]

            return {
                'folders': folders,
                'files': files,
                'next_token': response.get('NextContinuationToken', None)
            }
        except Exception:
            return {'folders': [], 'files': [], 'next_token': None}

    async def get_file_size(self, object_name: str) -> int:
        """
        Получает размер файла в байтах без его скачивания (читает только заголовки).
        """
        try:
            kwargs = {
                'Bucket': self.bucket_name,
                'Key': object_name
            }
            # HeadObject возвращает метаданные, включая ContentLength
            response = await self._execute('HeadObject', kwargs)
            return response.get('ContentLength', 0)

        except ClientError as err_boto_client_exception:
            error_code = err_boto_client_exception.response['Error']['Code']
            # HeadObject специфичен: если файла нет, он возвращает '404', а не 'NoSuchKey'
            if error_code == '404':
                raise FileNotFoundError(f"Файл '{object_name}' не найден на сервере")
            elif error_code == '403':
                raise PermissionError("Нет прав для чтения метаданных этого файла")
            else:
                raise
        except BotoCoreError:
            raise ConnectionError("Сервер хранения временно недоступен")

    async def copy_file(self, source_key: str, destination_key: str) -> bool:
        """
        Асинхронное копирование файла внутри S3.
        Защищено хард-лимитом S3 на копирование файлов более 5 ГБ.
        """
        # 5 Гигабайт в байтах
        LIMIT_5GB = 5 * 1024 * 1024 * 1024

        try:
            # 1. Запрашиваем размер файла
            file_size = await self.get_file_size(source_key)

            # 2. Проверяем лимит
            if file_size > LIMIT_5GB:
                # Здесь можно залогировать или выбросить специфичное исключение,
                # чтобы вызывающий код (например, Django View) мог отдать красивую ошибку юзеру.
                print(f"[Copy Error] Файл '{source_key}' весит {file_size} байт. "
                      f"Это превышает лимит CopyObject в 5 ГБ.")
                # raise ValueError("Размер файла превышает лимит в 5 ГБ. Требуется Multipart Copy.")
                return False

            # 3. Если всё в порядке, выполняем копирование
            kwargs = {
                'Bucket': self.bucket_name,
                'Key': destination_key,
                'CopySource': f"{self.bucket_name}/{source_key}"
            }
            await self._execute('CopyObject', kwargs)
            return True

        except FileNotFoundError:
            # print(f"Невозможно скопировать: исходный файл {source_key} не существует")
            return False
        except Exception as e:
            # print(f"Ошибка копирования файла {source_key}: {e}")
            return False

    async def move_file(self, source_key: str, destination_key: str) -> bool:
        """Перемещение (переименование) файла = Копирование + Удаление."""
        # 1. Сначала копируем
        copied = await self.copy_file(source_key, destination_key)
        if not copied:
            return False

        # 2. Если скопировано успешно, удаляем оригинал
        try:
            await self.delete_file(source_key)
            return True
        except Exception:
            # Если оригинал не удалился, по-хорошему нужно откатить копию,
            # но в базовом варианте можно просто вернуть False
            return False

    async def delete_directory(self, prefix: str) -> bool:
        """
        Удаление виртуальной папки (всех объектов с заданным префиксом).
        """
        # Убедимся, что префикс заканчивается на '/', чтобы при удалении папки 'doc'
        # случайно не удалить папку 'documents'
        if not prefix.endswith('/'):
            prefix += '/'

        keys_to_delete = []
        try:
            # Используем твой генератор для ленивого получения списка файлов
            async for file_info in self.list_all_files_generator(prefix=prefix):
                keys_to_delete.append(file_info['key'])

                # Как только накопилось 1000 ключей, отправляем запрос на массовое удаление
                if len(keys_to_delete) >= 1000:
                    await self.delete_multiple_files(keys_to_delete)
                    keys_to_delete.clear()  # Очищаем список для следующей пачки

            # Удаляем остатки (если файлов было меньше 1000 или остался хвост)
            if keys_to_delete:
                await self.delete_multiple_files(keys_to_delete)

            return True
        except Exception as e:
            # print(f"Ошибка удаления каталога {prefix}: {e}")
            return False

    async def copy_directory(self, source_prefix: str, destination_prefix: str, concurrency: int = 10) -> bool:
        """
        Копирование содержимого одной папки в другую.
        :param concurrency: Сколько файлов копировать одновременно.
        """
        if not source_prefix.endswith('/'):
            source_prefix += '/'
        if not destination_prefix.endswith('/'):
            destination_prefix += '/'

        try:
            tasks = []
            async for file_info in self.list_all_files_generator(prefix=source_prefix):
                source_key = file_info['key']

                # Заменяем старый префикс на новый.
                # Было: 'old_folder/images/1.jpg' -> Стало: 'new_folder/images/1.jpg'
                destination_key = source_key.replace(source_prefix, destination_prefix, 1)

                # Создаем задачу на копирование
                tasks.append(self.copy_file(source_key, destination_key))

                # Если накопилось задач = concurrency, выполняем их параллельно
                if len(tasks) >= concurrency:
                    await asyncio.gather(*tasks)
                    tasks.clear()

            # Добиваем оставшиеся задачи
            if tasks:
                await asyncio.gather(*tasks)

            return True
        except Exception as e:
            # print(f"Ошибка копирования каталога: {e}")
            return False

    async def move_directory(self, source_prefix: str, destination_prefix: str) -> bool:
        """
        Перемещение (переименование) виртуальной папки.
        Это самая ресурсоемкая операция: копируем всё, затем удаляем старое.
        """
        # 1. Полностью копируем каталог в новое место
        copied = await self.copy_directory(source_prefix, destination_prefix)

        # 2. Если копирование прошло успешно, безжалостно удаляем старый каталог
        if copied:
            await self.delete_directory(source_prefix)
            return True

        return False

    @classmethod
    def shutdown(cls):
        """Вызывается при остановке сервера для корректного закрытия пула потоков."""
        cls._executor.shutdown(wait=True)
