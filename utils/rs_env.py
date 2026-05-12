"""
Модуль для работы с переменными окружения и настройками приложения.

Предоставляет класс SettingObject, реализующий паттерн Singleton
для доступа к переменным окружения из файла .env.

Версия 1.02
"""
import os
import re
import logging
from typing import Any, Dict, Optional, TypeVar, Type, cast

# Настройка логирования
logger = logging.getLogger(__name__)
T = TypeVar('T')

DEFAULT_ENCODING: str = 'UTF-8'

class SettingObject:
    """
    Класс для работы с настройками из файла .env

    Реализует паттерн Singleton для обеспечения единого доступа
    к настройкам приложения из любой части кода.

    Примеры использования:

    # Загрузка переменных окружения
    settings = SettingObject()
    settings.load_env_file('.env.local')

    # Доступ к настройкам
    debug_mode = settings['DEBUG']
    database_url = settings.get('DATABASE_URL', 'sqlite:///db.sqlite3')

    # Получение типизированных значений
    port = settings.get_int('PORT', 8000)
    is_enabled = settings.get_bool('FEATURE_ENABLED', False)
    """
    __slots__ = ('config',)
    _instance = None

    def __new__(cls):
        """Реализация паттерна Singleton"""
        if not hasattr(cls, '_instance') or cls._instance is None:
            cls._instance = super(SettingObject, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Инициализация объекта настроек"""
        if not hasattr(self, 'config'):
            self.config: Dict[str, str] = {}
            # Загрузка переменных окружения по умолчанию, если необходимо
            # self.load_env_file()

    def __contains__(self, key: str) -> bool:
        """Проверка наличия ключа в настройках"""
        return key in self.config

    def __getitem__(self, key: str) -> str:
        """Получение значения по ключу"""
        if key not in self.config:
            raise KeyError(f"Настройка '{key}' не найдена")
        return self.config[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Получение значения с возможностью указания значения по умолчанию"""
        return self.config.get(key, default)

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Получение булевого значения из настроек"""
        if key not in self.config:
            return default
        return self.str_to_bool(self.config[key]) or default

    def get_int(self, key: str, default: int = 0) -> int:
        """Получение целочисленного значения из настроек"""
        if key not in self.config:
            return default
        try:
            return int(self.config[key])
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в int, используется значение по умолчанию")
            return default

    def get_dict(self, key: str, default=None) -> dict[str, Any]:
        """Получение словаря из настроек"""
        if default is None:
            default = {}
        if key not in self.config:
            return default
        try:
            return dict([v.split('=', 1) for v in self.config[key].split(',') if v])
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в dict, используется значение по умолчанию")
            return default

    def get_tuple(self, key: str, default=None) -> tuple:
        """Получение словаря из настроек"""
        if default is None:
            default = ()
        if key not in self.config:
            return default
        try:
            val = self.config[key].strip('(').strip(')').split(',')
            return tuple([x for x in val if x])
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в tuple, используется значение по умолчанию")
            return default

    def get_byte(self, key: str, default=None) -> bytes:
        """Получение бинари из настроек"""
        if default is None:
            default = bytearray()
        if key not in self.config:
            return default
        try:
            return self.config[key].encode(DEFAULT_ENCODING)
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в bytes, используется значение по умолчанию")
            return default

    def get_str(self, key: str, default=None, multiline=False) -> str:
        """Получение строки из настроек"""
        if default is None:
            default = ''
        if key not in self.config:
            return default
        try:
            value = str(self.config[key])
            if multiline:
                return re.sub(r'(\\r)?\\n', r'\n', value)
            return value
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в string, используется значение по умолчанию")
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Получение числа с плавающей точкой из настроек"""
        if key not in self.config:
            return default
        try:
            float_str = re.sub(r'[^\d,.-]', '', self.config[key])
            parts = re.split(r'[,.]', float_str)
            if len(parts) == 1:
                float_str = parts[0]
            else:
                float_str = f"{''.join(parts[0:-1])}.{parts[-1]}"
            return float(float_str)
        except ValueError:
            logger.warning(f"Не удалось преобразовать '{key}' в float, используется значение по умолчанию")
            return default

    def get_typed(self, key: str, type_cls: Type[T], default: T = None) -> T:
        """Получение значения с преобразованием к указанному типу"""
        if key not in self.config:
            return default
        try:
            return cast(T, type_cls(self.config[key]))
        except (ValueError, TypeError):
            logger.warning(f"Не удалось преобразовать '{key}' к типу {type_cls.__name__}")
            return default

    @staticmethod
    def str_to_bool(value: Any) -> Optional[bool]:
        """
        Преобразование строкового значения в булево

        Поддерживает значения:
        - True: 'true', 't', 'yes', 'y', 'on', '1'
        - False: 'false', 'f', 'no', 'n', 'off', '0'

        Возвращает None, если значение не может быть преобразовано
        """
        if isinstance(value, bool):
            return value

        if not value:
            return None

        if isinstance(value, str):
            value = value.lower().strip()
            if value in ('true', 't', 'yes', 'y', 'on', '1'):
                return True
            if value in ('false', 'f', 'no', 'n', 'off', '0'):
                return False

        return None

    def __call__(self, *args, **kwargs) -> Dict[str, str]:
        """
        При вызове объекта как функции возвращает словарь настроек

        Пример: settings() -> Dict[str, str]
        """
        return self.config.copy()

    def load_env_file(self, path: str = '.env') -> None:
        """
        Загрузка переменных окружения из файла

        Args:
            path: Путь к файлу с переменными окружения
                 По умолчанию '.env' в корне проекта
        """
        # Загрузка из системных переменных окружения
        self.config.update(os.environ)

        # Загрузка из файла, если он существует
        if os.path.isfile(path):
            try:
                with open(path, 'r', encoding=DEFAULT_ENCODING) as file:
                    for line in file:
                        line = line.strip()

                        if not line or line.startswith('#'):
                            continue

                        match = re.match(r'^\s*([A-Za-z0-9_]+)\s*=\s*(.*?)\s*$', line)
                        if match:
                            key, value = match.groups()
                            value = value.strip('\'"')
                            self.config[key] = value

                    os.environ.update({key: value for key, value in self.config.items()})

            except (IOError, OSError) as e:
                logger.warning(f"Ошибка при чтении файла настроек {path}: {e}")
        else:
            logger.info(f"Файл настроек {path} не найден, используются только системные переменные окружения")


# Создаем глобальный экземпляр для удобного импорта
rs_settings = SettingObject()
