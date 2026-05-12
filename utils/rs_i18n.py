"""
Версия 1.0.0
    Локализация проекта:

1.  Poedit - читать description.txt

Использование:
    Позволяет вызывать объект как функцию: _("текст", lang="en")
    from rs_i18n import _

Использование dockerfile:
    COPY locales/ /rsagent_sms/locales/

"""
import gettext
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
LOCALES_DIR = BASE_DIR / 'locales'

DEFAULT_LANG = os.getenv('AGENT_DEFAULT_LANG', 'ru')

class Translator:

    __slots__ = "_cache"

    def __init__(self):
        self._cache = {}

    def _get_translator(self, lang):
        if lang not in self._cache:
            self._cache[lang] = gettext.translation(
                domain='base',
                localedir=LOCALES_DIR,
                languages=[lang],
                fallback=True
            )
        return self._cache[lang]

    def __call__(self, text: str, lang=None):
        """
        Вызов: _("Текст") -> использует DEFAULT_LANG
        Вызов: _("Текст", lang="en") -> использует "en"
        """
        target_lang = lang or DEFAULT_LANG
        translator = self._get_translator(target_lang)
        return translator.gettext(text)

_ = Translator()
