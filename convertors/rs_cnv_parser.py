"""
    2026 Жуков Игорь. All rights reserved. Confidential and proprietary. Do not copy or distribute.
    Версия 2.0.0
"""
class XmlParser:
    __slots__ = ()

    @staticmethod
    def get_find(node, path=None):
        return node.find(path) if node is not None else None

    @staticmethod
    def get_find_all(node, path=None):
        if node is None or path is None:
            return []
        return node.findall(path)

    @staticmethod
    def get_attrib(node, path=None, default=None):
        if default is None:
            default = {}
        if node is None:
            return default

        found = node if path is None else node.find(path)
        if found is None:
            return default

        attrib = found.attrib
        if not attrib:
            return {}

        return {k.split('}')[-1]: v for k, v in attrib.items()}

    @staticmethod
    def get_text(node, path=None, default=''):
        if node is None:
            return default

        found = node if path is None else node.find(path)
        if found is None:
            return default

        text = found.text
        return text.strip() if text else default

    @staticmethod
    def del_strip_ns(tag: str) -> str:
        """Убирает XML namespace из имени тега: '{urn:cbr-ru:ed:v2.0}ED807' -> 'ED807'"""
        return tag.split('}')[-1] if '}' in tag else tag