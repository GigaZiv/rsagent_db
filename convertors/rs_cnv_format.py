"""
    Универсальный парсер
    Версия 2.0.0
"""
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List

from utils.rs_logger import get_logger

logger = get_logger("RSConvertor")


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


parse = XmlParser()


def process_folder(folder_path) -> List[Dict[str, Any]]:
    """"""
    results = []

    path = Path(folder_path)
    for file in path.glob('*.xml'):
        out_dict = parse_rosreestr_xml(str(file))
        if out_dict:
            results.append(out_dict)
    return results


def parse_rosreestr_xml(file_path: str, tag: str) -> dict[str, Any]:
    """
    Универсальный парсер Росреестра для Docker-агента.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

    except StopIteration as e:
        logger.error("Ошибка: XML-файл абсолютно пустой")
        return {'error': str(e)}

    except (ET.ParseError, UnicodeDecodeError) as e:
        print(f"Ошибка синтаксиса или кодировки XML: {e}")
        return {'error': str(e)}

    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}")
        return {'error': str(e)}

    attr_root = parse.get_attrib(root)
    item = {
        'extract_base_type': tag,
        'guid': attr_root.get('guid'),
        'qr': attr_root.get('qr'),
        'recipient_statement': parse.get_text(root, "./recipient_statement"),
        'status': parse.get_text(root, "./status"),
        'date_formation': parse.get_text(root, ".//date_formation"),
        'date_received_request':
            parse.get_text(root, ".//details_request/date_received_request"),
        'date_receipt_request_reg_authority_rights':
            parse.get_text(root, ".//details_request/date_receipt_request_reg_authority_rights"),
        'organ_registr_rights': parse.get_text(root, ".//group_top_requisites/organ_registr_rights"),
        'registration_number': parse.get_text(root, ".//group_top_requisites/registration_number"),
    }

    land_record_raw = []
    for record in parse.get_find_all(root, './/land_record'):
        land_record = {'cad_number': parse.get_text(record, ".//common_data/cad_number"),
                       'quarter_cad_number': parse.get_text(record, ".//common_data/quarter_cad_number"),
                       'type_code': parse.get_text(record, ".//common_data/type/code") or "0",
                       'type_value': parse.get_text(record, ".//common_data/type/value") or "Объект недвижимости",
                       'sub_type_code': parse.get_text(record, ".//common_data/subtype/code") or "0",
                       'sub_type_value': parse.get_text(record, ".//common_data/subtype/value") or "-",
                       'reg_date_by_doc': parse.get_text(record, ".//object/reg_date_by_doc") or "-",
                       'address_location': {
                           'address': parse.get_text(record, ".//address/readable_address")
                                      or parse.get_text(record, ".//address_location/address/readable_address"),
                           'address_fias': {
                               'level_settlement': {
                                   'okato': parse.get_text(record, './/address_fias/level_settlement/okato'),
                                   'kladr': parse.get_text(record, './/address_fias/level_settlement/kladr')
                               },
                               'region': {
                                   'code': parse.get_text(record, './/address_fias/level_settlement/region/code'),
                                   'value': parse.get_text(record, './/address_fias/level_settlement/region/value')
                               },
                               'city': {
                                   'type_city': parse.get_text(record,
                                                               './/address_fias/level_settlement/city/type_city'),
                                   'name_city': parse.get_text(record,
                                                               './/address_fias/level_settlement/city/name_city')
                               },
                               'cost': parse.get_text(record, ".//cost/value"),
                           }
                       }}

        cad_links = parse.get_find(record, ".//cad_links")
        if cad_links is not None:
            land_record['related_cad_numbers'] = (
                list(set([n.text for n in parse.get_find_all(cad_links, ".//cad_number") if n.text])))

        params = parse.get_find(record, ".//params")
        if params is not None:
            land_record |= {
                'params': {
                    'category_code': parse.get_text(params, './category/type/code'),
                    'category_value': parse.get_text(params, './category/type/value'),
                    'permitted_use': parse.get_text(params, './permitted_use/permitted_use_established/by_document'),
                }
            }

        polygons_raw = []
        contour_nodes = parse.get_find_all(record, ".//contours/contour")
        for contour in contour_nodes:
            land_record['sk_id'] = parse.get_text(contour, ".//entity_spatial/sk_id")

            spatial_elements = parse.get_find_all(contour, ".//spatial_element") or \
                               parse.get_find_all(contour, ".//entity_spatial/spatials_elements/spatial_element") or \
                               ([contour] if parse.get_find(contour, ".//ordinate") is not None else [])

            for s_elem in spatial_elements:
                points = []
                for ord_node in s_elem.findall(".//ordinate"):
                    y_v = parse.get_text(ord_node, "y").replace(',', '.')
                    x_v = parse.get_text(ord_node, "x").replace(',', '.')
                    if x_v and y_v:
                        # Порядок Y X для SQL PG
                        points.append(f"{y_v} {x_v}")

                if len(points) >= 3:
                    if points != points[-1]: points.append(points[0])  # Замыкаем
                    polygons_raw.append(f"(({', '.join(points)}))")

            if polygons_raw:
                land_record['msk_meters'] = f"MULTIPOLYGON({', '.join(polygons_raw)})"

        land_record_raw.append(land_record)
    if land_record_raw:
        item |= {'land_record': land_record_raw}

    build_record_raw = []
    for record in parse.get_find_all(root, './/build_record'):
        build_record = {'cad_number': parse.get_text(record, ".//common_data/cad_number"),
                        'quarter_cad_number': parse.get_text(record, ".//common_data/quarter_cad_number"),
                        'type_code': parse.get_text(record, ".//common_data/type/code") or "0",
                        'type_value': parse.get_text(record, ".//common_data/type/value") or "Объект недвижимости",
                        'sub_type_code': parse.get_text(record, ".//common_data/subtype/code") or "0",
                        'sub_type_value': parse.get_text(record, ".//common_data/subtype/value") or "-",
                        'reg_date_by_doc': parse.get_text(record, ".//object/reg_date_by_doc") or "-",
                        'address_location': {
                            'address': parse.get_text(record, ".//address/readable_address")
                                       or parse.get_text(record, ".//address_location/address/readable_address"),
                            'address_fias': {
                                'level_settlement': {
                                    'okato': parse.get_text(record, './/address_fias/level_settlement/okato'),
                                    'kladr': parse.get_text(record, './/address_fias/level_settlement/kladr')
                                },
                                'region': {
                                    'code': parse.get_text(record, './/address_fias/level_settlement/region/code'),
                                    'value': parse.get_text(record, './/address_fias/level_settlement/region/value')
                                },
                                'city': {
                                    'type_city': parse.get_text(record,
                                                                './/address_fias/level_settlement/city/type_city'),
                                    'name_city': parse.get_text(record,
                                                                './/address_fias/level_settlement/city/name_city')
                                },
                                'cost': parse.get_text(record, ".//cost/value"),
                            }
                        }}

        cad_links = parse.get_find(record, ".//cad_links")
        if cad_links is not None:
            build_record['related_cad_numbers'] = (
                list(set([n.text for n in parse.get_find_all(cad_links, ".//cad_number") if n.text])))

        params = parse.get_find(record, ".//params")
        if params is not None:
            build_record |= {
                'params': {
                    'params_area': parse.get_text(params, 'area'),
                    'params_floors': parse.get_text(params, 'floors')
                },
                'year_built': parse.get_text(params, 'year_built')
            }

        polygons_raw = []
        contour_nodes = parse.get_find_all(record, ".//contours/contour")
        for contour in contour_nodes:
            build_record['sk_id'] = parse.get_text(contour, ".//entity_spatial/sk_id")

            spatial_elements = parse.get_find_all(contour, ".//spatial_element") or \
                               parse.get_find_all(contour, ".//entity_spatial/spatials_elements/spatial_element") or \
                               ([contour] if parse.get_find(contour, ".//ordinate") is not None else [])

            for s_elem in spatial_elements:
                points = []
                for ord_node in s_elem.findall(".//ordinate"):
                    y_v = parse.get_text(ord_node, "y").replace(',', '.')
                    x_v = parse.get_text(ord_node, "x").replace(',', '.')
                    if x_v and y_v:
                        # Порядок Y X для SQL PG
                        points.append(f"{y_v} {x_v}")

                if len(points) >= 3:
                    if points != points[-1]: points.append(points[0])  # Замыкаем
                    polygons_raw.append(f"(({', '.join(points)}))")

            if polygons_raw:
                build_record['msk_meters'] = f"MULTIPOLYGON({', '.join(polygons_raw)})"

        build_record_raw.append(build_record)
    if build_record_raw:
        item |= {'build_record': build_record_raw}

    room_record_raw = []
    for room in parse.get_find_all(root, './/room_record'):
        room_record = {
            'cad_number': parse.get_text(room, 'object/common_data/cad_number')
        }
        room_record_raw.append(room_record)
    if room_record_raw:
        item |= {'room_records': room_record_raw}

    right_record_raw = []
    for right in parse.get_find_all(root, ".//right_records/right_record"):
        right_info = {
            'registration_date': parse.get_text(right, ".//record_info/registration_date"),
            'right_type_code': parse.get_text(right, ".//right_data/right_type/code"),
            'right_type_value': parse.get_text(right, ".//right_data/right_type/value"),
            'right_number': parse.get_text(right, ".//right_data/right_number"),
            'right_holders': [],
            'underlying_documents': []
        }
        for holder in parse.get_find_all(right, ".//right_holders/right_holder"):
            surname, name, patronymic = (parse.get_text(holder, ".//surname"), parse.get_text(holder, ".//name"),
                                         parse.get_text(holder, ".//patronymic"))
            full_name = f"{surname} {name} {patronymic}".strip()
            if not full_name:
                full_name = parse.get_text(holder, ".//legal_entity/entity_common_data/name") or \
                            parse.get_text(holder, ".//public_formation/public_formation_type/value")

            right_info['right_holders'].append({
                'name': full_name or "Сведения ограничены (266-ФЗ)",
                'inn': parse.get_text(holder, ".//inn"),
                'birth_date': parse.get_text(holder, ".//birth_date"),
                'citizenship': {
                    'code': parse.get_text(holder, ".//citizenship_country/code"),
                    'value': parse.get_text(holder, ".//citizenship_country/value"),
                    'snils': parse.get_text(holder, ".//individual/snils"),
                },
                'identity_doc': {
                    'document_code': {
                        'code': parse.get_text(holder, ".//document_code/code"),
                        'value': parse.get_text(holder, ".//document_code/value"),
                    },
                    'name': parse.get_text(holder, ".//identity_doc/document_name"),
                    'series': parse.get_text(holder, ".//identity_doc/document_series"),
                    'number': parse.get_text(holder, ".//identity_doc/document_number"),
                    'date': parse.get_text(holder, ".//identity_doc/document_date"),
                    'issuer': parse.get_text(holder, ".//identity_doc/document_issuer")
                },
                'mailing_addess': parse.get_text(holder, ".//contacts/mailing_addess"),
                'share': parse.get_text(right, ".//right_data/shares/share/value_text")
            })

        for underlying_document in parse.get_find_all(root, ".//underlying_documents/underlying_document"):
            right_info['underlying_documents'].append({
                'document_name': parse.get_text(underlying_document, './/document_name'),
                'document_number': parse.get_text(underlying_document, './/document_number'),
                'document_date': parse.get_text(underlying_document, './/document_date'),
                'document_code': parse.get_text(underlying_document, './/document_code/code'),
                'document_value': parse.get_text(underlying_document, './/document_code/value')
            })

        right_record_raw.append(right_info)
    if right_record_raw:
        item |= {'right_record': right_record_raw}

    return item
