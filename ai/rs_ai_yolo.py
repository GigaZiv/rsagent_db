"""
    2026 Жуков Игорь. All rights reserved. Confidential and proprietary. Do not copy or distribute.
    Версия 2.0.0
"""
import os
import threading
import time
from pathlib import Path
from typing import Any

os.environ["POLARS_SKIP_CPU_CHECK"] = "1"
os.environ["YOLO_OFFLINE"] = "True"
os.environ["YOLO_VERBOSE"] = "False"

from ultralytics import YOLO

from utils.rs_logger import get_logger

logger = get_logger("RSYoloProcessor")

def resolve_cyrillic_font() -> str:
    """Находит доступный кириллический шрифт"""
    base_dir = Path(__file__).resolve().parent.parent
    candidates = [
        base_dir / "arial.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return path.as_posix()
    return "arial.ttf"

FONT_PATH = resolve_cyrillic_font()


# Настройки камеры DJI Mini
SENSOR_WIDTH_MM = 6.3
FOCAL_LENGTH_MM = 4.49
DEFAULT_GSD = 0.0175  # м/пиксель (1.75 см на пиксель)

# Соответствие версий из task_meta файлам весов
MODEL_MAP = {
    "v8-nano": "ai/models/building_model.pt",
    "yolo11m": "ai/models/yolo11m.pt",
    "yolo11n-seg": "ai/models/yolo11n-seg.pt",
    "yoloe-11m-seg": "ai/models/yoloe-11m-seg.pt",
    "default": "ai/models/building_model.pt",
}

# Кэш загруженных моделей и блокировка для потокобезопасности PyTorch
_MODELS_CACHE: dict[str, YOLO] = {}
_YOLO_LOCK = threading.Lock()


def get_yolo_model(model_version: str) -> YOLO:
    """Потокобезопасно загружает и кэширует модель YOLO."""
    model_path = MODEL_MAP.get(model_version, MODEL_MAP["default"])
    if model_path not in _MODELS_CACHE:
        logger.info(f"Загрузка модели YOLO в память: [{model_path}]...")
        _MODELS_CACHE[model_path] = YOLO(model_path)
    return _MODELS_CACHE[model_path]


def process_image_yolo(
        image_path: str,
        task_meta: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Синхронная функция для запуска в asyncio.to_thread.
    Находит постройки, рисует рамки, сохраняет новый файл и возвращает структуру для JSONField (task_log).
    """
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Исходное изображение не найдено: {image_path}")

    meta = task_meta or {}
    target_folder = meta.get("target-folder") or "processed/results/"
    model_version = meta.get("model-version") or "v8-nano"

    try:
        conf_threshold = float(meta.get("confidence-threshold", 0.45))
    except (ValueError, TypeError):
        conf_threshold = 0.45

    gsd = float(meta.get("gsd", DEFAULT_GSD))

    # Создаем целевую папку для сохранения размеченного фото
    os.makedirs(target_folder, exist_ok=True)
    base_name = os.path.basename(image_path)
    name_part, ext_part = os.path.splitext(base_name)
    output_file_path = os.path.join(target_folder, f"{name_part}_detected{ext_part}")

    start_ts = time.perf_counter()

    # Инференс защищаем Lock'ом, чтобы параллельные задачи не конфликтовали в GPU/CPU памяти PyTorch
    with _YOLO_LOCK:
        model = get_yolo_model(model_version)
        results = model.predict(source=image_path, conf=conf_threshold, verbose=False)

    result = results[0]
    #result.names[0] = 'Постройка'

    # annotated_img = result.plot(
    #     pil=True,
    #     font=FONT_PATH,
    #     line_width=2,
    #     font_size=12
    # )

    # import cv2
    # cv2.imwrite(output_file_path, annotated_img)

    result.save(
        filename=output_file_path,
        pil=True,
        font=FONT_PATH,
        line_width=2,
        font_size=12
    )

    detections: list[dict[str, Any]] = []
    class_counts: dict[str, int] = {}
    total_area_m2 = 0.0

    names = result.names  # Словарь классов модели {0: 'building', ...}

    if result.boxes is not None:
        for idx, box in enumerate(result.boxes, start=1):
            cls_id = int(box.cls[0].item())
            cls_name = names.get(cls_id, f"class_{cls_id}")
            conf = round(float(box.conf[0].item()), 4)

            # Координаты рамки в пикселях: [x1, y1, x2, y2]
            x1, y1, x2, y2 = [round(float(v), 1) for v in box.xyxy[0].tolist()]
            width_px = round(x2 - x1, 1)
            height_px = round(y2 - y1, 1)

            # Расчет реальных габаритов и площади по GSD камеры DJI Mini
            width_m = round(width_px * gsd, 2)
            height_m = round(height_px * gsd, 2)
            area_m2 = round(width_m * height_m, 2)
            total_area_m2 += area_m2

            class_counts[cls_name] = class_counts.get(cls_name, 0) + 1

            detections.append({
                "index": idx,
                "class_id": cls_id,
                "class_name": cls_name,
                "confidence": conf,
                "bbox_px": [x1, y1, x2, y2],
                "size_px": {"width": width_px, "height": height_px},
                "size_m": {"width": width_m, "height": height_m},
                "area_m2": area_m2
            })

    elapsed_ms = round((time.perf_counter() - start_ts) * 1000, 1)
    img_h, img_w = result.orig_shape

    # Готовая структура для записи в БД и в task_log = models.JSONField
    return {
        "status": "SUCCESS",
        "model_version": model_version,
        "confidence_threshold": conf_threshold,
        "inference_time_ms": elapsed_ms,
        "image_info": {
            "source_path": image_path,
            "result_path": output_file_path,
            "width_px": img_w,
            "height_px": img_h,
            "gsd_m_per_px": gsd,
            "camera": {
                "sensor_width_mm": SENSOR_WIDTH_MM,
                "focal_length_mm": FOCAL_LENGTH_MM
            }
        },
        "summary": {
            "total_objects": len(detections),
            "class_counts": class_counts,
            "total_area_m2": round(total_area_m2, 2)
        },
        "detections": detections
    }
