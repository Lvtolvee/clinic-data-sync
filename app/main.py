
import os
import json
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Set, Optional, Dict, Any
import hashlib as _hashlib
import json as _json

from app.config import load_non_secret_env, Settings
from app.logging import setup_logging, get_logger
from app.db.client import get_connection
from app.db.extract import (
    fetch_primary_patients_today,
    fetch_future_appointments,
    fetch_main_info,
    collect_patient_data,
)
from app.reports.patient_report import build_patient_report
from app.export.csv_exporter import export_patients_to_csv, export_personal_data_to_csv

# -----------------------
# Settings & logging
# -----------------------
load_non_secret_env()
settings = Settings()
setup_logging(
    level=settings.LOG_LEVEL,
    log_file=settings.LOG_FILE,
    audit_log_file=settings.AUDIT_LOG_FILE,
)
log = get_logger(__name__)

DATA_FILE = Path("known_patients.json")
CSV_DIR = Path("output") / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR = Path("output") / "reports"
PDF_DIR.mkdir(parents=True, exist_ok=True)

# Anti-duplicate set (for THIS run only; not persisted)
processed_in_this_run: Set[str] = set()


# -----------------------
# Utils
# -----------------------
_SUPPORTED_DATE_FORMATS = ("%Y-%m-%d", "%d.%m.%Y", "%Y.%m.%d", "%d-%m-%Y")


def parse_date_str(s: str) -> date:
    s = s.strip()
    for fmt in _SUPPORTED_DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(
        f"Нераспознанный формат даты: '{s}'. Поддерживаются: {', '.join(_SUPPORTED_DATE_FORMATS)}"
    )


def load_known_patients() -> Dict[str, Dict[str, Any]]:
    if DATA_FILE.exists():
        try:
            with DATA_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Не удалось прочитать {DATA_FILE}: {e}. Начинаем с пустого known.")
            return {}
    return {}


def save_known_patients(known: Dict[str, Dict[str, Any]]) -> None:
    try:
        with DATA_FILE.open("w", encoding="utf-8") as f:
            json.dump(known, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.exception(f"Ошибка записи {DATA_FILE}: {e}")


def make_sql_cache_key(sql: str, params, target_date: date, *, extra: dict | None = None) -> str:
    key = {
        "sql": sql,
        "params": params,
        "date": target_date.isoformat(),
        "extra": extra or {},
    }
    data = _json.dumps(key, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8")
    return _hashlib.md5(data).hexdigest()


def calculate_patient_hash(patient_data: dict) -> str:
    # Стабильный хеш содержимого отчёта
    payload = _json.dumps(patient_data, ensure_ascii=False, sort_keys=True, default=str)
    return _hashlib.md5(payload.encode("utf-8")).hexdigest()


# -----------------------
# Core
# -----------------------
def process_patient(conn, pcode: str, known: Dict[str, Dict[str, Any]], target_date: date) -> None:
    info = fetch_main_info(conn, pcode)
    if not info:
        log.warning(f"Пациент {pcode} — не найден в базе, пропускаю")
        return
    fio = f"{info.get('LASTNAME', '')} {info.get('FIRSTNAME', '')} {info.get('MIDNAME', '')}".strip()
    log.info(f"Пациент {pcode} {fio} — начали обработку")

    # enrich known entry if new
    entry = known.setdefault(
        pcode,
        {
            "last_checked": target_date.isoformat(),
            "last_appointment_date": None,
            "data_hash": None,
        },
    )

    # future appointments (на дату итерации)
    future_appts = fetch_future_appointments(conn, pcode)
    log.info(f"Пациент {pcode} — записей на будущее: {len(future_appts)}")

    # будущие записи — только чтобы залогировать количество
    future_appts = fetch_future_appointments(conn, pcode)
    log.info(f"Пациент {pcode} — записей на будущее: {len(future_appts)}")

    # собираем данные пациента (без лишних аргументов)
    data = collect_patient_data(conn, pcode)

    # calculate content hash
    new_hash = calculate_patient_hash(data)
    old_hash = entry.get("data_hash")

    if new_hash != old_hash:
        try:
            build_patient_report(data, PDF_DIR)
            log.info(f"Пациент {pcode} — отчёт обновлён")
        except Exception as e:
            log.exception(f"Пациент {pcode} — ошибка при сборке отчёта: {e}")
        entry["data_hash"] = new_hash
    else:
        log.info(f"Пациент {pcode} — изменений нет, отчёт без изменений")

    entry["last_checked"] = target_date.isoformat()
    log.info(f"Пациент {pcode} — обработан")


def main(date_range: List[date], filter_pcodes: Optional[List[str]] = None) -> None:
    known = load_known_patients()
    all_processed_pcodes: List[str] = []

    filter_pcodes = filter_pcodes or []

    with get_connection(settings) as conn:
        for target_date in date_range:
            log.info(f"Запуск за дату: {target_date}")
            processed_today: List[str] = []
            try:
                # 1) если указали явные PCODE
                if filter_pcodes:
                    for pcode in filter_pcodes:
                        if pcode in processed_in_this_run:
                            log.debug(f"Скип {pcode}: уже обработан в этом запуске")
                            continue
                        process_patient(conn, pcode, known, target_date)
                        processed_in_this_run.add(pcode)
                        processed_today.append(pcode)
                # 2) иначе — берём пациентов за день
                else:
                    daily = fetch_primary_patients_today(conn, target_date)
                    count_all = len(daily)
                    log.info(f"Найдено пациентов за день: {count_all}")
                    pcodes_for_day = [str(p["PCODE"]) for p in daily]
                    # убираем тех, кого уже трогали в этом запуске
                    pcodes_for_day = [p for p in pcodes_for_day if p not in processed_in_this_run]
                    if not pcodes_for_day:
                        log.info(f"За {target_date} новых пациентов нет")
                    for pcode in pcodes_for_day:
                        process_patient(conn, pcode, known, target_date)
                        processed_in_this_run.add(pcode)
                        processed_today.append(pcode)

                log.info(f"За {target_date} обработано пациентов: {len(processed_today)}")
                all_processed_pcodes.extend(processed_today)
            finally:
                # сохраняем known ПОСЛЕ КАЖДОГО ДНЯ
                save_known_patients(known)
                log.debug(f"Сохранён known после дня {target_date}")

    # Итоговые CSV по завершении запуска
    if all_processed_pcodes:
        try:
            log.info(f"Создание итоговых CSV для {len(all_processed_pcodes)} пациентов...")
            csv_path_med = CSV_DIR / "processed_patients.csv"
            csv_path_pers = CSV_DIR / "processed_patients_personal_data.csv"
            log.info(f"Сформирован CSV (медицинские): {csv_path_med}")
            log.info(f"Сформирован CSV (персональные): {csv_path_pers}")
            # Для экспорта нужны активные соединения/данные из БД
            with get_connection(settings) as conn:
                export_patients_to_csv(conn, all_processed_pcodes, csv_path_med)
                export_personal_data_to_csv(conn, all_processed_pcodes, csv_path_pers)
            log.info("CSV-файлы успешно созданы. Загружаем их в Битрикс...")

            # Режим интеграции
            try:
                mode = (settings.BITRIX_MODE or "").lower()
                if mode == "api":
                    from app.export.bitrix_api_loader import main as load_csv_to_bitrix_api
                    log.info("Режим загрузки: Bitrix REST API")
                    load_csv_to_bitrix_api(csv_path_med, csv_path_pers, settings)
                elif mode == "selenium":
                    from app.export.bitrix_loader import main as load_csv_to_bitrix_selenium
                    log.info("Режим загрузки: Bitrix Selenium UI")
                    load_csv_to_bitrix_selenium(csv_path_med, csv_path_pers, settings)
                else:
                    log.info("BITRIX_MODE не задан или неизвестен — пропускаю загрузку в Битрикс")
            except Exception as e:
                log.exception(f"Ошибка загрузки CSV в Битрикс: {e}")
        except Exception as e:
            log.exception(f"Ошибка при формировании CSV: {e}")
    else:
        log.info("За запуск не обработано ни одного пациента — CSV не формируем")


# -----------------------
# CLI
# -----------------------
def build_date_range(args) -> List[date]:
    if args.date:
        # одна дата
        d = parse_date_str(args.date)
        log.info(f"Использую дату фильтра: {d.isoformat()}")
        return [d]
    if args.start and args.end:
        start = parse_date_str(args.start)
        end = parse_date_str(args.end)
        if end < start:
            raise SystemExit("END < START")
        days = (end - start).days + 1
        rng = [start + timedelta(days=i) for i in range(days)]
        log.info(f"Диапазон дат: {start.isoformat()} — {end.isoformat()} ({len(rng)} дн.)")
        return rng
    # fallback: сегодня
    today = date.today()
    log.info(f"Дата не указана, используется сегодня: {today.isoformat()}")
    return [today]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Экспорт отчётов по пациентам")
    g = parser.add_argument_group("Дата")
    g.add_argument("--date", help="Одна дата (например, 2025-10-15 или 15.10.2025)")
    g.add_argument("--start", help="Начало диапазона дат")
    g.add_argument("--end", help="Конец диапазона дат")

    parser.add_argument(
        "--pcode",
        help="Список PCODE через запятую (если задан, обрабатываем только их)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    drange = build_date_range(args)

    filter_pcodes: List[str] = []
    if args.pcode:
        filter_pcodes = [p.strip() for p in args.pcode.split(",") if p.strip()]
        if not filter_pcodes:
            raise SystemExit("Ошибка: указаны пустые PCODE")
        log.info(f"Указаны PCODE для обработки: {', '.join(filter_pcodes)}")

    main(drange, filter_pcodes)
