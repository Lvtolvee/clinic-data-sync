import os
import json
import argparse
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

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
from app.export.csv_exporter import export_patients_to_csv
from app.export.csv_exporter import export_personal_data_to_csv

#Настройки
load_non_secret_env()
settings = Settings()
setup_logging(
    level=settings.LOG_LEVEL,
    log_file=settings.LOG_FILE,
    audit_log_file=settings.AUDIT_LOG_FILE,
)
log = get_logger(__name__)

DATA_FILE = Path("known_patients.json")
PDF_DIR = Path("output") / "reports"
PDF_DIR.mkdir(parents=True, exist_ok=True)


def calculate_patient_hash(patient_data: dict) -> str:
    # считаем хеш по основным данным, чтобы понять, изменился ли пациент
    info = patient_data.get("info", {})

    def serialize_value(value):
        if value is None:
            return None
        elif isinstance(value, (datetime, date)):
            return str(value)
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)

    key_fields = {
        "LASTNAME": serialize_value(info.get("LASTNAME")),
        "FIRSTNAME": serialize_value(info.get("FIRSTNAME")),
        "MIDNAME": serialize_value(info.get("MIDNAME")),
        "BDATE": serialize_value(info.get("BDATE")),
        "FULL_ADDR": serialize_value(info.get("FULL_ADDR")),
        "PHONE1": serialize_value(info.get("PHONE1")),
        "PHONE2": serialize_value(info.get("PHONE2")),
        "PHONE3": serialize_value(info.get("PHONE3")),
        "CLMAIL": serialize_value(info.get("CLMAIL")),
        "AGESTATUS_NAME": serialize_value(info.get("AGESTATUS_NAME")),
        "TYPESTATUS_NAME": serialize_value(info.get("TYPESTATUS_NAME")),
        "visits_count": len(patient_data.get("approved_plans", [])),
        "CONSULT_DOCTOR": serialize_value(info.get("CONSULT_DOCTOR")),
        "FIRST_DOCTOR": serialize_value(info.get("FIRST_DOCTOR")),
        "FIRSTWORKDATE": serialize_value(info.get("FIRSTWORKDATE")),
        "current_stage": serialize_value(patient_data.get("current_stage")),
        "composite_plan_count": len(patient_data.get("composite_plan", [])),
        "complex_plans_count": len(patient_data.get("complex_plans", [])),
        "approved_plans_count": len(patient_data.get("approved_plans", [])),
        "total_sum": serialize_value(info.get("total_sum")),
        "paid_sum": serialize_value(info.get("paid_sum")),
    }

    data_str = json.dumps(key_fields, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()


def load_known_patients():
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return {}
                data = json.loads(content)
                return data
        except json.JSONDecodeError:
            log.warning("Файл known_patients.json повреждён, пересоздаём.")
            return {}
    return {}


def save_known_patients(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def process_patient(conn, pcode, known, target_date):
    # Обновляет PDF, если данные пациента поменялись
    current_data = collect_patient_data(conn, pcode)
    current_hash = calculate_patient_hash(current_data)
    appts = fetch_future_appointments(conn, pcode)
    latest_appt = max([str(a["WORK_DATE_STR"]) for a in appts], default=None)

    patient_info = known.get(pcode, {})
    last_saved_appt = patient_info.get("last_appointment_date")
    last_saved_hash = patient_info.get("data_hash")

    pdf_path = PDF_DIR / f"patient_{pcode}.pdf"

    need_regen = False
    reason = ""

    if not pdf_path.exists():
        need_regen = True
        reason = "PDF отсутствует"
    elif last_saved_hash is None:
        need_regen = True
        reason = "первая проверка данных"
    elif current_hash != last_saved_hash:
        need_regen = True
        reason = "изменились данные пациента"
    elif last_saved_appt is None or (latest_appt and latest_appt > last_saved_appt):
        need_regen = True
        reason = f"новый приём {latest_appt}"

    if need_regen:
        build_patient_report(conn, pcode, str(pdf_path))
        log.info(f"PDF создан/обновлён для {pcode} ({reason}) → {pdf_path}")

        known[pcode] = {
            "last_appointment_date": latest_appt,
            "data_hash": current_hash,
            "last_checked": str(target_date),
            "last_updated": str(date.today()),
        }
    else:
        # просто помечаем, что проверяли
        log.info(f"Пациент {pcode}: изменений нет, PDF не тронут")
        known[pcode]["last_checked"] = str(target_date)


def main(date_range: List[date], filter_pcodes: List[str] = None):
    """
    Основная функция обработки пациентов.
    Теперь:
    - не дублирует новых пациентов при проходе по known;
    - проверяет только тех, у кого last_checked < target_date;
    - ускорена и безопаснее.
    """
    log.info(f"Запуск обработки за диапазон {date_range[0]} → {date_range[-1]}")
    known = load_known_patients()
    all_processed_patients = []

    csv_dir = Path("output") / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    with get_connection(settings) as conn:
        for target_date in date_range:
            log.info(f"\n=== Обработка за {target_date} ===")
            processed_patients = []

            # если указали конкретных пациентов
            if filter_pcodes:
                for pcode in filter_pcodes:
                    info = fetch_main_info(conn, pcode)
                    if not info:
                        log.warning(f"Пациент с PCODE={pcode} не найден в базе")
                        continue
                    if pcode not in known:
                        known[pcode] = {
                            "last_checked": str(target_date),
                            "last_appointment_date": None,
                            "data_hash": None,
                        }
                        log.info(f"Новый пациент (по PCODE): {pcode} {info['LASTNAME']} {info['FIRSTNAME']}")
                    process_patient(conn, pcode, known, target_date)
                    processed_patients.append(pcode)

            # если никого не указали, берём всех новых
            if not filter_pcodes:
                for pcode, pdata in list(known.items()):
                    last_checked_str = pdata.get("last_checked")
                    try:
                        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d").date() if last_checked_str else None
                    except ValueError:
                        last_checked = None

                    if not last_checked or last_checked < target_date:
                        process_patient(conn, pcode, known, target_date)
                        processed_patients.append(pcode)

            # если никого не указали, берём пациентов по дате
            if not filter_pcodes:
                new_patients = fetch_primary_patients_today(conn, target_date)
                for p in new_patients:
                    pcode = str(p["PCODE"])
                    if pcode not in known:
                        known[pcode] = {
                            "last_checked": str(target_date),
                            "last_appointment_date": None,
                            "data_hash": None,
                        }
                        log.info(f"Новый пациент (по дате): {pcode} {p['LASTNAME']} {p['FIRSTNAME']}")
                        process_patient(conn, pcode, known, target_date)
                        processed_patients.append(pcode)
                    else:
                        # если уже известен, но last_checked < текущей даты — обновим
                        last_checked_str = known[pcode].get("last_checked")
                        try:
                            last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d").date() if last_checked_str else None
                        except ValueError:
                            last_checked = None
                        if not last_checked or last_checked < target_date:
                            process_patient(conn, pcode, known, target_date)
                            processed_patients.append(pcode)

            all_processed_patients.extend(processed_patients)

        # после завершения диапазона дат
        if all_processed_patients:
            try:
                log.info(f"Создание итоговых CSV для {len(all_processed_patients)} пациентов...")
                csv_path_med = csv_dir / "processed_patients.csv"
                csv_path_pers = csv_dir / "processed_patients_personal_data.csv"

                export_patients_to_csv(conn, all_processed_patients, csv_path_med)
                export_personal_data_to_csv(conn, all_processed_patients, csv_path_pers)

                log.info("CSV-файлы успешно созданы. Загружаем их в Битрикс...")

                #выбор режима интеграции
                try:
                    if settings.BITRIX_MODE.lower() == "api":
                        from app.export.bitrix_api_loader import main as load_csv_to_bitrix_api
                        log.info("Режим загрузки: Bitrix REST API ")
                        load_csv_to_bitrix_api()
                    else:
                        from app.export.bitrix_loader import load_csv_to_bitrix
                        log.info("Режим загрузки: Selenium ")
                        load_csv_to_bitrix(settings)

                    log.info("Загрузка CSV в Битрикс завершена успешно")
                except Exception as e:
                    log.error(f"Ошибка при загрузке CSV файлов в Bitrix24: {e}")

            except Exception as e:
                log.error(f"Ошибка при создании или загрузке CSV: {e}")
        else:
            log.warning("Нет пациентов для экспорта в CSV")

    save_known_patients(known)
    log.info(f"Обработка диапазона завершена. Всего обработано пациентов: {len(all_processed_patients)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сканирование пациентов и генерация отчетов")
    parser.add_argument("--start-date", help="Начальная дата диапазона (dd.MM.yyyy)")
    parser.add_argument("--end-date", help="Конечная дата диапазона (dd.MM.yyyy)")
    parser.add_argument("--date", help="Одиночная дата (dd.MM.yyyy)")
    parser.add_argument("--pcode", help="Фильтр по конкретному пациенту или пациентам (через запятую)")

    args = parser.parse_args()

    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, "%d.%m.%Y").date()
            end_date = datetime.strptime(args.end_date, "%d.%m.%Y").date()
        except ValueError:
            raise SystemExit("Ошибка: даты должны быть в формате dd.MM.yyyy")

        if start_date > end_date:
            raise SystemExit("Ошибка: начальная дата не может быть позже конечной")

        date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        log.info(f"Обработка диапазона дат: {start_date} → {end_date}")

    elif args.date:
        try:
            single_date = datetime.strptime(args.date, "%d.%m.%Y").date()
            date_range = [single_date]
        except ValueError:
            raise SystemExit("Ошибка: укажи дату в формате dd.MM.yyyy")
    else:
        today = date.today()
        date_range = [today]
        log.info(f"Дата не указана, используется сегодня: {today}")

    # обработка PCODE
    filter_pcodes = []
    if args.pcode:
        pcode_list = [p.strip() for p in args.pcode.split(",")]
        filter_pcodes = [p for p in pcode_list if p]
        if not filter_pcodes:
            raise SystemExit("Ошибка: указаны пустые PCODE")
        log.info(f"Указаны PCODE для обработки: {', '.join(filter_pcodes)}")

    main(date_range, filter_pcodes)
