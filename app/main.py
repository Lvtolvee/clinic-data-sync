# app/main.py
from __future__ import annotations

import json
import argparse
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

from app.config import load_non_secret_env, Settings
from app.logging import setup_logging, get_logger, patient_log, stage_log
from app.db.client import get_connection
from app.db.extract import (
    fetch_primary_patients_today,
    fetch_future_appointments,
    fetch_main_info,
    collect_patient_data,
)
from app.reports.patient_report import build_patient_report
from app.export.csv_exporter import export_patients_to_csv, export_personal_data_to_csv

# Инициализация конфигурации и логов
load_non_secret_env()
settings = Settings()
setup_logging(
    level=getattr(settings, "LOG_LEVEL", "INFO"),
    log_file=getattr(settings, "LOG_FILE", "logs/app.log"),
    audit_log_file=getattr(settings, "AUDIT_LOG_FILE", "logs/audit.log"),
)
log = get_logger(__name__)

# Хранилище состояния и директории
DATA_FILE = Path("known_patients.json")
PDF_DIR = Path("output") / "reports"
PDF_DIR.mkdir(parents=True, exist_ok=True)


# ===================== УТИЛИТЫ =====================

def _serialize_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return str(value)
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def calculate_patient_hash(patient_data: dict) -> str:
    """Хешируем только значимые поля, чтобы детектить изменения."""
    info = patient_data.get("info", {})

    key_fields = {
        "LASTNAME": _serialize_value(info.get("LASTNAME")),
        "FIRSTNAME": _serialize_value(info.get("FIRSTNAME")),
        "MIDNAME": _serialize_value(info.get("MIDNAME")),
        "BDATE": _serialize_value(info.get("BDATE")),
        "FULL_ADDR": _serialize_value(info.get("FULL_ADDR")),
        "PHONE1": _serialize_value(info.get("PHONE1")),
        "PHONE2": _serialize_value(info.get("PHONE2")),
        "PHONE3": _serialize_value(info.get("PHONE3")),
        "CLMAIL": _serialize_value(info.get("CLMAIL")),
        "AGESTATUS_NAME": _serialize_value(info.get("AGESTATUS_NAME")),
        "TYPESTATUS_NAME": _serialize_value(info.get("TYPESTATUS_NAME")),
        "CONSULT_DOCTOR": _serialize_value(info.get("CONSULT_DOCTOR")),
        "FIRST_DOCTOR": _serialize_value(info.get("FIRST_DOCTOR")),
        "FIRSTWORKDATE": _serialize_value(info.get("FIRSTWORKDATE")),
        "current_stage": _serialize_value(patient_data.get("current_stage")),
        "composite_plan_count": len(patient_data.get("composite_plan", [])),
        "complex_plans_count": len(patient_data.get("complex_plans", [])),
        "approved_plans_count": len(patient_data.get("approved_plans", [])),
        "total_sum": _serialize_value(info.get("total_sum")),
        "paid_sum": _serialize_value(info.get("paid_sum")),
    }

    data_str = json.dumps(key_fields, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(data_str.encode("utf-8")).hexdigest()


def load_known_patients() -> dict:
    if DATA_FILE.exists():
        try:
            text = DATA_FILE.read_text(encoding="utf-8").strip()
            return json.loads(text) if text else {}
        except json.JSONDecodeError:
            stage_log("Хранилище пациентов", status="повреждено", файл=str(DATA_FILE))
            return {}
    return {}


def save_known_patients(data: dict) -> None:
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ===================== ОСНОВНАЯ ОБРАБОТКА ПАЦИЕНТА =====================

def process_patient(conn, pcode: str, known: dict, target_date: date, is_new: bool = False) -> None:
    """
    Обрабатывает пациента и пишет РОВНО ОДНУ строку patient_log:
      - новый -> статус="внесен", комментарий="новый пациент"
      - изменился -> статус="обновлен", комментарий="генерация отчёта"
      - без изменений -> статус="пропущен", комментарий="без изменений"
      - ошибка -> статус="ошибка", комментарий="не удалось обработать"
    """
    try:
        current_data = collect_patient_data(conn, pcode)
        current_hash = calculate_patient_hash(current_data)
        appts = fetch_future_appointments(conn, pcode)
        latest_appt = max([str(a.get("WORK_DATE_STR", "")) for a in appts], default=None)

        patient_info = known.get(pcode, {})
        last_saved_appt = patient_info.get("last_appointment_date")
        last_saved_hash = patient_info.get("data_hash")

        pdf_path = PDF_DIR / f"patient_{pcode}.pdf"

        # Решаем, надо ли регенерировать PDF
        need_regen = False
        if not pdf_path.exists():
            need_regen = True
        elif last_saved_hash is None:
            need_regen = True
        elif current_hash != last_saved_hash:
            need_regen = True
        elif last_saved_appt is None or (latest_appt and last_saved_appt and latest_appt > last_saved_appt):
            need_regen = True

        if need_regen:
            # генерим PDF и фиксируем новое состояние
            build_patient_report(conn, pcode, str(pdf_path))
            known[pcode] = {
                "last_appointment_date": latest_appt,
                "data_hash": current_hash,
                "last_checked": str(target_date),
                "last_updated": str(date.today()),
                "processed_on": str(target_date),
            }
            # --- ЕДИНСТВЕННАЯ строка на pcode ---
            if is_new:
                patient_log(pcode, status="внесен", comment="новый пациент")
            else:
                patient_log(pcode, status="обновлен", comment="генерация отчёта", pdf=pdf_path.name)
        else:
            # без изменений — только отметим проверку
            known.setdefault(pcode, {})
            known[pcode]["last_checked"] = str(target_date)
            known[pcode]["processed_on"] = str(target_date)
            # --- ЕДИНСТВЕННАЯ строка на pcode ---
            if is_new:
                # теоретически не должно случиться (новым обычно генерим PDF), но оставим на случай отсутствия данных
                patient_log(pcode, status="внесен", comment="новый пациент")
            else:
                patient_log(pcode, status="пропущен", comment="без изменений")

    except Exception as e:
        # Ошибка — тоже одна строка
        known.setdefault(pcode, {})
        known[pcode]["processed_on"] = str(target_date)
        patient_log(pcode, status="ошибка", comment="не удалось обработать", ошибка=str(e))


# ===================== ОБХОД ДИАПАЗОНА ДАТ =====================

def main(date_range: List[date], filter_pcodes: List[str] | None = None) -> None:
    stage_log("Обработка диапазона дат", status="старт", начало=str(date_range[0]), конец=str(date_range[-1]))
    known = load_known_patients()

    # пациенты, обработанные за ВЕСЬ запуск (во всём диапазоне)
    already_processed: set[str] = set()

    csv_dir = Path("output") / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    with get_connection(settings) as conn:
        for target_date in date_range:
            stage_log("Дата", status="обработка", значение=str(target_date))

            # пациенты, обработанные В ЭТУ дату
            processed_today: set[str] = set()
            processed_list: list[str] = []

            def _mark(p: str):
                processed_today.add(p)
                already_processed.add(p)
                processed_list.append(p)
                known.setdefault(p, {})
                known[p]["processed_on"] = str(target_date)

            # --- 1) Ручной фильтр по PCODE ---
            if filter_pcodes:
                for pcode in filter_pcodes:
                    if pcode in processed_today or pcode in already_processed:
                        continue

                    info = fetch_main_info(conn, pcode)
                    if not info:
                        patient_log(pcode, status="пропущен", comment="не найден в БД")
                        _mark(pcode)
                        continue

                    if pcode not in known:
                        known[pcode] = {
                            "last_checked": str(target_date),
                            "last_appointment_date": None,
                            "data_hash": None,
                        }

                    if known.get(pcode, {}).get("processed_on") == str(target_date):
                        patient_log(pcode, status="пропущен", comment="уже обработан сегодня")
                        _mark(pcode)
                        continue

                    process_patient(conn, pcode, known, target_date, is_new=(known[pcode].get("data_hash") is None))
                    _mark(pcode)

            # --- 2) Известные пациенты (если нет ручного фильтра) ---
            if not filter_pcodes:
                for pcode, pdata in list(known.items()):
                    if pcode in processed_today or pcode in already_processed:
                        continue

                    if pdata.get("processed_on") == str(target_date):
                        _mark(pcode)
                        continue

                    last_checked_str = pdata.get("last_checked")
                    try:
                        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d").date() if last_checked_str else None
                    except ValueError:
                        last_checked = None

                    if not last_checked or last_checked < target_date:
                        process_patient(conn, pcode, known, target_date, is_new=(pdata.get("data_hash") is None))
                        _mark(pcode)

            # --- 3) Первичка по текущей дате (если нет ручного фильтра) ---
            if not filter_pcodes:
                new_patients = fetch_primary_patients_today(conn, target_date)
                for p in new_patients:
                    pcode = str(p["PCODE"])
                    if pcode in processed_today or pcode in already_processed:
                        continue

                    if pcode not in known:
                        known[pcode] = {
                            "last_checked": str(target_date),
                            "last_appointment_date": None,
                            "data_hash": None,
                        }

                    if known[pcode].get("processed_on") == str(target_date):
                        _mark(pcode)
                        continue

                    # если известен, но давно не проверялся — обработаем
                    last_checked_str = known[pcode].get("last_checked")
                    try:
                        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d").date() if last_checked_str else None
                    except ValueError:
                        last_checked = None

                    process_patient(conn, pcode, known, target_date, is_new=(known[pcode].get("data_hash") is None))
                    _mark(pcode)

            stage_log("Дата", status="итог", значение=str(target_date), обработано=len(processed_list))
            save_known_patients(known)

        # --- Экспорт CSV по уникальным pcode за запуск ---
        unique_pcodes = sorted(already_processed)
        if unique_pcodes:
            try:
                stage_log("Экспорт CSV", status="старт", пациентов=len(unique_pcodes))
                csv_path_med = csv_dir / "processed_patients.csv"
                csv_path_pers = csv_dir / "processed_patients_personal_data.csv"

                ok_med = export_patients_to_csv(conn, unique_pcodes, csv_path_med)
                ok_pers = export_personal_data_to_csv(conn, unique_pcodes, csv_path_pers)

                stage_log(
                    "Экспорт CSV",
                    status="успех" if (ok_med and ok_pers) else "частично",
                    файл_med=str(csv_path_med),
                    файл_pers=str(csv_path_pers),
                )

                # Загрузка в Bitrix
                try:
                    if getattr(settings, "BITRIX_MODE", "api").lower() == "api":
                        from app.export.bitrix_api_loader import main as load_csv_to_bitrix_api
                        stage_log("Загрузка в Bitrix", status="старт", режим="REST API")
                        load_csv_to_bitrix_api()
                    else:
                        from app.export.bitrix_loader import load_csv_to_bitrix
                        stage_log("Загрузка в Bitrix", status="старт", режим="Selenium")
                        load_csv_to_bitrix(settings)

                    stage_log("Загрузка в Bitrix", status="успех")
                except Exception as e:
                    stage_log("Загрузка в Bitrix", status="ошибка", сообщение=str(e))

            except Exception as e:
                stage_log("Экспорт CSV", status="ошибка", сообщение=str(e))
        else:
            stage_log("Экспорт CSV", status="пропуск", причина="нет пациентов")

    stage_log("Обработка диапазона дат", status="завершено", всего_уникально=len(unique_pcodes))


# ===================== CLI =====================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сканирование пациентов и генерация отчетов")
    parser.add_argument("--start-date", help="Начальная дата диапазона (dd.MM.yyyy)")
    parser.add_argument("--end-date", help="Конечная дата диапазона (dd.MM.yyyy)")
    parser.add_argument("--date", help="Одиночная дата (dd.MM.yyyy)")
    parser.add_argument("--pcode", help="Фильтр по конкретному пациенту/пациентам (через запятую)")

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
    elif args.date:
        try:
            single_date = datetime.strptime(args.date, "%d.%m.%Y").date()
            date_range = [single_date]
        except ValueError:
            raise SystemExit("Ошибка: укажи дату в формате dd.MM.yyyy")
    else:
        today = date.today()
        date_range = [today]

    filter_pcodes: List[str] = []
    if args.pcode:
        pcode_list = [p.strip() for p in args.pcode.split(",")]
        filter_pcodes = [p for p in pcode_list if p]
        if not filter_pcodes:
            raise SystemExit("Ошибка: указаны пустые PCODE")

    main(date_range, filter_pcodes)
