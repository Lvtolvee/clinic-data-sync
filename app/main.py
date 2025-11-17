from __future__ import annotations

import json
import argparse
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

from app.config import load_non_secret_env, Settings
from app.custom_logging import setup_logging, get_logger, patient_log, stage_log
from app.db.client import get_connection
from app.db.extract import (
    fetch_primary_patients_today,
    fetch_future_appointments,
    fetch_main_info,
    collect_patient_data,
    fetch_repeat_patients
)
from app.utils.formatting import format_patient_data
from app.reports.patient_report import build_patient_report
from app.export.csv_exporter import export_patients_to_csv, export_personal_data_to_csv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_non_secret_env(str(ENV_PATH))
settings = Settings()
setup_logging(
    level=getattr(settings, "LOG_LEVEL", "INFO"),
    log_file=getattr(settings, "LOG_FILE", "logs/app.log"),
    audit_log_file=getattr(settings, "AUDIT_LOG_FILE", "logs/audit.log"),
)
log = get_logger(__name__)

DATA_FILE = Path("known_patients.json")
PDF_DIR = Path("output") / "reports"
PDF_DIR.mkdir(parents=True, exist_ok=True)


def _serialize_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return str(value)
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def calculate_patient_hash(patient_data: dict) -> str:
    #Хешируем только те поля, которые уходят в Bitrix (CSV_HEADERS)

    # Берём финальную модель данных (как для CSV)
    formatted = format_patient_data(dict(patient_data))

    # Поля, которые реально отправляются в Bitrix
    key_fields = {
        "ФИО": formatted.get("ФИО"),
        "Фамилия": formatted.get("Фамилия"),
        "Имя": formatted.get("Имя"),
        "Отчество": formatted.get("Отчество"),
        "Возраст пациента": formatted.get("Возраст пациента"),
        "ФИО консультанта": formatted.get("ФИО консультанта"),
        "Тип пациента 1": formatted.get("Статус пациента"),
        "Тип пациента 2": formatted.get("Тип пациента"),
        "Доктор первичного приёма": formatted.get("Доктор первичного приёма"),
        "Дата первичного приёма": formatted.get("Дата первичного приёма"),
        "Количество визитов": formatted.get("Количество визитов в клинику"),
        "Следующий визит": formatted.get("Предстоящие приёмы"),
        "Стоимость предварительных планов": sum(p["Итого"] for p in formatted.get("Комплексные планы", [])),
        "Стоимость согласованных планов": sum(p["Итого"] for p in formatted.get("Согласованные планы", [])),
        "Сумма оплат": formatted.get("Общая оплаченная сумма по согласованным планам"),
        "Процент выполнения": formatted.get("Процент выполнения плана, %"),
        "Стадия": formatted.get("Стадия"),
        "Текущая стадия лечения": formatted.get("Текущая стадия лечения"),
        "Ответственный": formatted.get("Ответственный"),
        "Филиал": formatted.get("Филиал"),
        "По рекомендации": formatted.get("По рекомендации"),
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


def process_patient(conn, pcode: str, known: dict, target_date: date, is_new: bool = False) -> None:
    try:
        current_data = collect_patient_data(conn, pcode)
        current_hash = calculate_patient_hash(current_data)
        appts = fetch_future_appointments(conn, pcode)
        
        def _parse(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").date()
            except:
                return None

        dates = [_parse(a.get("WORK_DATE_STR")) for a in appts]
        latest_appt = max([d for d in dates if d], default=None)
        if latest_appt:
            latest_appt = latest_appt.strftime("%Y-%m-%d")

        patient_info = known.get(pcode, {})
        last_saved_appt = patient_info.get("last_appointment_date")
        last_saved_hash = patient_info.get("data_hash")

        pdf_path = PDF_DIR / f"patient_{pcode}.pdf"

        need_regen = False
        if not pdf_path.exists():
            need_regen = True
        elif current_hash != last_saved_hash:
            need_regen = True
        elif last_saved_hash is None:
            need_regen = True
        elif last_saved_appt is None or (latest_appt and last_saved_appt and latest_appt > last_saved_appt):
            need_regen = True

        if need_regen:
            build_patient_report(conn, pcode, str(pdf_path))
            known[pcode] = {
                "last_appointment_date": latest_appt,
                "data_hash": current_hash,
                "last_checked": str(target_date),
                "last_updated": str(date.today()),
                "processed_on": str(target_date),
            }
            if is_new:
                patient_log(pcode, status="внесен", comment="новый пациент")
            else:
                patient_log(pcode, status="обновлен", comment="генерация отчёта", pdf=pdf_path.name)
        else:
            known.setdefault(pcode, {})
            known[pcode]["last_checked"] = str(target_date)
            known[pcode]["processed_on"] = str(target_date)
            if is_new:
                patient_log(pcode, status="внесен", comment="новый пациент")
            else:
                patient_log(pcode, status="пропущен", comment="без изменений")

    except Exception as e:
        known.setdefault(pcode, {})
        known[pcode]["processed_on"] = str(target_date)
        patient_log(pcode, status="ошибка", comment="не удалось обработать", ошибка=str(e))


def process_and_register_patient(conn, pcode, known, target_date, all_processed_pcodes, is_new=False):
    process_patient(conn, pcode, known, target_date, is_new)
    if pcode not in all_processed_pcodes:
        all_processed_pcodes.append(pcode)


def main(date_range: List[date], filter_pcodes: List[str] | None = None) -> None:
    log.info(f"Запуск обработки за диапазон {date_range[0]} → {date_range[-1]}")
    known = load_known_patients()

    all_processed_pcodes: list[str] = []

    csv_dir = Path("output") / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path_med = csv_dir / "processed_patients.csv"
    csv_path_pers = csv_dir / "processed_patients_personal_data.csv"
    xlsx_path_med = csv_dir / "processed_patients.xlsx"
    mgmt_report = csv_dir / "management_report.xlsx"

    for p in (csv_path_med, csv_path_pers, xlsx_path_med, mgmt_report):
        try:
            if p.exists():
                p.unlink()
                log.info(f"Старый файл удалён: {p}")
        except Exception as e:
            log.warning(f"Не удалось удалить {p}: {e}")

    with get_connection(settings) as conn:
        log.info("Обновляем известных пациентов перед обработкой дат...")
        # Получаем всех пациентов типа "Повторный пациент под кураторством"
        repeat_rows = fetch_repeat_patients(conn)
        repeat_pcodes = {str(r["PCODE"]) for r in repeat_rows}
        log.info(f"Повторных пациентов под кураторством: {len(repeat_pcodes)}")

        # Проверяем ВСЕХ пациентов из known_patients.json
        for pcode, pdata in list(known.items()):
            try:
                current_data = collect_patient_data(conn, pcode)
                current_hash = calculate_patient_hash(current_data)
                last_saved_hash = pdata.get("data_hash")

                if last_saved_hash != current_hash:
                    # Хеш изменился → пересоздаем отчёт
                    log.info(f"Изменения у {pcode}: хэш изменился — пересоздаём отчёт")

                    process_patient(conn, pcode, known, date_range[0], is_new=False)

                    # обновляем только нужные поля
                    known[pcode]["data_hash"] = current_hash
                    known[pcode]["last_checked"] = str(date_range[0])
                    known[pcode]["last_updated"] = str(date.today())

                    # включаем в CSV
                    if pcode not in all_processed_pcodes:
                        all_processed_pcodes.append(pcode)

                else:
                    # Хеш НЕ изменился — только обновляем дату проверки
                    known[pcode]["last_checked"] = str(date_range[0])

            except Exception as e:
                log.error(f"Ошибка при проверке {pcode}: {e}")
                continue

        for target_date in date_range:
            log.info(f"\n=== Обработка за {target_date} ===")
            processed_today: list[str] = []

            # СНАЧАЛА: повторные пациенты под кураторством
            for pcode in repeat_pcodes:
                info = fetch_main_info(conn, pcode)
                if not info:
                    log.warning(f"Пациент с PCODE={pcode} не найден")
                    continue

                if pcode not in known:
                    known[pcode] = {
                        "last_checked": str(target_date),
                        "last_appointment_date": None,
                        "data_hash": None,
                    }
                    log.info(
                        f"Новый пациент (повторный под кураторством): {pcode} {info.get('LASTNAME', '')} {info.get('FIRSTNAME', '')}")

                process_and_register_patient(
                    conn, pcode, known, target_date,
                    all_processed_pcodes, is_new=False  # Аналогично обновлению старых
                )
                processed_today.append(pcode)

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
                        log.info(f"Новый пациент (по PCODE): {pcode} {info.get('LASTNAME','')} {info.get('FIRSTNAME','')}")
                    process_and_register_patient(conn, pcode, known, target_date, all_processed_pcodes, is_new=True)
                    processed_today.append(pcode)

            if not filter_pcodes:
                new_patients = fetch_primary_patients_today(conn, target_date)
                for p in new_patients:
                    pcode = str(p["PCODE"])
                    last_checked_str = known.get(pcode, {}).get("last_checked")
                    try:
                        last_checked = datetime.strptime(last_checked_str, "%Y-%m-%d").date() if last_checked_str else None
                    except ValueError:
                        last_checked = None

                    if not last_checked or last_checked < target_date:
                        if pcode not in known:
                            known[pcode] = {
                                "last_checked": str(target_date),
                                "last_appointment_date": None,
                                "data_hash": None,
                            }
                            log.info(f"Новый пациент (по дате): {pcode} {p.get('LASTNAME','')} {p.get('FIRSTNAME','')}")
                        process_and_register_patient(conn, pcode, known, target_date, all_processed_pcodes, is_new=True)
                        processed_today.append(pcode)

        if all_processed_pcodes:
            unique_pcodes = sorted(set(all_processed_pcodes))
            try:
                export_patients_to_csv(conn, unique_pcodes, csv_path_med)
                export_personal_data_to_csv(conn, unique_pcodes, csv_path_pers)
                log.info(f"Экспорт CSV: всего {len(unique_pcodes)} пациентов")
            except Exception as e:
                log.error(f"Ошибка экспорта CSV: {e}")
        else:
            log.info("Нет пациентов для экспорта CSV")

        save_known_patients(known)
        log.info(f"Файл known_patients.json обновлён ({len(known)} записей)")

        if all_processed_pcodes:
            try:
                log.info("Начинаем загрузку CSV в Битрикс...")
                if settings.BITRIX_MODE.lower() == "api":
                    from app.export.bitrix_api_loader import main as load_csv_to_bitrix_api
                    log.info("Режим загрузки: Bitrix REST API")
                    load_csv_to_bitrix_api()
                else:
                    from app.export.bitrix_loader import load_csv_to_bitrix
                    log.info("Режим загрузки: Selenium")
                    load_csv_to_bitrix(settings)
                log.info("Загрузка CSV в Битрикс завершена успешно")
            except Exception as e:
                log.error(f"Ошибка при загрузке CSV в Bitrix24: {e}")
        else:
            log.warning("Нет данных для загрузки в Битрикс")

    log.info(f"Обработка диапазона завершена. Всего уникальных пациентов: {len(set(all_processed_pcodes))}")


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
