# app/export/csv_exporter.py

from __future__ import annotations
import csv
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from decimal import Decimal, ROUND_HALF_UP

from app.logging import get_logger
from app.db.extract import collect_patient_data
from app.utils.formatting import format_patient_data

log = get_logger(__name__)

# Настройки
CSV_ENCODING = "cp1251"
CSV_DELIMITER = ";"

# Заголовки отчёта
CSV_HEADERS = [
    "Название лида",
    "Фамилия",
    "Имя",
    "Отчество",
    "Возраст пациента",
    "ФИО консультанта пациента",
    "Тип пациента 1",
    "Тип пациента 2",
    "Наличие/отсутствие снимка ОПТГ у пациента",
    "ФИО доктора, проводившего первичный прием",
    "Дата первого визита",
    "Количество визитов в клинику",
    "Дата следующего приема и ФИО доктора, к кому пациент записан на прием",
    "Стоимость всех предварительных планов",
    "Стоимость всех согласованных планов",
    "Сумма оплаченных денег пациентом в клинику",
    "Процент выполнения плана",
    "Комплексный план",
    "Стадия",
    "Текущая стадия лечения",
]


# ======= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =======

def calculate_age(birth_date) -> str:
    """Вычисляет возраст по дате рождения"""
    try:
        if isinstance(birth_date, str):
            birth_date = datetime.strptime(birth_date, "%d.%m.%Y").date()
        elif isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return str(age)
    except Exception:
        return "—"


def convert_patient_data_to_csv_row(formatted_data: Dict[str, Any]) -> Dict[str, str]:
    """Формирует строку данных для CSV/Excel"""
    fullname = formatted_data.get("ФИО", "—")
    lname = formatted_data.get("Фамилия", "—")
    fname = formatted_data.get("Имя", "—")
    mname = formatted_data.get("Отчество", "—")
    age = calculate_age(formatted_data.get("Дата рождения"))
    consultant = formatted_data.get("ФИО консультанта", "—")
    patient_type_1 = formatted_data.get("Статус пациента", "—")
    patient_type_2 = formatted_data.get("Тип пациента", "—")
    opgt = formatted_data.get("ОПТГ", "—")
    first_doctor = formatted_data.get("Доктор первичного приёма", "—")
    first_visit_date = formatted_data.get("Дата первичного приёма")
    visits_count = formatted_data.get("Количество визитов в клинику", "—")

    # --- Предстоящие приёмы ---
    future_appointments = formatted_data.get("Предстоящие приёмы", [])
    next_appointment = "—"
    canceled_exists = False
    if future_appointments:
        for appt in future_appointments:
            status = appt.get("Статус")
            if status == "ОТМЕНЕНО":
                canceled_exists = True
            elif status == "ОЖИДАЕТСЯ":
                date_ = appt.get("Дата", appt.get("WORK_DATE_STR", ""))
                filial = appt.get("Филиал", appt.get("FILIAL_NAME", ""))
                doctor = appt.get("Доктор", appt.get("DOCTOR_NAME", ""))
                comment = appt.get("Комментарий", appt.get("SCHEDAPPEALS_COMMENT", ""))
                next_appointment = f"{date_}, {filial}, {doctor}, Комментарий: {comment}"
                canceled_exists = False
                break

    # --- Стоимости ---
    complex_plans = formatted_data.get("Комплексные планы", [])
    prelim_cost = sum(plan.get("Итого", 0) for plan in complex_plans)

    approved_plans = formatted_data.get("Согласованные планы", [])
    approved_cost = sum(plan.get("Итого", 0) for plan in approved_plans)
    paid_amount = formatted_data.get("Общая оплаченная сумма по согласованным планам", 0)
    plan_percent = Decimal(0)
    if approved_cost:
        plan_percent = (Decimal(paid_amount) / Decimal(approved_cost) * 100).quantize(Decimal("1"),
                                                                                      rounding=ROUND_HALF_UP)
    # --- Стадия ---
    current_stage = formatted_data.get("Текущая стадия лечения", "—")
    if canceled_exists or not future_appointments:
        stage = "Нет записей"
    else:
        stage = current_stage

    return {
        "Название лида": fullname or "—",
        "Фамилия": lname,
        "Имя": fname,
        "Отчество": mname,
        "Возраст пациента": age,
        "ФИО консультанта пациента": consultant,
        "Тип пациента 1": patient_type_1,
        "Тип пациента 2": patient_type_2,
        "Наличие/отсутствие снимка ОПТГ у пациента": opgt,
        "ФИО доктора, проводившего первичный прием": first_doctor,
        "Дата первого визита": first_visit_date,
        "Количество визитов в клинику": visits_count,
        "Дата следующего приема и ФИО доктора, к кому пациент записан на прием": next_appointment,
        "Стоимость всех предварительных планов": f"{prelim_cost:.2f} руб.",
        "Стоимость всех согласованных планов": f"{approved_cost:.2f} руб.",
        "Сумма оплаченных денег пациентом в клинику": f"{paid_amount:.2f} руб.",
        "Процент выполнения плана": f"{plan_percent}%",
        "Комплексный план": "—",
        "Стадия": stage,
        "Текущая стадия лечения": current_stage,
    }


def _apply_excel_formatting(ws):
    """Форматирование Excel-листа"""
    # 1) Шапка
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # 2) Ширины колонок по содержимому
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[get_column_letter(col[0].column)].width = max_length + 2

    # 3) Общие выравнивания
    for row in ws.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    # 4) Фильтры на все столбцы + заморозка шапки (удобно)
    if ws.max_row and ws.max_column:
        first_cell = "A1"
        last_cell = f"{get_column_letter(ws.max_column)}{ws.max_row}"
        ws.auto_filter.ref = f"{first_cell}:{last_cell}"
        ws.freeze_panes = "A2"  # опционально: закрепить строку заголовков


def _write_excel_file(output_file: Path, rows: List[Dict[str, str]]):
    wb = Workbook()
    ws = wb.active
    ws.title = "Отчёт"
    ws.append(CSV_HEADERS)
    for row in rows:
        ws.append([row.get(h, "") for h in CSV_HEADERS])
    _apply_excel_formatting(ws)
    wb.save(output_file)


def _append_to_management_report(management_path: Path, rows: List[Dict[str, str]]):
    """Добавляет данные без дубликатов"""
    if management_path.exists():
        wb = load_workbook(management_path)
        ws = wb.active
        existing_names = {str(cell.value).strip() for cell in ws["A"][1:] if cell.value}
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "Management"
        ws.append(CSV_HEADERS)
        existing_names = set()

    added = 0
    for row in rows:
        name = str(row.get("Название лида", "")).strip()
        if name and name not in existing_names:
            ws.append([row.get(h, "") for h in CSV_HEADERS])
            existing_names.add(name)
            added += 1

    _apply_excel_formatting(ws)
    wb.save(management_path)
    log.info(f"Добавлено {added} новых записей в {management_path}")


# ======= ОСНОВНАЯ ФУНКЦИЯ =======

def export_patients_to_csv(conn, patient_pcodes: List[str], output_file: Path) -> bool:
    """Создаёт processed_patients.csv, processed_patients.xlsx и обновляет management_report.xlsx"""
    csv_rows: List[Dict[str, str]] = []

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        management_path = output_file.parent / "management_report.xlsx"
        excel_output = output_file.with_suffix(".xlsx")
        csv_output = output_file.with_suffix(".csv")

        # Сбор данных по пациентам
        for pcode in patient_pcodes:
            try:
                log.info(f"Обрабатываем пациента {pcode}")
                raw_data = collect_patient_data(conn, pcode)
                formatted = format_patient_data(raw_data)
                csv_row = convert_patient_data_to_csv_row(formatted)
                csv_rows.append(csv_row)
            except Exception as e:
                log.error(f"Ошибка при обработке {pcode}: {e}")
                continue

        if not csv_rows:
            log.warning("Нет данных для экспорта пациентов.")
            return False

        # 1️⃣ CSV без форматирования
        with open(csv_output, "w", newline="", encoding=CSV_ENCODING) as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, delimiter=CSV_DELIMITER)
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)
        log.info(f"Создан CSV-файл: {csv_output}")

        # 2️⃣ Excel с форматированием
        _write_excel_file(excel_output, csv_rows)
        log.info(f"Создан Excel-файл: {excel_output}")

        # 3️⃣ Обновление накопительного отчёта
        _append_to_management_report(management_path, csv_rows)

        return True

    except Exception as e:
        log.error(f"Ошибка при экспорте пациентов: {e}")
        return False

def export_personal_data_to_csv(conn, patient_pcodes: List[str], output_file: Path) -> bool:
    """Создает CSV с персональными данными пациентов"""
    headers = ["Фамилия", "Имя", "Отчество", "Дата рождения", "Телефон", "Email", "Адрес"]

    try:
        output_file = output_file.with_suffix(".csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        csv_rows = []
        for pcode in patient_pcodes:
            try:
                raw_data = collect_patient_data(conn, pcode)
                formatted = format_patient_data(raw_data)
                row = {
                    "Фамилия": formatted.get("Фамилия", "—"),
                    "Имя": formatted.get("Имя", "—"),
                    "Отчество": formatted.get("Отчество", "—"),
                    "Дата рождения": formatted.get("Дата рождения", "—"),
                    "Телефон": formatted.get("Телефон", "—"),
                    "Email": formatted.get("Email", "—"),
                    "Адрес": formatted.get("Адрес", "—"),
                }
                csv_rows.append(row)
            except Exception as e:
                log.error(f"Ошибка при обработке {pcode}: {e}")
                continue

        with open(output_file, "w", newline="", encoding=CSV_ENCODING) as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=CSV_DELIMITER)
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)

        log.info(f"Создан CSV с персональными данными: {output_file}")
        return True

    except Exception as e:
        log.error(f"Ошибка при экспорте персональных данных: {e}")
        return False
