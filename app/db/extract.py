def _fetch_one(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetch_all(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


@log_call()
def fetch_primary_patients_today(conn, target_date):
    # пациенты с первичным приёмом на указанную дату
    return _fetch_all(conn, SQL_PRIMARY_APPTS_TODAY, (target_date,))

@log_call()
def fetch_main_info(conn, pcode: str):
    return _fetch_one(conn, SQL_MAIN_QUERY, (pcode,))


@log_call()
def fetch_last_obslnum(conn, pcode: str):
    return _fetch_one(conn, SQL_GET_LAST_OBSLED, (pcode,))


@log_call()
def fetch_paramsinfo(conn, obslnum: int):
    return _fetch_all(conn, SQL_GET_PARAMSINFO, (obslnum,))


@log_call()
def fetch_composite_plan(conn, pcode: str):
    return _fetch_all(conn, SQL_GET_TREATMENT_PLAN, (pcode,))


@log_call()
def fetch_future_appointments(conn, pcode: str):
    return _fetch_all(conn, SQL_GET_APPOINTMENTS, (pcode,))


@log_call()
def fetch_complex_plans(conn, pcode: str):
    return _fetch_all(conn, SQL_GET_COMPLEX_PLANS, (pcode,))


# --- Согласованные планы ---
@log_call()
def fetch_approved_plans(conn, pcode: str):
    return _fetch_all(conn, SQL_GET_APPROVED_PLANS, (pcode,))

@log_call()
def fetch_approved_plans_paid(conn, pcode: str):
    row = _fetch_one(conn, SQL_GET_APPROVED_PLANS_PAID, (pcode,))
    return row["PAID_SUM"] if row and row["PAID_SUM"] is not None else 0

@log_call()
def fetch_current_stage(conn, pcode: str):
    treatcodes = _fetch_all(conn, SQL_GET_TREATCODES, (pcode,))
    stage_value = None
    for t in treatcodes:
        rows = _fetch_all(conn, SQL_GET_STAGE, (t["TREATCODE"],))
        for r in rows:
            if r["VALUETEXT"]:  # берём только не NULL
                stage_value = r["VALUETEXT"]
    return stage_value

@log_call()
def fetch_future_appointments(conn, pcode: str):
    appointments = _fetch_all(conn, SQL_GET_FUTURE_APPOINTMENTS, (pcode,))
    enriched = []
    for a in appointments:
        schedid = a["SCHEDID"]
        sched_info = _fetch_one(conn, SQL_GET_SCHEDULE_INFO, (schedid,))
        status = "ОЖИДАЕТСЯ"
        if sched_info:
            duration = (sched_info["FHOUR"] * 60 + sched_info["FMIN"]) - (sched_info["BHOUR"] * 60 + sched_info["BMIN"])
            if duration in (1, 10):
                status = "ОТМЕНЕНО"

        enriched.append({
            # оригинальные ключи для main.py и formatting.py
            "WORK_DATE_STR": a["WORK_DATE_STR"],
            "DOCTOR_NAME": a["DOCTOR_NAME"],
            "FILIAL_NAME": a["FILIAL_NAME"],
            "SCHEDAPPEALS_COMMENT": a["SCHEDAPPEALS_COMMENT"],

            # «человеческие» ключи для отчёта
            "Дата": a["WORK_DATE_STR"],
            "Филиал": a["FILIAL_NAME"],
            "Доктор": a["DOCTOR_NAME"],
            "Комментарий": a["SCHEDAPPEALS_COMMENT"],
            "Статус": status,
        })

    return enriched

@log_call()
def collect_patient_data(conn, pcode: str) -> dict:
    result = {}

    # Основная информация
    info = fetch_main_info(conn, pcode)
    result["info"] = info

    # OBSLNUM + параметры обследования
    obsled = fetch_last_obslnum(conn, pcode)
    if obsled:
        result["last_obslnum"] = obsled["OBSLNUM"]
        result["params"] = fetch_paramsinfo(conn, obsled["OBSLNUM"])
    else:
        result["last_obslnum"] = None
        result["params"] = []

    # Составной план лечения
    result["composite_plan"] = fetch_composite_plan(conn, pcode)

    result["current_stage"] = fetch_current_stage(conn, pcode)

    # Будущие приёмы
    result["appointments"] = fetch_future_appointments(conn, pcode)

    # Комплексные планы
    complex_plans = fetch_complex_plans(conn, pcode)
    enriched_complex_plans = []
    for cp in complex_plans:
        cp_details = _fetch_all(conn, SQL_GET_PLAN_DETAILS, (cp["DID"],))
        cp_copy = cp.copy()
        cp_copy["details"] = cp_details
        enriched_complex_plans.append(cp_copy)
    result["complex_plans"] = enriched_complex_plans

    # Согласованные планы
    approved_plans = fetch_approved_plans(conn, pcode)
    grouped = {}
    for row in approved_plans:
        did = row["DID"]
        if did not in grouped:
            grouped[did] = {
                "DEPNAME": row["DEPNAME"],
                "SUMMARUB": row["SUMMARUB"],
                "TREATDATE": row["PDATE"],
                "DOCTOR_NAME": row["DOCTOR_NAME"],
                "details": []
            }
        grouped[did]["details"].append({
            "SCHNAME": row["SCHNAME"],
            "SCOUNT": row["SCOUNT"],
            "AMOUNTRUB": row["AMOUNTRUB"],
        })
    result["approved_plans"] = list(grouped.values())

    # Общая оплаченная сумма (BALANCEAMOUNT)
    result["approved_plans_paid"] = fetch_approved_plans_paid(conn, pcode)

    result["future_appointments"] = fetch_future_appointments(conn, pcode)

    return result
