def format_patient_data(data: dict) -> dict:
    result = {}

    # Основная информация
    info = data.get("info") or {}
    result["ФИО"] = f"{info.get('LASTNAME', '')} {info.get('FIRSTNAME', '')} {info.get('MIDNAME', '')}".strip()
    result["Фамилия"] = info.get('LASTNAME', '')
    result["Имя"] = info.get('FIRSTNAME', '')
    result["Отчество"] = info.get('MIDNAME', '')
    bdate = info.get("BDATE")
    result["Дата рождения"] = bdate.strftime("%d.%m.%Y") if bdate else "—"
    result["Адрес"] = info.get("FULL_ADDR") or "—"
    result["Телефон"] = ", ".join(
        [t for t in [info.get("PHONE1"), info.get("PHONE2"), info.get("PHONE3")] if t]
    ) or "—"
    result["Email"] = info.get("CLMAIL") or "—"
    #Филиал
    result["Филиал"] = info.get("FILIAL_NAME") or "—"
    #По рекомендации
    result["По рекомендации"] = info.get("REKLAMA") or 0
    # ФИО консультанта
    result["ФИО консультанта"] = info.get("CONSULT_DOCTOR") or "—"
    # Дата первичного приёма
    fw = info.get("FIRSTWORKDATE")
    result["Дата первичного приёма"] = fw.strftime("%d.%m.%Y") if fw else "—"
    result["Доктор первичного приёма"] = info.get("FIRST_DOCTOR") or "—"
    result["Статус пациента"] = info.get("AGESTATUS_NAME") or "Статус не установлен"
    result["Тип пациента"] = info.get("TYPESTATUS_NAME") or "Статус не установлен"
    result["Текущая стадия лечения"] = data.get("current_stage") or "—"
    approved_plans = data.get("approved_plans") or []
    result["Количество визитов в клинику"] = info.get("VISIT_COUNT") or 0

    result["Предстоящие приёмы"] = data.get("future_appointments", [])

    # Параметры обследования
    params = data.get("params") or []
    result["Параметры обследования"] = {p["NAMEPARAMS"]: p["VALUETEXT"] for p in params}

    # Составной план лечения
    comp_plan = data.get("composite_plan") or []
    result["Составной план"] = [p["CONCATENATION"] for p in comp_plan]


    # Комплексные планы
    complex_plans = data.get("complex_plans") or []
    pretty_complex = []
    for cp in complex_plans:
        header = f"{cp.get('PLANTYPENAME', '—')} ({cp.get('DEPNAME', '—')})"
        details = []
        total = 0
        for d in cp.get("details", []):
            name = d.get("SCHNAME")
            count = d.get("SCOUNT") or 0
            amount = d.get("ROUND") or 0
            line_sum = count * amount
            total += line_sum
            details.append({"name": name,
                            "count": count,
                            "amount": amount,
                            "total": line_sum})
        pretty_complex.append({"План": header, "Состав": details, "Итого": total})
    result["Комплексные планы"] = pretty_complex

    # --- Согласованные планы ---
    approved_plans = data.get("approved_plans") or []
    pretty_approved = []
    for plan in approved_plans:
        header = f"Согласованный план ({plan.get('DEPNAME', '—')})"
        details = []
        #total = plan.get("SUMMARUB", 0) так было
        total = 0
        for d in plan.get("details", []):
            name = d.get("SCHNAME")
            count = d.get("SCOUNT") or 0
            amount = d.get("AMOUNTRUB") or 0
            line_sum = count * amount
            total += line_sum
            details.append({
                "name": name,
                "count": count,
                "amount": amount,
                "total": line_sum
            })
        pretty_approved.append({
            "План": header,
            "Дата": plan.get("TREATDATE").strftime("%d.%m.%Y") if plan.get("TREATDATE") else "—",
            "Доктор": plan.get("DOCTOR_NAME") or "—",
            "Состав": details,
            "Итого": total
        })
    result["Согласованные планы"] = pretty_approved
    result["Общая оплаченная сумма по согласованным планам"] = data.get("approved_plans_paid", 0)

    return result
