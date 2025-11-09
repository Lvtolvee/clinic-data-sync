from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from app.db.extract import collect_patient_data
from app.utils.formatting import format_patient_data


def _register_preferred_font(font_name: str, candidates: Iterable[Path]) -> str:
    for candidate in candidates:
        if candidate.exists():
            pdfmetrics.registerFont(TTFont(font_name, str(candidate)))
            return font_name
    return "Helvetica"


FONT_NAME = _register_preferred_font(
    "Arial",
    (
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
        Path("C:/Windows/Fonts/arial.ttf"),
    ),
)


def _format_future_date(raw: str | None) -> str:
    if not raw:
        return "—"
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y.%m.%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return raw


def _resolve_report_path(
    pcode: str, output_dir: Path | str | None, output_file: Path | str | None
) -> Path:
    if output_file is not None:
        report_path = Path(output_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        return report_path

    if output_dir is not None:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"{pcode}.pdf"

    raise ValueError("Должен быть указан либо output_dir, либо output_file")


def _render_report(data: Mapping[str, Any], report_path: Path) -> Path:
    doc = SimpleDocTemplate(str(report_path), pagesize=A4)
    styles = getSampleStyleSheet()
    normal = styles["Normal"]
    normal.fontName = FONT_NAME

    title_style = ParagraphStyle(
        "Title",
        parent=styles["Heading1"],
        alignment=1,
        spaceAfter=10,
        fontName=FONT_NAME,
    )
    h2_style = ParagraphStyle("H2", parent=styles["Heading2"], fontName=FONT_NAME)

    story = []

    # Заголовок
    story.append(Paragraph("Отчёт по пациенту", title_style))
    story.append(Spacer(1, 12))

    # Основные сведения
    story.append(Paragraph(f"<b>ФИО:</b> {data.get('ФИО', '—')}", normal))
    story.append(Paragraph(f"<b>Дата рождения:</b> {data.get('Дата рождения', '—')}", normal))
    story.append(Paragraph(f"<b>Адрес:</b> {data.get('Адрес', '—')}", normal))
    story.append(Paragraph(f"<b>Телефон:</b> {data.get('Телефон', '—')}", normal))
    story.append(Paragraph(f"<b>Email:</b> {data.get('Email', '—')}", normal))
    story.append(Spacer(1, 12))

    story.append(Paragraph(f"<b>ФИО консультанта:</b> {data.get('ФИО консультанта', '—')}", normal))
    story.append(Paragraph(f"<b>Дата первичного приёма:</b> {data.get('Дата первичного приёма', '—')}", normal))
    story.append(Paragraph(f"<b>Доктор первичного приёма:</b> {data.get('Доктор первичного приёма', '—')}", normal))
    story.append(Paragraph(f"<b>Статус пациента:</b> {data.get('Статус пациента', '—')}", normal))
    story.append(Paragraph(f"<b>Тип пациента:</b> {data.get('Тип пациента', '—')}", normal))
    story.append(Paragraph(f"<b>Текущая стадия лечения:</b> {data.get('Текущая стадия лечения', '—')}", normal))
    story.append(Paragraph(f"<b>Количество визитов в клинику:</b> {data.get('Количество визитов в клинику', 0)}", normal))

    story.append(Spacer(1, 12))

    # Предстоящие приёмы
    story.append(Paragraph("<b>Предстоящие приёмы:</b>", h2_style))
    future = data.get("Предстоящие приёмы", [])
    if future:
        table_data = [[
            Paragraph("Дата", normal),
            Paragraph("Филиал", normal),
            Paragraph("Доктор", normal),
            Paragraph("Комментарий", normal),
            Paragraph("Статус", normal),
        ]]
        for r in future:
            table_data.append([
                Paragraph(_format_future_date(r.get("Дата")), normal),
                Paragraph(r["Филиал"] or "—", normal),
                Paragraph(r["Доктор"] or "—", normal),
                Paragraph(r["Комментарий"] or "—", normal),
                Paragraph(r["Статус"], normal),
            ])
        table = Table(table_data, colWidths=[70, 70, 130, 150, 80], repeatRows=1)
        table.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), FONT_NAME),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),  # выравнивание даты
            ("ALIGN", (-1, 1), (-1, -1), "CENTER"),  # выравнивание статуса
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ]))

        story.append(table)
    else:
        story.append(Paragraph("—", normal))
    story.append(Spacer(1, 12))

    # Комплексные планы
    story.append(Paragraph("<b>Комплексные планы:</b>", h2_style))
    complex_plans = data.get("Комплексные планы", [])
    if complex_plans:
        total_all_complex = 0
        for cp in complex_plans:
            table_data = [[
                Paragraph("Услуга", normal),
                Paragraph("Кол-во", normal),
                Paragraph("Стоимость", normal),
                Paragraph("Итого", normal),
            ]]
            for d in cp["Состав"]:
                table_data.append([
                    Paragraph(d["name"], normal),
                    str(d["count"]),
                    f"{d['amount']:,.2f}",
                    f"{d['total']:,.2f}",
                ])
            table_data.append(["", "", Paragraph("ИТОГО", normal), f"{cp['Итого']:,.2f}"])

            table = Table(table_data, colWidths=[300, 50, 70, 80], repeatRows=1)
            table.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, -1), FONT_NAME),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("BACKGROUND", (0, -1), (-1, -1), colors.whitesmoke),
                ("ALIGN", (1, 1), (-1, -1), "CENTER"),
                ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))

            block = [
                Paragraph(f"<u>{cp['План']}</u>", normal),
                Spacer(1, 6),
                table,
                Spacer(1, 12),
            ]
            story.append(KeepTogether(block))
            story.append(Spacer(1, 20))

            total_all_complex += cp["Итого"]

        story.append(Paragraph(f"<b>Общая сумма по комплексным планам: {total_all_complex:,.2f} руб.</b>", normal))
    else:
        story.append(Paragraph("—", normal))
    story.append(Spacer(1, 12))

    # Согласованные планы
    story.append(PageBreak())  # начинаем с новой страницы
    story.append(Paragraph("<b>Согласованные планы:</b>", h2_style))
    approved = data.get("Согласованные планы", [])
    if approved:
        total_all = 0
        for plan in approved:
            table_data = [[
                Paragraph("Услуга", normal),
                Paragraph("Кол-во", normal),
                Paragraph("Цена", normal),
                Paragraph("Сумма", normal),
            ]]
            for r in plan["Состав"]:
                table_data.append([
                    Paragraph(r["name"] or "—", normal),
                    str(r["count"]),
                    f"{r['amount']:,.2f}",
                    f"{r['total']:,.2f}",
                ])
            table_data.append([
                Paragraph("<b>ИТОГО</b>", normal),
                "", "",
                f"{plan['Итого']:,.2f}"
            ])

            table = Table(table_data, colWidths=[300, 50, 70, 80], repeatRows=1)
            table.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("BACKGROUND", (0, -1), (-1, -1), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, -1), FONT_NAME),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ALIGN", (1, 1), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))

            block = [
                Paragraph(f"<b>{plan['План']}</b>", normal),
                Spacer(1, 4),
                Paragraph(f"Дата приёма: {plan.get('Дата') or '—'}", normal),
                Paragraph(f"Доктор проводивший приём: {plan.get('Доктор') or '—'}", normal),
                Spacer(1, 6),
                table,
                Spacer(1, 12),
            ]
            story.append(KeepTogether(block))
            story.append(Spacer(1, 12))

            total_all += plan["Итого"]

        story.append(Paragraph(
            f"<b>Общая выставленная сумма по согласованным планам: {total_all:,.2f} руб.</b>",
            normal
        ))
        paid_total = data.get("Общая оплаченная сумма по согласованным планам", 0)
        story.append(Paragraph(
            f"<b>Общая оплаченная сумма по согласованным планам: {paid_total:,.2f} руб.</b>",
            normal
        ))
    else:
        story.append(Paragraph("—", normal))


    # Генерация PDF
    doc.build(story)

    return report_path


def _build_patient_report(
    pcode: str,
    *,
    patient_data: Mapping[str, Any] | None = None,
    output_dir: Path | str | None = None,
    conn: Any | None = None,
    output_file: Path | str | None = None,
) -> Path:
    if patient_data is None:
        if conn is None:
            raise ValueError("Необходимо указать либо patient_data, либо conn")
        patient_data = collect_patient_data(conn, pcode)

    report_path = _resolve_report_path(pcode, output_dir, output_file)
    formatted = format_patient_data(dict(patient_data))
    return _render_report(formatted, report_path)


def build_patient_report(*args, **kwargs) -> Path:
    if not args:
        raise TypeError("в build_patient_report() отсутствует обязательный аргумент: 'pcode'")

    if not isinstance(args[0], str):
        if len(args) != 3:
            raise TypeError(
                "Устаревший вызов должен точно передавать позиционные аргументы (conn, pcode, output_file)"
            )

        conn, pcode, output_file = args
        if kwargs:
            raise TypeError("Устаревший вызов не принимает аргументы ключевого слова")
        return _build_patient_report(
            pcode,
            conn=conn,
            output_file=output_file,
        )

    pcode = args[0]
    remaining = args[1:]

    patient_data = kwargs.pop("patient_data", None)
    output_dir = kwargs.pop("output_dir", None)
    output_file = kwargs.pop("output_file", None)
    conn = kwargs.pop("conn", None)

    if kwargs:
        unexpected = ", ".join(sorted(kwargs.keys()))
        raise TypeError(f"Неожиданные аргументы ключевого слова: {unexpected}")

    if remaining:
        if patient_data is None:
            patient_data = remaining[0]
        else:
            raise TypeError("patient_data предоставляется как позиционно, так и в виде ключевого слова")

    if len(remaining) > 1:
        if output_dir is None:
            output_dir = remaining[1]
        else:
            raise TypeError("output_dir указывается как позиционно, так и в качестве ключевого слова")

    if len(remaining) > 2:
        raise TypeError("Слишком много позиционных аргументов для build_patient_report")

    return _build_patient_report(
        pcode,
        patient_data=patient_data,
        output_dir=output_dir,
        conn=conn,
        output_file=output_file,
    )
