import csv
import time
import requests
from pathlib import Path
from app.logging import get_logger
from app.config import Settings, load_non_secret_env

log = get_logger(__name__)

CONTACTS_CSV = Path("output/csv/processed_patients_personal_data.csv")
LEADS_CSV = Path("output/csv/processed_patients.csv")

# Настройки
load_non_secret_env()
settings = Settings()



def _read_csv(path: Path):
    #CSV в список словарей
    if not path.exists():
        log.warning(f"Файл {path} не найден, пропуск.")
        return []
    with open(path, encoding="cp1251") as f:
        reader = csv.DictReader(f, delimiter=";")
        return [row for row in reader]


def _api_call(url: str, data: dict) -> dict | None:
    # вызов Bitrix API
    if not url:
        log.error("URL API не указан.")
        return None

    try:
        response = requests.post(url, json=data, timeout=15)
        response.raise_for_status()
        res = response.json()
        if "error" in res:
            log.error(f"Ошибка Bitrix API: {res['error_description']}")
            return None
        return res
    except Exception as e:
        log.error(f"Ошибка вызова {url}: {e}")
        return None


def _get_contact(contact_id: str | None) -> dict | None:
    #Получение контакт по ID
    if not contact_id:
        return None
    result = _api_call(settings.BITRIX_CONTACT_GET_URL, {"id": contact_id})
    return result.get("result") if result else None


def _get_lead(lead_id: str | None) -> dict | None:
    # Получение лид по ID
    if not lead_id:
        return None
    result = _api_call(settings.BITRIX_LEAD_GET_URL, {"id": lead_id})
    return result.get("result") if result else None


def _link_lead_contact(lead_id: int, contact_id: int):
    #связь лида и контакта
    if not lead_id or not contact_id:
        return
    params = {"id": lead_id, "fields": {"CONTACT_ID": contact_id}}
    res = _api_call(settings.BITRIX_LEAD_CONTACT_ADD_URL, params)
    if res:
        log.info(f"Связь лида {lead_id} с контактом {contact_id} создана.")

def upload_contacts() -> list[dict]:
    # Создание и обновление контактов
    rows = _read_csv(CONTACTS_CSV)
    processed = []

    for r in rows:
        contact_id = r.get("ID")
        contact_exists = _get_contact(contact_id) if contact_id else None

        contact_data = {
            "NAME": r.get("Имя"),
            "LAST_NAME": r.get("Фамилия"),
            "SECOND_NAME": r.get("Отчество"),
            "BIRTHDATE": r.get("Дата рождения"),
            "PHONE": [{"VALUE": r.get("Телефон"), "VALUE_TYPE": "MOBILE"}],
            "EMAIL": [{"VALUE": r.get("Email"), "VALUE_TYPE": "WORK"}],
            "ADDRESS": r.get("Адрес"),
        }

        if contact_exists:
            url = settings.BITRIX_CONTACT_UPDATE_URL
            payload = {"id": contact_id, "fields": contact_data}
            log.info(f"Обновление контакта ID={contact_id}")
        else:
            url = settings.BITRIX_CONTACT_ADD_URL
            payload = {"fields": contact_data}
            log.info("Создание нового контакта")

        result = _api_call(url, payload)
        if result and result.get("result"):
            cid = contact_id or result["result"]
            processed.append({"ID": cid, **r})

        time.sleep(0.3)

    log.info(f"Контактов обработано: {len(processed)}")
    return processed

# Загрузка лидов
def upload_leads(contacts: list[dict]):
    # Создание и обновление лидов, привязка к контактам
    rows = _read_csv(LEADS_CSV)
    if not rows:
        return

    for r in rows:
        lead_id = r.get("ID")
        lead_exists = _get_lead(lead_id) if lead_id else None

        lead_data = {
            "TITLE": r.get("Название лида"),
            "NAME": r.get("Имя"),
            "LAST_NAME": r.get("Фамилия"),
            "SECOND_NAME": r.get("Отчество"),
            "UF_CRM_1758803186": r.get("Возраст пациента"),
            "UF_CRM_1847926521": r.get("ФИО консультанта пациента"),
            "UF_CRM_1880134790": r.get("Тип пациента 1"),
            "UF_CRM_1907342175": r.get("Тип пациента 2"),
            "UF_CRM_1723548903": r.get("Наличие/отсутствие снимка ОПТГ у пациента"),
            "UF_CRM_1765439800": r.get("ФИО доктора, проводившего первичный прием"),
            "UF_CRM_1932105698": r.get("Дата первого визита"),
            "UF_CRM_1876501327": r.get("Количество визитов в клинику"),
            "UF_CRM_1957123099": r.get("Дата следующего приема и ФИО доктора, к кому пациент записан на прием"),
            "UF_CRM_1925804455": r.get("Стоимость всех предварительных планов"),
            "UF_CRM_1857129080": r.get("Стоимость всех согласованных планов"),
            "UF_CRM_1739085210": r.get("Сумма оплаченных денег пациентом в клинику"),
            "UF_CRM_1777215408": r.get("Процент выполнения плана"),
            "UF_CRM_1802397666": r.get("Комплексный план"),
            "UF_CRM_1948803207": r.get("Стадия"),
            "UF_CRM_1709912534": r.get("Текущая стадия лечения"),
        }

        if lead_exists:
            url = settings.BITRIX_LEAD_UPDATE_URL
            payload = {"id": lead_id, "fields": lead_data}
            log.info(f"Обновление лида ID={lead_id}")
        else:
            url = settings.BITRIX_LEAD_ADD_URL
            payload = {"fields": lead_data}
            log.info("Создание нового лида")

        result = _api_call(url, payload)
        if result and result.get("result"):
            lid = lead_id or result["result"]

            # Привязка к контакту (по имени/фамилии)
            linked = next((c for c in contacts if c.get("Имя") == r.get("Имя") and c.get("Фамилия") == r.get("Фамилия")), None)
            if linked:
                _link_lead_contact(int(lid), int(linked["ID"]))

        time.sleep(0.3)

    log.info(f"Лидов обработано: {len(rows)}")

def main():
    log.info("=== Загрузка данных в Bitrix24 через REST API ===")
    contacts = upload_contacts()
    upload_leads(contacts)
    log.info("=== Загрузка завершена ===")


if __name__ == "__main__":
    main()
