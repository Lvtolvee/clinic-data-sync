from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait,Select
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import os

from app.config import load_non_secret_env, Settings
from app.logging import get_logger

log = get_logger(__name__)

contact_file_path = os.path.abspath("output/csv/processed_patients_personal_data.csv")
lead_file_path = os.path.abspath("output/csv/processed_patients.csv")
report_file_path = os.path.abspath("output/csv/management_report.xlsx")

def load_csv_to_bitrix(settings):

    login = settings.BITRIX_LOGIN
    password = settings.BITRIX_PASSWORD.get_secret_value() if settings.BITRIX_PASSWORD else None
    browser = settings.BROWSER
    main_url = settings.BITRIX_MAIN_URL
    contact_url = settings.BITRIX_IMPORT_CONTACT_URL
    lead_url = settings.BITRIX_IMPORT_LEAD_URL
    disk_url=settings.BITRIX_IMPORT_DISK_URL


    # Инициализация драйвера для браузера
    if browser == 'firefox':
        driver = webdriver.Firefox()
    elif browser == 'chrome':
        driver = webdriver.Chrome()
    else:
        raise ValueError("Unsupported browser")

    #Авторизация
    log.info(f"Выполняется подключение к Битрикс24")
    driver.get(main_url)
    wait = WebDriverWait(driver, 10)
    driver.find_element(By.XPATH, '//*[@id="login"]').send_keys(login)
    time.sleep(3)
    driver.find_element(By.XPATH,'//*[@class="b24net-text-btn b24net-text-btn--call-to-action ui-btn ui-btn-lg ui-btn-success b24net-login-enter-form__continue-btn"]').click()
    time.sleep(3)
    driver.find_element(By.XPATH, '//*[@type ="password"]').send_keys(password)
    time.sleep(3)
    driver.find_element(By.XPATH,'//*[@class="b24net-text-btn b24net-text-btn--call-to-action ui-btn ui-btn-lg ui-btn-success b24net-password-enter-form__continue-btn"]').click()
    time.sleep(3)
    log.info(f"Авторизация в Битрикс24 прошла успешно")


    # Загрузка файла персональных данных
    log.info(f"Загрузка персональной информации пациентов")
    driver.get(contact_url)
    wait = WebDriverWait(driver, 10)
    time.sleep(10)
    driver.find_element(By.CSS_SELECTOR, "input[type='file']").send_keys(contact_file_path)
    Select(driver.find_element(By.ID,'import_file_encoding')).select_by_value('windows-1251')
    driver.find_element(By.ID,'next').click()
    time.sleep(10)
    driver.find_element(By.NAME, 'next').click()
    time.sleep(10)
    driver.find_element(By.ID, 'dup_ctrl_replace').click()
    driver.find_element(By.NAME, 'next').click()
    time.sleep(10)
    log.info(f"Загрузка персональной информации пациентов прошла успешно")

    #Загрузка медицинской информации
    log.info(f"Загрузка медицинской информации пациентов")
    driver.get(lead_url)
    wait = WebDriverWait(driver, 10)
    time.sleep(10)
    driver.find_element(By.CSS_SELECTOR, "input[type='file']").send_keys(lead_file_path)
    Select(driver.find_element(By.ID, 'import_file_encoding')).select_by_value('windows-1251')
    driver.find_element(By.ID, 'next').click()
    time.sleep(10)
    driver.find_element(By.NAME, 'next').click()
    time.sleep(10)
    driver.find_element(By.ID, 'dup_ctrl_replace').click()
    driver.find_element(By.NAME, 'next').click()
    time.sleep(10)
    log.info(f"Загрузка медицинской информации пациентов прошла успешно")

    # Загрузка управленческого отчёта
    log.info(f"Загрузка управленческого отчёта")
    driver.get(disk_url)
    wait = WebDriverWait(driver, 10)
    time.sleep(10)
    driver.find_element(By.CSS_SELECTOR, "input[type='file']").send_keys(report_file_path)
    log.info(f"Отчёт отправлен")
    time.sleep(5)
    try:
        driver.find_element(By.CSS_SELECTOR, "[class='bx-disk-btn bx-disk-btn-small bx-disk-btn-gray mb0']").click()
        log.info("Кнопка 'Заменить' найдена и нажата.")
    except NoSuchElementException:
        log.info("Кнопка 'Заменить' не найдена — пропускаем клик.")
    time.sleep(5)
    driver.find_element(By.ID, 'FolderListButtonClose').click()
    log.info(f"Загрузка управленческого отчёта прошла успешно")





    time.sleep(100)
    driver.quit()

