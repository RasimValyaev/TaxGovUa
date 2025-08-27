import os
import time
from pathlib import Path
import keyboard
from dotenv import load_dotenv
import psutil
import pyautogui
import pyperclip
from dateutil import parser
from ping3 import ping
from pywinauto.application import Application
from pywinauto.findwindows import find_windows
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from AsyncPostgresql import con_postgres_psycopg2
from ChangeKeyBoard import set_keyboard_layout
from ScrapeWithLogs import get_bearer_token

# Импортируем модуль для автоматического обновления ChromeDriver
try:
    from ChromeDriverUpdater import update_chromedriver_if_needed
    # Проверяем и обновляем ChromeDriver при необходимости
    update_chromedriver_if_needed()
except Exception as e:
    print(f"Ошибка при обновлении ChromeDriver: {e}")

cur_dir = os.path.dirname(os.path.abspath(__file__))
pyautogui.FAILSAFE = False  # Отключаем защиту от выхода курсора за пределы экрана
chrome_driver_path = os.path.join(Path(cur_dir).parent.__str__(), "chromedriver-win64", "chromedriver.exe")
set_keyboard_layout()  # Меняем раскладку клавиатуры на английскую

# Настройка ChromeOptions
chrome_options = Options()

# Отключаем Google APIs и связанные сервисы для устранения ошибок API
chrome_options.add_argument("--disable-background-networking")
chrome_options.add_argument("--disable-background-timer-throttling")
chrome_options.add_argument("--disable-backgrounding-occluded-windows")
chrome_options.add_argument("--disable-breakpad")
chrome_options.add_argument("--disable-client-side-phishing-detection")
chrome_options.add_argument("--disable-component-extensions-with-background-pages")
chrome_options.add_argument("--disable-default-apps")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-features=TranslateUI")
chrome_options.add_argument("--disable-hang-monitor")
chrome_options.add_argument("--disable-ipc-flooding-protection")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--disable-prompt-on-repost")
chrome_options.add_argument("--disable-renderer-backgrounding")
chrome_options.add_argument("--disable-sync")
chrome_options.add_argument("--disable-web-security")
chrome_options.add_argument("--no-default-browser-check")
chrome_options.add_argument("--no-first-run")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")


# Отключаем логирование для уменьшения количества сообщений
chrome_options.add_argument("--log-level=3")
chrome_options.add_argument("--silent")

# Отключаем Google Cloud Messaging (GCM) для устранения PHONE_REGISTRATION_ERROR
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Отключаем уведомления и другие сервисы
prefs = {
    "profile.default_content_setting_values.notifications": 2,
    "profile.default_content_settings.popups": 0,
    "profile.managed_default_content_settings.images": 2,
    "credentials_enable_service": False,
    "profile.password_manager_enabled": False
}
chrome_options.add_experimental_option("prefs", prefs)

load_dotenv()

# Путь к драйверу Chrome
service = Service(chrome_driver_path)


def close_dialog_if_open():
    try:
        # Подключение к приложению (замените 'chrome.exe' на имя вашего браузера или приложения)
        app = Application().connect(path=chrome_driver_path)

        # Получение окна диалога (замените 'Открытие' на заголовок вашего окна диалога)
        dialog = app.window(title="Открытие")

        # Закрытие окна диалога, если оно открыто
        if dialog.exists():
            dialog.close()
            print("Диалоговое окно 'Открытие' закрыто.")
    except Exception as e:
        pass
        # print(f"Ошибка при закрытии диалогового окна: {e}")


def refresh_screen(driver):
    try:
        driver.refresh()
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        # print("Screen refreshed successfully.")
    except Exception as e:
        # print(f"Error refreshing screen: {e}")
        pass


def site_is_available(driver):
    try:
        driver.get("https://cabinet.tax.gov.ua/login")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        return True
    except Exception as e:
        # print(f"Error: {e}")
        return False


def get_token(driver=None, token=None, count=1):

    # очищаем cash и cookies
    if driver:
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")


    # если открыто окно выбора файла, то закрываем его
    close_dialog_if_open()

    while (not driver or not token) and count < 5:
        if not driver:
            try:
                driver.quit()
            except Exception as e:
                pass

            # Запускаем браузер
            driver = webdriver.Chrome(service=service, options=chrome_options)
            if site_is_available(driver):
                authorize(driver)

        # refresh_screen(driver)
        token = get_bearer_token(driver)
        if token:
            return driver, token

        # Если токен не получен, возможно слетела авторизация.
        # Закрываем браузер и повторно запускаем
        # надо именно закрыть браузер, иначе будет просто страница обновляться
        if not driver:
            try:
                driver.quit()
                driver = None
            except Exception as e:
                pass
        time.sleep(5 * count)
        count += 1

    return None, None


# Функция для получения данных с текущей страницы
def get_table_data(driver, page_number, table_xpath):
    rows = driver.find_elements(By.XPATH, table_xpath)
    data_all = []
    for row in rows:
        data = []
        cells = row.find_elements(By.TAG_NAME, "td")
        for cell in cells[1:]:  # Проходимся по всем элементам, кроме последнего
            try:
                data.append(cell.text)
                # print(f"cells'{cell.text}'")
            except StaleElementReferenceException:
                # Повторно находим элемент, если он устарел
                cell = driver.find_element(By.XPATH, cell.get_attribute("xpath"))
                data.append(cell.text)
                # print(cell.text)
        # print("-" * 20)  # Разделитель между строками
        if data:
            data.insert(0, page_number)
            data_all.append(data)

    time.sleep(1)  # Небольшая задержка для стабильности
    return data_all


def get_table_data_all(driver, table_xpath="//tbody[@class='p-datatable-tbody']/tr"):
    # Ожидание, пока элементы таблицы станут видимыми
    wait = WebDriverWait(driver, 10)

    while True:
        try:
            # Находим строки таблицы по XPATH с ожиданием, пока они станут доступными
            rows = wait.until(
                EC.presence_of_all_elements_located((By.XPATH, table_xpath))
            )

            for i in range(len(rows)):
                # Нужно снова искать элементы после каждой итерации, т.к. элементы могут устареть
                rows = wait.until(
                    EC.presence_of_all_elements_located((By.XPATH, table_xpath))
                )
                row = rows[i]

                # Находим все ячейки в строке
                cells = row.find_elements(By.TAG_NAME, "td")

                for cell in cells:
                    try:
                        # Ваш метод обработки данных из ячейки
                        find_and_save_receipts(driver, cell)
                    except StaleElementReferenceException:
                        # Повторно находим ячейки в строке, если элемент устарел
                        # print("Элемент устарел, повторный поиск")
                        rows = wait.until(
                            EC.presence_of_all_elements_located((By.XPATH, table_xpath))
                        )
                        row = rows[i]
                        cells = row.find_elements(By.TAG_NAME, "td")
                        cell = cells[cells.index(cell)]  # Возвращаемся к текущей ячейке
                        find_and_save_receipts(driver, cell)
                    except Exception as e:
                        # print(f"Error: {e}")
                        pass

                # print("-" * 20)  # Разделитель между строками

                # Ожидание загрузки страницы после возврата
                wait.until(EC.presence_of_all_elements_located((By.XPATH, table_xpath)))
                time.sleep(1)  # Небольшая задержка для стабильности

            break  # Останавливаем цикл, когда все строки обработаны

        except Exception as e:
            # print(f"Ошибка при загрузке таблицы или обработке данных: {e}")
            time.sleep(1)
            if driver.current_url != "https://cabinet.tax.gov.ua/documents/in":
                driver.back()

            break


# Функция для клика по элементу с использованием XPath
def click_element_by_xpath(driver, xpath):
    try:
        element = driver.find_element(By.XPATH, xpath)
        driver.execute_script("arguments[0].click();", element)
        # Ожидание появления и кликабельности элемента
        element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))

        # Скроллинг элемента в видимую область с помощью JavaScript
        driver.execute_script("arguments[0].scrollIntoView(true);", element)  # Прокручиваем элемент в видимую область
        element.click()

        # Ожидание нового состояния страницы
        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='new-content']"))
        )

    except ElementClickInterceptedException:
        # print(
        #     f"ElementClickInterceptedException: Элемент с XPath {xpath} перекрыт другим элементом. повторяем клик."
        # )
        time.sleep(1)  # Небольшая задержка для стабильности

        # Повторно находим элемент и кликаем на него с помощью JavaScript
        # element = driver.find_element(By.XPATH, xpath)
        # Если элемент перекрыт другим элементом, выполняем повторную попытку клика
        time.sleep(1)  # Небольшая задержка
        element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath)))
        driver.execute_script("arguments[0].click();", element)

    except TimeoutException:
        # print(f"TimeoutException: Элемент с XPath {xpath} не кликабелен.")
        pass
    except NoSuchElementException:
        # print(f"NoSuchElementException: Элемент с XPath {xpath} не найден.")
        pass
    except StaleElementReferenceException:
        # print(
        #     f"StaleElementReferenceException: Элемент с XPath {xpath} устарел. Повторный поиск элемента."
        # )
        click_element_by_xpath(driver, xpath)  # Повторный вызов функции для поиска элемента
    except Exception as e:
        # print(f"Error: {e}")
        pass


def authorize(driver=None):
    try:
        if ping("tax.gov.ua") is None:
            print("Сайт tax.gov.ua недоступен.")
            return False

        if not driver:
            # Запускаем браузер
            driver = webdriver.Chrome(service=service, options=chrome_options)

        # Максимизируем окно браузера
        driver.maximize_window()
        url_login = "https://cabinet.tax.gov.ua/login"

        # Открываем сайт
        driver.get(url_login)
        while not driver.current_url == url_login:
            refresh_screen(driver)
            driver.get(url_login)
            pyautogui.hotkey("esc")
            time.sleep(1)

        # Ожидаем, пока блокирующий элемент исчезнет
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.CLASS_NAME, "p-blockui-document"))
        )
        time.sleep(2)

        while True:
            pyautogui.hotkey("esc")

            # Находим элемент dropdown и кликаем по нему, чтобы открыть список
            authorized_center_path = '//*[@id="selectedCAs111"]/div/span'

            # Ожидание элемента выпадающего списка с ID 'selectedCAs111' и клик по нему
            dropdown = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, authorized_center_path))
            )
            dropdown.click()
            click_element_by_xpath(
                driver, '//span[text()=\'КНЕДП ТОВ "Центр сертифікації ключів "Україна"\']'
            )
            pyautogui.hotkey("esc")
            time.sleep(2)

            # вводим путь к файлу
            key_path = '//*[@id="keyStatusPanel"]/div/div[3]/div[2]/div/div/input'

            while True:
                pyautogui.hotkey("esc")
                # Находим кнопку и кликаем по ней
                button = driver.find_element(By.XPATH, key_path)
                button.click()

                # Ожидаем появления окна выбора файла
                # Используем pyperclip для копирования пути к файлу в буфер обмена и pyautogui для вставки
                cur_dir = os.path.dirname(os.path.abspath(__file__))
                file_name = os.getenv("TAX_GOV_UA_FILENAME")
                file_path = os.path.join(cur_dir, file_name)

                # Копируем путь к файлу в буфер обмена
                pyperclip.copy(file_path)
                time.sleep(1)  # Небольшая задержка для стабильности

                clipboard_content = pyperclip.paste()
                if file_path == clipboard_content:
                    time.sleep(1)
                    break

            time.sleep(1)  # Дополнительная задержка перед вставкой
            # pyautogui.hotkey("ctrl", "v")
            keyboard.press_and_release('ctrl+v')
            time.sleep(1)
            pyautogui.press("enter")

            pyautogui.hotkey("esc")
            # Закрытие диалогового окна
            # dialog.close()

            # Находим поле для ввода пароля и вводим пароль
            psw_path = '//*[@id="keyStatusPanel"]/div/div[3]/div[4]/div/div/input'
            password_input = driver.find_element(By.XPATH, psw_path)
            password_input.send_keys(os.getenv("TAX_GOV_UA_PASSWORD"))

            value_key = get_input_value(driver, key_path)
            value_psw = get_input_value(driver, psw_path)
            if (dropdown.text == 'КНЕДП ТОВ "Центр сертифікації ключів "Україна"'
                    and value_key == os.getenv("TAX_GOV_UA_FILENAME")
                    and value_psw == os.getenv("TAX_GOV_UA_PASSWORD")):
                break
            pyautogui.hotkey("esc")
            pyautogui.hotkey("enter")
            refresh_screen(driver)
            time.sleep(2)

        # Находим кнопку "Зчитати" и кликаем по ней
        read_button = driver.find_element(By.XPATH, "//span[text()='Зчитати']")
        read_button.click()

        # Ожидаем пока прочитается файл
        time.sleep(5)

        # Находим кнопку "Увійти" и кликаем по ней
        login_button = driver.find_element(By.XPATH, "//span[text()='Увійти']")
        login_button.click()
        time.sleep(1)
        return True
    except NoSuchElementException as e:
        # print(f"страницы закончились: {e}")
        pass

    except Exception as e:
        # print(e)
        pass

    return False


def authorizations(driver):
    while True:
        if authorize(driver):
            break


def get_max_page_number(driver=None):

    if not driver:
        # Запуск авторизации
        authorizations(driver)

    # Максимизируем окно браузера
    driver.maximize_window()

    try:
        driver.get("https://cabinet.tax.gov.ua/documents/in")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".p-paginator-icon.pi.pi-angle-double-right")
            )
        )

        # Кликаем на элемент для перехода на последнюю страницу
        last_page_button = driver.find_element(
            By.CSS_SELECTOR, ".p-paginator-icon.pi.pi-angle-double-right"
        )
        last_page_button.click()

        # Ожидаем загрузки последней страницы
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".p-paginator-page.p-highlight")
            )
        )

        # Получаем номер последней страницы
        max_page_number = int(
            driver.find_element(By.CSS_SELECTOR, ".p-paginator-page.p-highlight").text
        )
        print(f"Максимальный номер страницы: {max_page_number}")
        return max_page_number

    except Exception as e:
        # print(f"Ошибка: {e}")
        pass

    finally:
        driver.quit()

    return 0


def go_to_next_page(driver, page_xpath):
    try:
        # Ожидание появления и кликабельности кнопки "Следующая страница"
        next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, page_xpath)))

        # Получаем номер последней страницы
        pagination_info = driver.find_element(By.CSS_SELECTOR, ".p-paginator-pages")
        last_page_number = int(pagination_info.find_elements(By.TAG_NAME, "button")[-1].text)

        if next_button.is_enabled():
            try:
                next_button.click()
            except ElementClickInterceptedException:
                print(
                    "ElementClickInterceptedException: Кнопка перекрыта другим элементом. Выполняем клик с помощью JavaScript."
                )
                driver.execute_script("arguments[0].click();", next_button)
            return True, last_page_number
    except NoSuchElementException:
        print(
            "NoSuchElementException: Кнопка перехода на следующую страницу не найдена."
        )
    except TimeoutException:
        print("TimeoutException: Кнопка перехода на следующую страницу не кликабельна.")
    except StaleElementReferenceException:
        print(
            "StaleElementReferenceException: Элемент устарел. Повторный поиск элемента."
        )
        return go_to_next_page(page_xpath)  # Повторный вызов функции для поиска элемента
    except Exception as e:
        print(f"Error: {e}")

    return False, None


def go_to_page(driver, page_number):
    try:
        while True:
            # Пример использования WebDriverWait для ожидания текущего номера страницы и получения его текста
            current_page = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".p-paginator-page.p-highlight"))
            ).text
            if int(current_page) == page_number:
                return True

            # Кликаем на кнопку перехода на следующую страницу
            button_path = "//button[@class='p-paginator-next p-paginator-element p-link p-ripple']"
            next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, button_path,)))
            next_button.click()
            time.sleep(0.5)  # Небольшая задержка для стабильности

    except Exception as e:
        # print(f"Error: {e}")
        return False


def extract_doc_info(doc_text):
    import re

    match = re.search(r"№ (\d+) від (\d{2}\.\d{2}\.\d{4})", doc_text)
    if match:
        date = parser.parse(match.group(2)).strftime("%Y.%m.%d")
        return f"{match.group(1)} {date}"
    return None


def save_receipts(driver, file_path, butthon_xpath=None):
    try:
        # на всякий случай закрываем окно если не закрылось с предыдущего раза
        pyautogui.press("escape")
        close_window_save_as(driver)

        # Удаляем файл, если он уже существует
        # file_path = os.path.join(cur_name, f"{doc_name}")
        if os.path.exists(file_path):
            os.remove(file_path)

        time.sleep(1)

        # Находим кнопку "XML/PDF" и кликаем по ней
        click_element_by_xpath(driver, butthon_xpath)

        # Копируем путь к файлу в буфер обмена
        pyperclip.copy(file_path)
        print(f"file_path: {file_path}")
        time.sleep(1)

        # Вставляем путь из буфера обмена
        pyautogui.hotkey("ctrl", "v")
        time.sleep(1)
        pyautogui.press("enter")
        time.sleep(1)

        # Переключаемся на новое окно, если оно открыто
        original_window = driver.current_window_handle
        for handle in driver.window_handles:
            if handle != original_window:
                driver.switch_to.window(handle)
                break

        # Возвращаемся в исходное окно
        driver.switch_to.window(original_window)

    except Exception as e:
        # print(e)
        pass


def is_saved_file_exists(file_path):
    # Проверяем, что файл был сохранен
    if os.path.exists(file_path):
        print(f"File {file_path} saved successfully.")
        return True
    else:
        print(f"File {file_path} not saved.")
    return False


def find_and_save_receipts(driver, cell):
    try:
        if "РIШЕННЯ" in cell.text.upper() or "ДОДАТОК" in cell.text.upper():
            cell.click()
            save_to_path = os.path.join(cur_dir, "downloads")

            # Находим кнопку "Перегляд" и кликаем по ней
            click_element_by_xpath(driver, "//span[@class='p-button-label' and text()='Перегляд']")

            time.sleep(1)

            # Получаем информацию о документе. Будем использовать ее для наименования файла
            button_parh = "/html/body/app-root/div/div[2]/div[2]/app-in-view/div[2]/p"
            element = driver.find_element(By.XPATH, button_parh)

            # Извлекаем информацию об имени документе
            doc_info = extract_doc_info(element.text)
            # на всякий случай закрываем окно если не закрылось с предыдущего раза
            pyautogui.press("escape")

            # Находим кнопку "XML" и кликаем по ней
            button_path = "//span[@class='p-button-label' and text()='XML']"
            click_element_by_xpath(driver, button_path)
            time.sleep(1)

            # Возвращаемся на предыдущую страницу, в случае если мы находимся на странице с документом
            driver.back()
    except Exception as e:
        # print(f"find_and_save_receipts. Error: {e}")
        pass


def close_window_save_as(driver):
    # Задержка, чтобы дать время на появление окна "Сохранить как"
    time.sleep(5)

    # Получаем PID процесса браузера (можно получить через driver.process_id, если это поддерживается)
    browser_pid = driver.service.process.pid

    # Используем psutil для поиска всех дочерних процессов браузера (например, в Chrome может быть несколько процессов)
    browser_process = psutil.Process(browser_pid)
    child_processes = browser_process.children(recursive=True)  # Все дочерние процессы браузера

    # Получаем PID всех процессов браузера
    all_pids = [proc.pid for proc in child_processes] + [browser_pid]

    # Ищем все окна с классом "#32770" (например, окна "Сохранить как")
    try:
        save_as_windows = find_windows(class_name="#32770")

        # Проходим по каждому найденному окну
        for handle in save_as_windows:
            # Подключаемся к окну
            app = Application(backend="uia").connect(handle=handle)
            window = app.window(handle=handle)

            # Проверяем, соответствует ли процесс окна процессу браузера
            if window.process_id() in all_pids:
                # Если окно связано с текущим процессом браузера, закрываем его
                window.close()
                # print(f"Окно с handle {handle} закрыто.")

    except Exception as e:
        # print(f"Ошибка: {e}")
        pass


# Пример функции для получения названия активного поля
def get_active_element_name(driver):
    active_element = driver.execute_script("return document.activeElement;")
    element_name = active_element.get_attribute("name")
    element_placeholder = active_element.get_attribute("placeholder")
    element_id = active_element.get_attribute("id")
    print(f"Название поля: {element_name}")
    print(f"Placeholder поля: {element_placeholder}")
    print(f"ID поля: {element_id}")
    return element_name, element_placeholder, element_id


# Пример использования функции для получения значения поля ввода
def get_input_value(driver, xpath):
    try:
        input_element = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        return input_element.get_attribute("value")
    except Exception as e:
        # print(f"Error: {e}")
        return None


def check_ping(host):
    # pip install ping3

    response = ping(host)
    if response is None:
        print(f"Сайт {host} недоступен.")
        return False
    else:
        print(f"Сайт {host} доступен. Время отклика: {response} секунд.")
        return True


def remove_duplicates(table_name):
    sql = f"SELECT fn_remove_duplicates('{table_name}');"
    conn = con_postgres_psycopg2()
    result = False
    # Выполнение запроса на удаление дубликатов
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        result = True

    # Закрытие соединения
    conn.close()
    return result


if __name__ == "__main__":
    # authorize()
    if check_ping("tax.gov.ua"):
        print(authorize())
