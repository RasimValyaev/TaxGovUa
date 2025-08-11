# -*- coding: utf-8 -*-

"""
pdf_downloader_edin.py

https://aistudio.google.com/prompts/1Lg9dAwkX7dal-giGK1L7M6ewq8mtxWuB

Скрипт для массового скачивания и обработки документов из системы EDIN.

Основные возможности:
1. Получение списка партнеров (GLN, ЕГРПОУ) через API EDIN.
2. Поиск и скачивание всех входящих и исходящих документов за указанный период.
3. Сохранение метаданных документов в PostgreSQL.
4. Скачивание PDF-документов, формирование структуры папок и Excel-отчетов.
5. Обработка ошибок и логирование проблемных документов.

Финальная версия скрипта.
Функционал:
1.  Реализован двухшаговый, надежный механизм получения списка партнеров:
    - Сначала получается полный список GLN через RETAILERS_URL.
    - Затем для каждого партнера делается точечный запрос к IDENTIFIERS_URL для получения ЕГРПОУ.
2.  В цикле для каждого контрагента выполняет поиск ВСЕХ входящих и исходящих
    документов всех типов за указанный период.
3.  Сохраняет метаданные всех найденных документов в PostgreSQL.
4.  Опционально (в зависимости от флага save_to_pdf) скачивает PDF-версии
    документов и создает отчеты.
    - ИМЯ ПАПКИ КЛИЕНТА: Формируется как: ЕГРПОУ (приоритет) или GLN.
    - СТРУКТУРА ПАПОК: КодКлиента/ГГГГММ/ТипДокумента/файл.pdf
    - НАДЕЖНОЕ ИМЕНОВАНИЕ ФАЙЛА: Формат "ТипДокумента №Номер від дд мм гггг.pdf".
    - EXCEL-ОТЧЕТ: Для каждого клиента создается свой Excel-отчет в его папке.
    - УСТРАНЕНИЕ ДУБЛИКАТОВ: Проверка дублей по ключу (Тип + Номер + Дата).
5.  При возникновении ошибки во время обработки контрагента, сохраняет
    список проблемных документов в файл error_ГГГГММДД_ЧЧММСС.json для анализа.


Требования:
- requests
- python-dotenv
- pandas
- psycopg2-binary
- PyMuPDF (fitz)
- openpyxl
"""


import requests  # Работа с HTTP-запросами
import os  # Операции с файловой системой
import datetime  # Работа с датой и временем
import logging  # Логирование событий
import re  # Регулярные выражения для обработки строк
import json  # Работа с JSON
from dotenv import load_dotenv  # Загрузка переменных окружения из .env
import pandas as pd  # Работа с таблицами и Excel
import psycopg2  # Работа с PostgreSQL
from psycopg2 import sql  # Динамическое формирование SQL-запросов
import fitz  # Импорт PyMuPDF для работы с PDF
import base64
from GeminiDocSignNo import extract_entity_by_gemini


# --- 1. Конфигурация и загрузка переменных окружения ---
load_dotenv()  # Загружаем переменные окружения из .env файла


# --- Учетные данные для API и БД ---
EDI_LOGIN = os.getenv("EDI_LOGIN")  # Логин для EDIN
EDI_PASSWORD = os.getenv("EDI_PASSWORD")  # Пароль для EDIN
SENDER_GLN = os.getenv("EDI_GLN")  # GLN отправителя
PG_USER = os.getenv("PG_USER")  # Пользователь PostgreSQL
PG_PASSWORD = os.getenv("PG_PASSWORD")  # Пароль PostgreSQL
PG_HOST_LOCAL = os.getenv("PG_HOST_LOCAL")  # Хост PostgreSQL
PG_PORT = os.getenv("PG_PORT")  # Порт PostgreSQL
PG_DBNAME = os.getenv("PG_DBNAME")  # Имя БД PostgreSQL

# Проверка наличия всех необходимых переменных окружения
if not all([EDI_LOGIN, EDI_PASSWORD, SENDER_GLN, PG_USER, PG_PASSWORD, PG_HOST_LOCAL, PG_PORT, PG_DBNAME]):
    raise ValueError("Ошибка: Не все переменные окружения заданы в .env файле.")


# --- URL-адреса API EDIN ---
AUTH_URL = "https://edo-v2.edin.ua/api/authorization/hash"  # Авторизация
SEARCH_URL = f"https://edo-v2.edin.ua/api/eds/docs/search?gln={SENDER_GLN}&family=edi"  # Поиск документов
DOWNLOAD_URL_TEMPLATE = "https://edo-v2.edin.ua/api/eds/doc/download"  # Скачивание PDF
RETAILERS_URL = "https://edo-v2.edin.ua/api/oas/allretailers"  # Список партнеров
IDENTIFIERS_URL = "https://edo-v2.edin.ua/api/oas/identifiers"  # Детали партнера


# --- Настройка логгирования ---
LOG_FILENAME = "download_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler(LOG_FILENAME, 'w', 'utf-8'),  # Лог в файл
        logging.StreamHandler()  # Лог в консоль
    ]
)


def extract_title_from_pdf(pdf_content):
    """
    Извлекает заголовок документа из первой страницы PDF.
    Используется для формирования уникального имени файла.
    Возвращает строку с заголовком или None, если не удалось извлечь.
    """
    try:
        pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
        if pdf_document.page_count > 0:
            first_page = pdf_document[0]
            text = first_page.get_text("text")
            index = text.find('№')
            if index != -1:
                text_before_number = text[:index]
                lines = [line.strip() for line in text_before_number.splitlines() if line.strip()]
                if lines:
                    title = lines[-1]
                    pdf_document.close()
                    return title
        pdf_document.close()
    except Exception as e:
        logging.warning(f"Не удалось проанализировать PDF для извлечения заголовка: {e}")
    return None


def sanitize_filename(name):
    """
    Заменяет недопустимые символы в имени файла/папки на '_'.
    """
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def get_db_connection():
    """
    Устанавливает соединение с PostgreSQL.
    Возвращает объект соединения или None при ошибке.
    """
    try:
        conn = psycopg2.connect(
            dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD,
            host=PG_HOST_LOCAL, port=PG_PORT
        )
        logging.info("Успешное подключение к PostgreSQL.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Ошибка подключения к PostgreSQL: {e}")
        return None


def flatten_json(y):
    """
    Преобразует вложенный JSON в плоский словарь для удобной записи в БД.
    """
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x: flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def save_document_to_db(cursor, document_data):
    """
    Сохраняет или обновляет метаданные документа в таблице t_edin_documents.
    Использует upsert (ON CONFLICT) по doc_id.
    """
    table_name = "t_edin_documents"
    flat_data = flatten_json(document_data)
    if 'doc_id' not in flat_data:
        logging.warning(f"Пропуск сохранения в БД: отсутствует doc_id в документе {document_data.get('doc_uuid')}")
        return
    try:
        columns = flat_data.keys()
        values = [flat_data[key] for key in columns]
        update_assignments_list = [
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(k), sql.Identifier(k))
            for k in columns if k != 'doc_id'
        ]
        update_assignments_list.append(sql.SQL("updated_at = NOW()"))
        update_assignments = sql.SQL(', ').join(update_assignments_list)
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (doc_id) DO UPDATE SET {}"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(values)),
            update_assignments
        )
        cursor.execute(insert_query, values)
        if cursor.rowcount > 0:
            logging.info(f"Данные для doc_id={flat_data['doc_id']} успешно вставлены/обновлены в БД.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении doc_id={flat_data.get('doc_id')} в БД: {e}")
        raise


def dump_error_json(documents_to_dump):
    """
    Сохраняет список проблемных документов в отдельный JSON-файл с меткой времени.
    """
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    error_filename = f"error_{timestamp_str}.json"
    logging.info(f"Сохранение проблемных данных в файл: {error_filename}")
    try:
        with open(error_filename, 'w', encoding='utf-8') as f:
            json.dump(documents_to_dump, f, ensure_ascii=False, indent=4)
        logging.info("Файл с ошибкой успешно сохранен.")
    except Exception as e:
        logging.error(f"Не удалось сохранить файл с ошибкой {error_filename}: {e}")


def get_sid(session, login, password):
    """
    Получает SID (токен авторизации) для работы с API EDIN.
    Возвращает SID или None при ошибке.
    """
    logging.info("Шаг 1: Получение SID...")
    payload = {'email': login, 'password': password}
    try:
        response = session.post(AUTH_URL, data=payload)
        response.raise_for_status()
        sid = response.json().get('SID')
        if not sid:
            raise ValueError("SID не найден в ответе сервера. Проверьте учетные данные.")
        logging.info("SID успешно получен.")
        session.headers.update({'Authorization': sid})
        return sid
    except Exception as e:
        logging.error(f"Критическая ошибка при авторизации: {e}")
        return None


def get_documents(session, direction_payload, start_date, end_date):
    """
    Получает список документов по направлению (входящие/исходящие) за указанный период.
    Возвращает список документов или None при ошибке.
    """
    try:
        start_ts = int(datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59).timestamp())
    except ValueError:
        logging.error("Формат даты неверный. Используйте 'YYYY-MM-DD'.")
        return None
    search_payload = {
        "statuses": [], "type": [], "limit": {"offset": "0", "count": "1000000"},
        "exchangeStatus": [], "extraParams": [], "tags": [], "loadTags": True,
        "multiExtraParams": [], "archive": False, "direction": direction_payload,
        "docDate": {"startTimestamp": start_ts, "finishTimestamp": end_ts},
        "loadChain": True, "families": [1, 7, 8]
    }
    try:
        response = session.post(SEARCH_URL, json=search_payload)
        response.raise_for_status()
        documents = response.json().get("items", [])
        return documents
    except Exception as e:
        logging.error(f"Ошибка при поиске документов: {e}")
        return None


def sign_exists_in_pdf(pdf_content_bytes):
    base64_string = base64.b64encode(pdf_content_bytes).decode('utf-8')
    result = extract_entity_by_gemini('', base64_string)
    if result and isinstance(result, dict):
        return result.get('sign', False)
    return False


def process_documents(session, cursor, documents, save_to_pdf, client_folder_path):
    """
    Обрабатывает список документов:
    - Сохраняет метаданные в БД
    - Скачивает PDF (если save_to_pdf=True)
    - Формирует Excel-отчет и лог-файл
    Возвращает количество скачанных файлов, список имен и данные для отчета.
    """
    if not documents:
        return 0, [], []
    downloaded_count = 0
    downloaded_filenames = []
    excel_report_data = []
    seen_excel_entries = set()

    for doc in documents:
        save_document_to_db(cursor, doc)

        if save_to_pdf:
            doc_uuid = doc.get('doc_uuid')
            generic_doc_type_desc = doc.get('type', {}).get('description', 'Без типа')
            doc_number = doc.get('docNumber', 'Без номера')
            doc_date_ts = doc.get('docDate')
            if not all([doc_uuid, doc_date_ts]):
                logging.warning(f"Пропуск скачивания PDF: не хватает данных. Doc ID: {doc.get('doc_id')}")
                continue

            logging.info(f"Загрузка PDF для doc_uuid: {doc_uuid}...")
            pdf_response = session.get(DOWNLOAD_URL_TEMPLATE, params={'gln': SENDER_GLN, 'doc_uuid': doc_uuid, 'format': 'pdf'})
            pdf_response.raise_for_status()
            pdf_content = pdf_response.content

            extracted_title = extract_title_from_pdf(pdf_content)
            final_doc_title = extracted_title or generic_doc_type_desc
            
            dt_object = datetime.datetime.fromtimestamp(doc_date_ts)
            dd_mm_yyyy_str_for_file = dt_object.strftime('%d %m %Y')
            dd_mm_yyyy_str_for_excel = dt_object.strftime('%d.%m.%Y')

            excel_key = (final_doc_title, doc_number, dd_mm_yyyy_str_for_excel)

            if excel_key in seen_excel_entries:
                logging.info(f"Пропуск дубликата для отчета и файла: {excel_key}")
                continue
            
            seen_excel_entries.add(excel_key)
            
            logging.info(f"Определен тип документа: '{final_doc_title}' (уникальный)")

            yyyymm_folder_name = dt_object.strftime('%Y%m')
            date_folder = os.path.join(client_folder_path, yyyymm_folder_name)
            type_folder = os.path.join(date_folder, sanitize_filename(final_doc_title))
            os.makedirs(type_folder, exist_ok=True)

            file_name_base = f"{final_doc_title} №{doc_number} від {dd_mm_yyyy_str_for_file}"
            
            # Проверка подписи/печати покупателя о получении товара
            is_signed = sign_exists_in_pdf(pdf_content)
            if not is_signed:  # and trim(final_doc_title.lower()) == 'видаткова накладна':
                file_name_base = file_name_base + " NoSign"  # NoSign - нет подписи покупателя о получении товара
            
            file_name = f"{sanitize_filename(file_name_base)}.pdf"
            full_path = os.path.join(type_folder, file_name)

            logging.info(f"Сохранение PDF: {full_path}")
            with open(full_path, 'wb') as f:
                f.write(pdf_content)
            downloaded_count += 1
            downloaded_filenames.append(file_name)

            excel_report_data.append({
                'Тип документа': final_doc_title,
                'Дата': dd_mm_yyyy_str_for_excel,
                'Номер': doc_number
            })
            logging.info(f"Успешно сохранен PDF: {full_path}")

    return downloaded_count, downloaded_filenames, excel_report_data


def create_filenames_log(client_folder_path, filenames):
    """
    Создает лог-файл с перечнем всех скачанных PDF-документов для клиента.
    """
    if not filenames: return
    log_path = os.path.join(client_folder_path, "documents.log")
    logging.info(f"Создание лог-файла 'documents.log' в папке {client_folder_path}...")
    filenames.sort()
    try:
        with open(log_path, 'w', encoding='utf-8') as f:
            for name in filenames: f.write(name + '\n')
        logging.info(f"Файл 'documents.log' успешно создан.")
    except IOError as e:
        logging.error(f"Не удалось создать файл 'documents.log': {e}")


def create_client_excel_report(report_data, client_folder_path, client_name, start_date, end_date):
    """
    Формирует Excel-отчет по скачанным документам для клиента.
    """
    if not report_data:
        logging.info(f"Нет данных для создания Excel отчета для клиента {client_name}.")
        return

    sanitized_name = sanitize_filename(client_name)
    excel_filename = f"Report_{sanitized_name}_{start_date}_to_{end_date}.xlsx"
    full_path = os.path.join(client_folder_path, excel_filename)

    logging.info(f"Создание Excel-отчета для клиента: {full_path}")
    try:
        df = pd.DataFrame(report_data)
        df = df[['Тип документа', 'Дата', 'Номер']]
        df.to_excel(full_path, index=False, engine='openpyxl')
        logging.info(f"Excel-отчет '{excel_filename}' успешно создан.")
    except Exception as e:
        logging.error(f"Не удалось создать Excel-отчет для клиента {client_name}: {e}")


def _get_base_partner_list(session):
    """
    Получает базовый список партнеров с их GLN через API EDIN.
    """
    logging.info("Шаг 2.1: Получение базового списка GLN всех партнеров...")
    try:
        response = session.get(RETAILERS_URL)
        response.raise_for_status()
        base_partners = response.json()
        logging.info(f"Найдено базовых партнеров: {len(base_partners)}")
        return base_partners
    except Exception as e:
        logging.error(f"Не удалось получить базовый список партнеров: {e}")
        return []


def _get_partner_details(session, partner_gln, sender_gln):
    """
    Получает детальную информацию о партнере (ЕГРПОУ и др.) по GLN.
    """
    params = {'gln': sender_gln, 'query': partner_gln}
    try:
        response = session.get(IDENTIFIERS_URL, params=params)
        response.raise_for_status()
        results = response.json()
        if results and isinstance(results, list):
            return results[0]
        elif results and isinstance(results, dict):
            return results
    except Exception as e:
        logging.warning(f"Не удалось получить детали для GLN {partner_gln}: {e}")
    return None


def get_all_partners_with_details(session, sender_gln):
    """
    Получает полный список партнеров и обогащает их детальной информацией (ЕГРПОУ и др.).
    """
    base_partners = _get_base_partner_list(session)
    if not base_partners:
        return []
    
    logging.info("Шаг 2.2: Обогащение данных по каждому партнеру (получение ЕГРПОУ)...")
    detailed_partners = []
    for partner in base_partners:
        if not isinstance(partner, dict):
            logging.warning(f"Пропуск некорректной записи в базовом списке: {partner}")
            continue
        
        partner_gln = partner.get('gln')
        if not partner_gln:
            continue
            
        details = _get_partner_details(session, partner_gln, sender_gln)
        if details:
            detailed_partners.append(details)
        else:
            logging.warning(f"Детали для партнера {partner.get('name')} (GLN: {partner_gln}) не найдены. Будут использованы базовые данные.")
            detailed_partners.append(partner)
            
    logging.info(f"Готово к обработке партнеров с детальной информацией: {len(detailed_partners)}")
    return detailed_partners


def main(start_date, end_date, save_to_pdf, client_identifier=None):
    """
    Основная функция-оркестратор:
    - Получает список партнеров
    - Для каждого партнера ищет и скачивает документы
    - Сохраняет метаданные и формирует отчеты
    """
    conn = get_db_connection()
    if not conn: return

    try:
        with requests.Session() as session:
            sid = get_sid(session, EDI_LOGIN, EDI_PASSWORD)
            if not sid: return

            target_partners = []
            if client_identifier:
                logging.info(f"Шаг 2: Запрошена обработка для одного клиента по идентификатору: {client_identifier}")
                partner = _get_partner_details(session, client_identifier, SENDER_GLN)
                if partner:
                    target_partners.append(partner)
                else:
                    logging.error(f"Указанный клиент с идентификатором {client_identifier} не найден. Обработка прервана.")
                    return
            else:
                logging.info("Шаг 2: Идентификатор клиента не указан. Получаем и обрабатываем всех партнеров...")
                target_partners = get_all_partners_with_details(session, SENDER_GLN)
                if not target_partners:
                    logging.warning("Не найдено партнеров для обработки.")
                    return

            for partner in target_partners:
                client_gln = str(partner.get('gln'))
                client_name = partner.get('name', '').strip()
                client_kpp = partner.get('companyKpp')

                if not client_gln:
                    logging.warning(f"Пропуск партнера '{client_name}' из-за отсутствия GLN.")
                    continue

                # Имя папки теперь формируется только из кода, без имени клиента.
                if client_kpp:
                    client_folder_base = f"{client_kpp}"
                    logging.info(f"--- Начало обработки партнера: {client_name} (ЕГРПОУ: {client_kpp}) ---")
                else:
                    client_folder_base = f"{client_gln}"
                    logging.info(f"--- Начало обработки партнера: {client_name} (GLN: {client_gln}, ЕГРПОУ отсутствует) ---")

                client_folder_path = sanitize_filename(client_folder_base)

                all_docs_for_partner = []
                try:
                    logging.info("Поиск исходящих документов...")
                    dir_out = {"type": "EQ", "sender": [SENDER_GLN], "receiver": [client_gln]}
                    out_docs = get_documents(session, dir_out, start_date, end_date)

                    logging.info("Поиск входящих документов...")
                    dir_in = {"type": "EQ", "sender": [client_gln], "receiver": [SENDER_GLN]}
                    in_docs = get_documents(session, dir_in, start_date, end_date)

                    all_docs_for_partner = (out_docs or []) + (in_docs or [])
                    
                    if not all_docs_for_partner:
                        logging.info(f"Документы для партнера {client_name} в указанном периоде не найдены.")
                        continue
                    
                    logging.info(f"Всего найдено документов от API: {len(all_docs_for_partner)}")

                    with conn.cursor() as cursor:
                        downloaded_count, downloaded_filenames, excel_data = process_documents(
                            session, cursor, all_docs_for_partner, save_to_pdf, client_folder_path
                        )

                    conn.commit()
                    logging.info(f"Изменения в базе данных для {client_name} успешно сохранены.")

                    if save_to_pdf and downloaded_count > 0:
                        create_filenames_log(client_folder_path, downloaded_filenames)
                        create_client_excel_report(excel_data, client_folder_path, client_name, start_date, end_date)

                except Exception as e:
                    logging.error(f"Ошибка при обработке партнера {client_name}. Откат изменений для этого партнера. Ошибка: {e}")
                    if all_docs_for_partner:
                        dump_error_json(all_docs_for_partner)
                    conn.rollback()

                logging.info(f"--- Завершение обработки партнера: {client_name} ---")

    except Exception as e:
        logging.error(f"Произошла глобальная ошибка: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Соединение с PostgreSQL закрыто.")


if __name__ == "__main__":
    # --- ОСНОВНЫЕ НАСТРОЙКИ ЗАПУСКА ---
    # Укажите период выгрузки документов
    START_DATE = "2024-09-01"
    END_DATE = "2024-12-31"
    SAVE_PDF_AND_REPORTS = True  # True — скачивать PDF и формировать отчеты
    
    TARGET_CLIENT_IDENTIFIER = None  # Можно указать GLN/ЕГРПОУ для выборочной обработки

    # --- Запуск основной функции ---
    main(START_DATE, END_DATE, SAVE_PDF_AND_REPORTS, client_identifier=TARGET_CLIENT_IDENTIFIER)