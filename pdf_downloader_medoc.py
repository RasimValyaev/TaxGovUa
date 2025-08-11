# -*- coding: utf-8 -*-

"""
================================================================================
НАЗНАЧЕНИЕ ФАЙЛА:
--------------------------------------------------------------------------------
Этот скрипт предназначен для асинхронной загрузки первичных документов
(например, счетов, актов) в формате PDF из API системы электронного
документооборота "M.E.Doc".

Скрипт находит все документы для указанного контрагента (по коду ЕГРПОУ)
за заданный период времени и сохраняет их в локальную папку.

Файлы раскладываются по подпапкам в зависимости от типа документа.

downloadPdf.py
https://aistudio.google.com/prompts/1T6A_xzSS7Mol8SihX7R716K_s7NcrGO7

================================================================================
КЛЮЧЕВЫЕ ОСОБЕННОСТИ:
--------------------------------------------------------------------------------
1.  АСИНХРОННОСТЬ:
    Использует библиотеки `asyncio` и `aiohttp` для выполнения множества
    сетевых запросов параллельно, что значительно ускоряет процесс загрузки
    большого количества документов.

2.  ОГРАНИЧЕНИЕ НАГРУЗКИ (SEMAPHORE):
    Чтобы не перегружать API-сервер и избегать ошибок, количество
    одновременных запросов на скачивание PDF ограничено (по умолчанию 5).

3.  РАЗБИВКА БОЛЬШИХ ДИАПАЗОНОВ ДАТ:
    Если запрашиваемый период превышает один месяц, скрипт автоматически
    разбивает его на месячные интервалы. Это предотвращает таймауты и ошибки
    на стороне API при запросе слишком большого объема данных за раз.

4.  НАДЕЖНОСТЬ И ОБРАБОТКА ОШИБОК:
    - Корректно обрабатывает ошибки сети и таймауты.
    - Решает специфическую проблему `TransferEncodingError`, принудительно
      закрывая соединение после каждого запроса (`Connection: close`).
    - Ведет подробное логирование всех шагов и возможных проблем.

5.  КОНФИГУРАЦИЯ ЧЕРЕЗ .ENV:
    Сетевые настройки (адрес сервера) вынесены в переменные окружения,
    которые можно задать в файле `.env`.

================================================================================
ЗАВИСИМОСТИ:
--------------------------------------------------------------------------------
- aiohttp
- python-dotenv
- python-dateutil
- collections (встроенная)

Установка: pip install aiohttp python-dotenv python-dateutil

================================================================================
КАК ИСПОЛЬЗОВАТЬ:
--------------------------------------------------------------------------------
1.  Создайте файл `.env` в той же директории, что и скрипт.
2.  Добавьте в него переменную `PG_HOST_LOCAL=ВАШ_IP_АДРЕС_СЕРВЕРА`.
3.  Настройте параметры в функции `main()`: `partner`.
4.  Запустите скрипт: python ваш_скрипт.py
5.  Загруженные файлы появятся в папке, названной кодом партнера,
    рассортированные по подпапкам (`Продажа`, `Акт` и т.д.).
6.  Если некоторые файлы нечитаемы, используйте функцию `repair_files_by_id`.
================================================================================
"""

import asyncio
import base64
import os
import re
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from dotenv import load_dotenv
import aiohttp
from dateutil.relativedelta import relativedelta

load_dotenv()

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Конфигурация ---
HOSTNAME_PUBLIC = os.getenv("PG_HOST_LOCAL", "192.168.1.254")
ID_ORG = 781


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def clean_filename(filename: str) -> str:
    """Удаляет недопустимые символы из имени файла и лишние пробелы."""
    return re.sub(r'[\\/*?:"<>|]', "", filename).strip()

def get_doc_type_name(docname: Optional[str]) -> str:
    """
    Извлекает из имени документа описательную часть до знака '№'.
    Если знак '№' в строке отсутствует, возвращает исходную строку.
    Возвращает '_Інше' для пустого ввода.
    """
    if not docname:
        return "_Інше"

    separator_index = docname.find('№')

    if separator_index == -1:
        return docname.strip()
    
    return docname[:separator_index].strip()

def split_date_range_by_month(start_date_str: str, end_date_str: str) -> List[Tuple[date, date]]:
    date_format = '%Y/%m/%d'
    start_dt = datetime.strptime(start_date_str, date_format).date()
    end_dt = datetime.strptime(end_date_str, date_format).date()
    if start_dt + relativedelta(months=1) > end_dt:
        return [(start_dt, end_dt)]
    print("Диапазон дат слишком большой. Разбиваю на месячные интервалы...")
    date_ranges = []
    current_start = start_dt
    while current_start <= end_dt:
        chunk_end = current_start + relativedelta(months=1) - relativedelta(days=1)
        if chunk_end > end_dt:
            chunk_end = end_dt
        date_ranges.append((current_start, chunk_end))
        current_start = chunk_end + relativedelta(days=1)
    return date_ranges


# --- ОСНОВНЫЕ АСИНХРОННЫЕ ФУНКЦИИ ---

async def fetch_one_url(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> Optional[Any]:
    async with semaphore:
        try:
            headers = {"Connection": "close"}
            # Устанавливаем общий таймаут на 300 секунд (5 минут)
            timeout = aiohttp.ClientTimeout(total=300) 
            
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logging.error(f"Ошибка запроса к {url}. Статус: {response.status}, Ответ: {await response.text()}")
                    return None
        except asyncio.TimeoutError:
            logging.error(f"Таймаут при запросе к {url}. Сервер не ответил за 300 секунд.")
            return None
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при запросе к {url}: {e}")
            return None

async def get_document_as_pdf(session: aiohttp.ClientSession, doc: Dict[str, Any], semaphore: asyncio.Semaphore,
                              facsimile: bool, output_dir: str, suffix: str = "") -> Optional[str]:
    doc_id = doc.get('doc_id')
    url = f"http://{HOSTNAME_PUBLIC}:63777/api/Info/PrintDocPDF?idOrg={ID_ORG}&docID={doc_id}&facsimile={str(facsimile).lower()}"
    data = await fetch_one_url(session, url, semaphore)
    if not data:
        logging.warning(f"Нет ответа от API для doc_id: {doc_id} (facsimile={facsimile}).")
        return None
    try:
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            logging.error(f"Неожиданный формат ответа API для doc_id: {doc_id}")
            return None
        
        document_info = data[0]
        file_raw = document_info.get('File')
        file_name_from_api = document_info.get('FileName')

        if not file_raw:
            logging.error(f"В ответе API отсутствует 'File' для doc_id: {doc_id}")
            return None
        
        base_name = ""
        if file_name_from_api:
            if re.search(r'\d{2}\.\d{2}\.\d{4}', file_name_from_api):
                base_name = file_name_from_api
            else:
                docname = doc.get('docname', '')
                doc_num = doc.get('doc_num', '')
                doc_date_str = doc.get('doc_date')
                formatted_date = ''
                if doc_date_str:
                    try:
                        parsed_date = datetime.fromisoformat(doc_date_str)
                        formatted_date = parsed_date.strftime('%d.%m.%Y')
                    except (ValueError, TypeError):
                        pass
                
                base_name = ' '.join(filter(None, [docname, doc_num, formatted_date]))
        
        if not base_name:
            logging.error(f"Не удалось сформировать базовое имя для doc_id: {doc_id}. Используется запасное имя.")
            base_name = f"UNNAMED_{doc_id}"

        final_name = base_name.replace('.', ' ').upper()
        final_file_name = f"{clean_filename(final_name)}{suffix}.PDF"

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, final_file_name)
        
        file_data = base64.b64decode(file_raw)
        with open(file_path, 'wb') as f:
            f.write(file_data)
        return file_path
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка при обработке doc_id {doc_id}: {e}")
        return None


async def download_documents_for_partner(session: aiohttp.ClientSession, partner_edrpou: str, date_from: str,
                                         date_end: str, semaphore: asyncio.Semaphore):
    base_output_dir = partner_edrpou
    print(f"Запуск процесса загрузки для партнёра {partner_edrpou} с {date_from} по {date_end}")
    print(f"Файлы будут сохранены в базовую папку: ./{base_output_dir}")

    date_ranges = split_date_range_by_month(date_from, date_end)
    all_documents = []
    for start_chunk, end_chunk in date_ranges:
        chunk_from_str = start_chunk.strftime('%Y/%m/%d')
        chunk_end_str = end_chunk.strftime('%Y/%m/%d')
        print(f"Запрос списка документов за период: {chunk_from_str} - {chunk_end_str}")
        url = (f"http://{HOSTNAME_PUBLIC}:63777/api/Info/GetPrimaryReestr?"
               f"idOrg={ID_ORG}&docType=-1&moveType=0&dateFrom={chunk_from_str}&dateEnd={chunk_end_str}")
        documents_chunk = await fetch_one_url(session, url, semaphore)
        if documents_chunk:
            all_documents.extend(documents_chunk)
        else:
            logging.warning(f"Не удалось получить документы за период {chunk_from_str} - {chunk_end_str}.")
        await asyncio.sleep(1)

    if not all_documents:
        logging.warning("Не удалось получить данные о документах ни за один из периодов.")
        return

    partner_docs = [doc for doc in all_documents if doc.get('partner_edrpou') == partner_edrpou]

    if not partner_docs:
        print(f"Для партнёра {partner_edrpou} не найдены документы в указанном диапазоне дат.")
        return

    grouped_by_id = defaultdict(list)
    for doc in partner_docs:
        if doc.get('doc_id'):
            grouped_by_id[doc['doc_id']].append(doc)

    unique_partner_docs = []
    for doc_id, doc_group in grouped_by_id.items():
        if len(doc_group) == 1:
            unique_partner_docs.append(doc_group[0])
        else:
            try:
                sorted_group = sorted(
                    doc_group,
                    key=lambda d: datetime.fromisoformat(d.get('moddate', '1970-01-01T00:00:00')),
                    reverse=True
                )
                unique_partner_docs.append(sorted_group[0])
            except (ValueError, TypeError) as e:
                logging.warning(f"Ошибка при сортировке дубликатов для doc_id {doc_id} (Ошибка: {e}). Будет использован первый найденный.")
                unique_partner_docs.append(doc_group[0])
    
    print(f"Всего найдено в реестре (с дубликатами): {len(partner_docs)} документов.")
    print(f"Найдено уникальных документов (с учетом 'moddate'): {len(unique_partner_docs)}. Начинаю загрузку PDF...")

    tasks = []
    for doc in unique_partner_docs:
        doc_type_folder_name = get_doc_type_name(doc.get('docname'))
        doc_specific_output_dir = os.path.join(base_output_dir, doc_type_folder_name)
        
        task = get_document_as_pdf(session, doc, semaphore, facsimile=True, output_dir=doc_specific_output_dir)
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    failed_docs = []
    successful_count = 0
    for doc, result_path in zip(unique_partner_docs, results):
        if result_path:
            successful_count += 1
        else:
            failed_docs.append(doc)

    total_files_on_disk = 0
    if os.path.exists(base_output_dir):
        for root, dirs, files in os.walk(base_output_dir):
            total_files_on_disk += len(files)

    print("\n" + "="*40)
    print("--- ИТОГИ ЗАГРУЗКИ ---")
    print(f"Найдено в реестре (с дубликатами): {len(partner_docs)}")
    print(f"Найдено уникальных документов: {len(unique_partner_docs)}")
    print(f"✅ Успешно загружено по данным скрипта: {successful_count}")
    print(f"💽 Фактически файлов в папках: {total_files_on_disk}")
        
    if failed_docs:
        failed_ids = [d.get('doc_id', 'N/A') for d in failed_docs]
        print(f"❌ Не удалось загрузить: {len(failed_docs)} файлов")
        print(f"   ID незагруженных документов: {failed_ids}")
    print("="*40 + "\n")
    print("🎉 Все задачи по загрузке завершены.")


async def repair_files_by_id(session: aiohttp.ClientSession, partner_edrpou: str, all_docs_from_reestr: List[Dict[str, Any]],
                             doc_ids_to_repair: List[str], semaphore: asyncio.Semaphore):
    if not doc_ids_to_repair:
        print("Список ID для ремонта пуст.")
        return
        
    repair_output_dir = os.path.join(partner_edrpou, "_РЕМОНТ")
    print(f"\n--- ЗАПУСК РЕМОНТА ФАЙЛОВ ДЛЯ ПАРТНЕРА {partner_edrpou} ---")
    print(f"Будет загружено {len(doc_ids_to_repair)} читаемых версий в папку ./{repair_output_dir}")

    docs_to_repair = [doc for doc in all_docs_from_reestr if doc.get('doc_id') in doc_ids_to_repair]
    if not docs_to_repair:
        print("Не удалось найти данные для указанных ID в исходном реестре.")
        return

    tasks = []
    for doc in docs_to_repair:
        task = get_document_as_pdf(
            session, doc, semaphore,
            facsimile=False,
            output_dir=repair_output_dir,
            suffix="_readable"
        )
        tasks.append(task)
        
    results = await asyncio.gather(*tasks)
    
    repaired_count = sum(1 for r in results if r is not None)
    print(f"--- РЕМОНТ ЗАВЕРШЕН ---")
    print(f"✅ Успешно создано читаемых копий: {repaired_count} из {len(doc_ids_to_repair)}")


async def main():
    partner = '05475067'
    date_from = '2024/01/01'
    date_to =   '2024/12/31'
    # date_to = datetime.today().strftime('%Y/%m/%d')
    semaphore = asyncio.Semaphore(5)

    async with aiohttp.ClientSession() as session:
        await download_documents_for_partner(
            session=session,
            partner_edrpou=partner,
            date_from=date_from,
            date_end=date_to,
            semaphore=semaphore
        )
        
        # ids_to_repair = [
        #     "A1513006-DA4B-44D2-BD65-5227A1D3AB00",
        # ]
        # # Для ремонта нужно получить полный список документов, чтобы найти нужные данные
        # # Это более сложный сценарий, пока оставим его закомментированным.
        # # all_docs = await download_documents_for_partner(...)
        # # await repair_files_by_id(session, partner, all_docs, ids_to_repair, semaphore)


if __name__ == '__main__':
    asyncio.run(main())