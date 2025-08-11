# -*- coding: utf-8 -*-
# pyinstaller --onefile --windowed --name PrivatBankDownloader PrivatBankPdfGUI.py
# https://gemini.google.com/app/2043f90d1b17f244
# Выгрузка квитанций ПриватБанка в PDF БЕЗ GUI
import asyncio
import os
import re
from datetime import datetime
from collections import defaultdict
import aiohttp
import aiofiles
import pandas as pd
from dotenv import load_dotenv
from dateutil.parser import parse

# Загружаем переменные окружения из файла .env
load_dotenv()

# --- КОНФИГУРАЦИЯ ---
# Базовые URL для API ПриватБанка
TRANSACTIONS_URL = "https://acp.privatbank.ua/api/statements/transactions"
RECEIPT_URL = "https://acp.privatbank.ua/api/paysheets/print_receipt"
# Максимальное количество одновременных загрузок PDF
MAX_CONCURRENT_DOWNLOADS = 10
# Корневая папка для всех квитанций
ROOT_DOWNLOAD_DIR = "receipts"

def sanitize_filename(name):
    """Удаляет недопустимые символы из имени для создания файла/папки."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

async def get_all_transactions(session, token, start_date, end_date):
    """
    Асинхронно получает полный список транзакций за указанный период.
    """
    all_transactions = []
    follow_id = None
    page_num = 1
    headers = {'user-agent': 'Avtoklient', 'token': token, 'Content-Type': 'application/json;charset=utf-8'}
    print(f"Получение транзакций с {start_date} по {end_date}...")
    while True:
        params = {'startDate': start_date, 'endDate': end_date, 'limit': 500}
        if follow_id:
            params['followId'] = follow_id
        try:
            async with session.post(TRANSACTIONS_URL, params=params, headers=headers, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()
                transactions_on_page = data.get('transactions', [])
                if transactions_on_page:
                    all_transactions.extend(transactions_on_page)
                    print(f"  - Получено {len(transactions_on_page)} транзакций со страницы {page_num}")
                if data.get('exist_next_page'):
                    follow_id = data.get('next_page_id')
                    page_num += 1
                else:
                    break
        except aiohttp.ClientError as e:
            print(f"Ошибка при получении транзакций: {e}")
            break
        except Exception as e:
            print(f"Неожиданная ошибка при обработке транзакций: {e}")
            break
    return all_transactions

async def download_receipt(session, transaction, token, client_code_dir, semaphore):
    """
    Асинхронно загружает PDF-квитанцию для одной транзакции в папку 
    КодКлиента/ТипДокумента/Период/файл.pdf.
    """
    async with semaphore:
        doc_num = transaction.get('NUM_DOC', '0')
        doc_date_str = transaction.get('DAT_OD', 'UNKNOWN_DATE')
        filename_placeholder = f"{sanitize_filename(doc_num)} от {doc_date_str}.pdf"

        try:
            doc_datetime = parse(doc_date_str, dayfirst=True)
            
            # НОВАЯ ЛОГИКА: Определяем папку "Входящие" или "Исходящие"
            tran_type = transaction.get('TRANTYPE')
            if tran_type == 'C': # Credit
                doc_type_folder = "Входящие"
            elif tran_type == 'D': # Debit
                doc_type_folder = "Исходящие"
            else:
                doc_type_folder = "Неопределенные" # На случай других типов транзакций

            # Создаем папку периода YYYYMM внутри папки КодКлиента/ТипДокумента/
            month_folder_name = doc_datetime.strftime('%Y%m')
            # Строим новый путь с учетом типа документа
            period_dir = os.path.join(client_code_dir, doc_type_folder, month_folder_name)
            os.makedirs(period_dir, exist_ok=True)

            doc_date_formatted = doc_datetime.strftime('%d %m %Y')
            filename = f"{sanitize_filename(doc_num)} {doc_date_formatted}.pdf"
            filepath = os.path.join(period_dir, filename)

            if os.path.exists(filepath):
                print(f"-> Файл {filename} уже существует, пропуск.")
                return (period_dir, filename)

            print(f"-> Загрузка квитанции {filename}...")
            headers = {'token': token, 'Content-Type': 'application/json', 'Accept': 'application/octet-stream'}
            payload = {
                "transactions": [{"account": transaction.get('AUT_MY_ACC'), "reference": transaction.get('REF'), "refn": transaction.get('REFN')}],
                "perPage": 1
            }
            async with session.post(RECEIPT_URL, json=payload, headers=headers, timeout=60) as response:
                if response.status == 200:
                    content = await response.read()
                    async with aiofiles.open(filepath, 'wb') as f:
                        await f.write(content)
                    print(f"   - УСПЕШНО: Файл {filename} сохранен.")
                    return (period_dir, filename)
                else:
                    error_text = await response.text()
                    print(f"   - ОШИБКА ЗАГРУЗКИ: Не удалось получить файл {filename}.")
                    print(f"     Причина: Статус {response.status}. Ответ сервера: {error_text.strip()}")
                    return None
        except Exception as e:
            print(f"   - КРИТИЧЕСКАЯ ОШИБКА при обработке транзакции для файла {filename_placeholder}: {e}")
            return None

async def write_log_files(file_map):
    """Записывает собранные имена файлов в лог-файлы по месяцам."""
    for folder_path, filenames in file_map.items():
        if not filenames: continue
        month_str = os.path.basename(folder_path)
        log_filepath = os.path.join(folder_path, f"{month_str}.log")
        filenames.sort()
        log_content = ",".join(filenames)
        async with aiofiles.open(log_filepath, 'w', encoding='utf-8') as f:
            await f.write(log_content)
        print(f"Создан/обновлен лог-файл: {log_filepath}")

def create_excel_report(report_data, client_dir, partner_code, client_name, start_date, end_date):
    """Создает Excel отчет на основе данных транзакций."""
    if not report_data:
        print("Нет данных для создания Excel отчета.")
        return
    try:
        report_data.sort(key=lambda x: x['дата'])
        df = pd.DataFrame(report_data)
        df['дата'] = pd.to_datetime(df['дата']).dt.strftime('%d.%m.%Y')
        
        sanitized_client_name = sanitize_filename(client_name)
        excel_filename = f"Отчет_{partner_code}_{sanitized_client_name}_{start_date}_по_{end_date}.xlsx"
        excel_filepath = os.path.join(client_dir, excel_filename)
        
        df.to_excel(excel_filepath, index=False, engine='openpyxl')
        print(f"\nОтчет Excel для '{client_name}' успешно сохранен: {excel_filepath}")
    except Exception as e:
        print(f"\nНе удалось создать Excel отчет для '{client_name}': {e}")

async def main():
    """
    Главная функция: определяет параметры, получает транзакции, фильтрует их, запускает загрузку и создает логи.
    """
    # --- УКАЖИТЕ ВАШИ ДАННЫЕ ЗДЕСЬ ---
    # Оставьте partner_code = "" для обработки ВСЕХ контрагентов.
    # Укажите код (например, "43028967") для обработки только одного контрагента.
    partner_code = ""
    token = os.getenv("PB_PPK_TOKEN")
    start_date = "01-01-2024"    
    # end_date = datetime.today().strftime("%d-%m-%Y")
    end_date = "30-09-2024"
    
    # !!! ВАЖНО !!!
    # Укажите правильное имя поля для СУММЫ транзакции из API.
    # Распространенные варианты: "AMT", "SUM", "amount", "TRNAMT", "SUM_E"
    amount_field = "SUM_E"
    # ------------------------------------

    if not token:
        print("Ошибка: токен не найден. Проверьте файл .env и имя переменной PB_PPK_TOKEN.")
        return

    os.makedirs(ROOT_DOWNLOAD_DIR, exist_ok=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    async with aiohttp.ClientSession() as session:
        all_transactions = await get_all_transactions(session, token, start_date, end_date)
        if not all_transactions:
            print("Транзакции за указанный период не найдены.")
            return

        # Группируем все транзакции по коду контрагента
        grouped_transactions = defaultdict(list)
        client_names = {}
        for tx in all_transactions:
            p_code = tx.get('AUT_CNTR_CRF')
            if p_code:
                grouped_transactions[p_code].append(tx)
                if p_code not in client_names:
                    client_names[p_code] = tx.get('AUT_CNTR_NAM', 'UnknownClient')

        # Определяем, каких партнеров обрабатывать
        partners_to_process = {}
        if partner_code: # Режим одного клиента
            if partner_code in grouped_transactions:
                partners_to_process = {partner_code: grouped_transactions[partner_code]}
                print(f"Режим одного клиента. Найден контрагент: {client_names.get(partner_code)}")
            else:
                print(f"Контрагент с кодом {partner_code} не найден в транзакциях за указанный период.")
                return
        else: # Режим всех клиентов
            partners_to_process = grouped_transactions
            print(f"\nРежим всех клиентов. Обнаружено {len(partners_to_process)} контрагентов. Начинаю обработку...")

        # Основной цикл обработки по каждому контрагенту
        for p_code, p_transactions in partners_to_process.items():
            client_name = client_names.get(p_code, "UnknownClient")
            
            print("\n" + "="*60)
            print(f"Обработка клиента: {client_name} (код: {p_code})")
            print(f"Найдено транзакций: {len(p_transactions)}")
            
            # Создаем папку для клиента по его коду
            client_code_dir = os.path.join(ROOT_DOWNLOAD_DIR, p_code)
            os.makedirs(client_code_dir, exist_ok=True)

            tx_to_download, report_data = [], []
            log_file_map = defaultdict(list)

            for tx in p_transactions:
                tx_to_download.append(tx)
                
                try:
                    doc_datetime = parse(tx.get('DAT_OD', ''), dayfirst=True)
                    amount_raw = tx.get(amount_field)
                    if amount_raw is None:
                        print(f" - Предупреждение: поле суммы '{amount_field}' не найдено в транзакции {tx.get('REF')}. Сумма будет 0.")
                        amount = 0.0
                    else:
                        amount = float(amount_raw)

                    if tx.get('TRANTYPE') == 'D':
                        amount = -amount
                    
                    report_data.append({
                        "Клиент": client_name,
                        "дата": doc_datetime.strftime('%Y-%m-%d'),
                        "сумма": amount
                    })
                except (ValueError, TypeError) as e:
                    print(f" - Предупреждение: не удалось обработать данные для отчета из транзакции {tx.get('REF')}. Ошибка: {e}")

            if tx_to_download:
                print(f"\nНачало загрузки {len(tx_to_download)} квитанций...")
                tasks = [download_receipt(session, tx, token, client_code_dir, semaphore) for tx in tx_to_download]
                for result in await asyncio.gather(*tasks):
                    if result:
                        folder, filename = result
                        log_file_map[folder].append(filename)
            
            if log_file_map:
                await write_log_files(log_file_map)
            
            create_excel_report(report_data, client_code_dir, p_code, client_name, start_date, end_date)

    print("\n" + "="*60)
    print("Работа полностью завершена.")

if __name__ == "__main__":
    # Для Windows может понадобиться следующая строка
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())