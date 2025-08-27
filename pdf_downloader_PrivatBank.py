# -*- coding: utf-8 -*-
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

load_dotenv()

TRANSACTIONS_URL = "https://acp.privatbank.ua/api/statements/transactions"
RECEIPT_URL = "https://acp.privatbank.ua/api/paysheets/print_receipt"
MAX_CONCURRENT_DOWNLOADS = 10
ROOT_DOWNLOAD_DIR = "Privat_PPK"


def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)


async def get_all_transactions(session, token, start_date, end_date):
    all_transactions = []
    follow_id = None
    headers = {'user-agent': 'Avtoklient', 'token': token, 'Content-Type': 'application/json;charset=utf-8'}

    while True:
        params = {'startDate': start_date, 'endDate': end_date, 'limit': 500}
        if follow_id:
            params['followId'] = follow_id

        try:
            async with session.post(TRANSACTIONS_URL, params=params, headers=headers, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()

                if not data.get('transactions'):
                    break

                all_transactions.extend(data['transactions'])
                follow_id = data.get('next_page_id') if data.get('exist_next_page') else None

                if not follow_id:
                    break

        except Exception as e:
            print(f"Ошибка при получении транзакций: {e}")
            break

    return all_transactions


async def download_receipt(session, transaction, token, client_code_dir, semaphore):
    async with semaphore:
        doc_num = transaction.get('NUM_DOC', '0')
        doc_date_str = transaction.get('DAT_OD', 'UNKNOWN_DATE')

        try:
            doc_datetime = parse(doc_date_str, dayfirst=True)
            tran_type = transaction.get('TRANTYPE')

            doc_type_folder = "Входящие" if tran_type == 'C' else "Исходящие" if tran_type == 'D' else "Неопределенные"
            month_folder_name = doc_datetime.strftime('%Y%m')
            period_dir = os.path.join(client_code_dir, doc_type_folder, month_folder_name)
            os.makedirs(period_dir, exist_ok=True)

            doc_date_formatted = doc_datetime.strftime('%d %m %Y')
            filename = f"{sanitize_filename(doc_num)} {doc_date_formatted}.pdf"
            filepath = os.path.join(period_dir, filename)

            if os.path.exists(filepath):
                return (period_dir, filename)

            headers = {'token': token, 'Content-Type': 'application/json', 'Accept': 'application/octet-stream'}
            payload = {
                "transactions": [{
                    "account": transaction.get('AUT_MY_ACC'),
                    "reference": transaction.get('REF'),
                    "refn": transaction.get('REFN')
                }],
                "perPage": 1
            }

            for attempt in range(3):
                try:
                    async with session.post(RECEIPT_URL, json=payload, headers=headers, timeout=60) as response:
                        if response.status == 200:
                            content = await response.read()
                            async with aiofiles.open(filepath, 'wb') as f:
                                await f.write(content)
                            return (period_dir, filename)
                        await asyncio.sleep(2)
                except Exception as e:
                    if attempt == 2:
                        print(f"Ошибка загрузки {filename}: {str(e)}")
                    await asyncio.sleep(2)

        except Exception as e:
            print(f"Ошибка обработки транзакции {doc_num}: {str(e)}")

        return None


async def write_log_files(file_map):
    for folder_path, filenames in file_map.items():
        if not filenames:
            continue

        month_str = os.path.basename(folder_path)
        log_filepath = os.path.join(folder_path, f"{month_str}.log")
        filenames.sort()
        log_content = ",".join(filenames)

        try:
            async with aiofiles.open(log_filepath, 'w', encoding='utf-8') as f:
                await f.write(log_content)
        except Exception as e:
            print(f"Ошибка записи лога {log_filepath}: {str(e)}")


def create_excel_report(report_data, client_dir, partner_code, client_name, start_date, end_date):
    if not report_data:
        return

    try:
        report_data.sort(key=lambda x: x['дата'])
        df = pd.DataFrame(report_data)
        df['дата'] = pd.to_datetime(df['дата']).dt.strftime('%d.%m.%Y')

        sanitized_client_name = sanitize_filename(client_name)
        excel_filename = f"Отчет_{partner_code}_{sanitized_client_name}_{start_date}_по_{end_date}.xlsx"
        excel_filepath = os.path.join(client_dir, excel_filename)

        df.to_excel(excel_filepath, index=False, engine='openpyxl')
    except Exception as e:
        print(f"Ошибка создания отчета для {client_name}: {str(e)}")


async def main(token, start_date, end_date):
    partner_code = ""
    amount_field = "SUM_E"

    if not token:
        print("Ошибка: токен не найден")
        return

    os.makedirs(ROOT_DOWNLOAD_DIR, exist_ok=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession() as session:
        all_transactions = await get_all_transactions(session, token, start_date, end_date)
        if not all_transactions:
            print("Транзакции не найдены")
            return

        grouped_transactions = defaultdict(list)
        client_names = {}

        for tx in all_transactions:
            p_code = tx.get('AUT_CNTR_CRF')
            if p_code:
                grouped_transactions[p_code].append(tx)
                if p_code not in client_names:
                    client_names[p_code] = tx.get('AUT_CNTR_NAM', 'UnknownClient')

        partners_to_process = {}
        if partner_code:
            if partner_code in grouped_transactions:
                partners_to_process = {partner_code: grouped_transactions[partner_code]}
            else:
                print(f"Контрагент {partner_code} не найден")
                return
        else:
            partners_to_process = grouped_transactions

        for p_code, p_transactions in partners_to_process.items():
            client_name = client_names.get(p_code, "UnknownClient")
            print(f"Обработка: {client_name} ({len(p_transactions)} транзакций)")

            client_code_dir = os.path.join(ROOT_DOWNLOAD_DIR, p_code)
            os.makedirs(client_code_dir, exist_ok=True)

            tx_to_download, report_data = [], []
            log_file_map = defaultdict(list)

            for tx in p_transactions:
                tx_to_download.append(tx)

                try:
                    doc_datetime = parse(tx.get('DAT_OD', ''), dayfirst=True)
                    amount_raw = tx.get(amount_field)
                    amount = float(amount_raw) if amount_raw is not None else 0.0

                    if tx.get('TRANTYPE') == 'D':
                        amount = -amount

                    report_data.append({
                        "Клиент": client_name,
                        "дата": doc_datetime.strftime('%Y-%m-%d'),
                        "сумма": amount
                    })
                except Exception:
                    pass

            if tx_to_download:
                tasks = [download_receipt(session, tx, token, client_code_dir, semaphore) for tx in tx_to_download]
                results = await asyncio.gather(*tasks)

                for result in results:
                    if result:
                        folder, filename = result
                        log_file_map[folder].append(filename)

            if log_file_map:
                await write_log_files(log_file_map)

            create_excel_report(report_data, client_code_dir, p_code, client_name, start_date, end_date)

    print("Работа завершена")


if __name__ == "__main__":
    token = os.getenv("PB_PPK_TOKEN")
    start_date = "01-01-2024"
    end_date = datetime.today().strftime("%d-%m-%Y")

    asyncio.run(main(token, start_date, end_date))