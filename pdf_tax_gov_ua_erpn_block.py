# -*- coding: utf-8 -*-

import asyncio
import os
from datetime import datetime
from urllib.parse import quote
import asyncpg
import aiohttp
from dateutil.parser import parse
from dotenv import load_dotenv
from AsyncPostgresql import sql_to_df_async

# Загружаем переменные окружения
load_dotenv()

DB_USER = os.getenv('PG_USER')
DB_PASS = os.getenv('PG_PASSWORD')
DB_HOST = os.getenv('PG_HOST')
DB_NAME = os.getenv('PG_DBNAME')

# Импортируем вашу рабочую функцию для получения токена
try:
    from TaxGovUaConfig import get_token
except ImportError:
    print("🔥 Ошибка: Не удалось найти файл 'TaxGovUaConfig.py' с функцией get_token().")
    print("Пожалуйста, убедитесь, что файл находится в той же директории.")
    exit()

# --- КОНФИГУРАЦИЯ И ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---

URL_API_BASE = "https://cabinet.tax.gov.ua/ws/api"
URL_DOC_LIST = f"{URL_API_BASE}/nlnk/nlnkhd"
URL_PDF_DOC = f"{URL_API_BASE}/file/nlnkhd/pdf"
URL_PDF_RECEIPT1 = f"{URL_API_BASE}/file/nlnkhd/pdf/kvt"
URL_PDF_RECEIPT2 = f"{URL_API_BASE}/file/nlnkhd/pdf/kvt2"
URL_PDF_RECEIPT3 = f"{URL_API_BASE}/file/nlnkhd/pdf/kvt3"
URL_PDF_RECEIPT4 = f"{URL_API_BASE}/file/nlnkhd/pdf/kvt4"
DOWNLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
DESIRED_STATUSES = {1, 2, 13, 14, 15, 18}  # Статусы, заблокированных документов

SQL = r"""
    WITH 
    doc_pn AS (
        SELECT code 
        FROM t_tax_cabinet_erpn_api
        WHERE crtdate > current_date - 365 AND (hsmcstt in (1, 2, 13, 14, 15, 18))
    ),
    doc_rk AS (
        SELECT crcode
        FROM t_tax_cabinet_erpn_api
        WHERE crtdate > current_date - 365 AND (hsmcstt in (1, 2, 13, 14, 15, 18))
            AND coalesce(crcode,0) <> 0
    ),
    nn_rk AS (
        SELECT DISTINCT
            CASE 
                WHEN ftype = 0 THEN     
                    concat(cptin,'\' , to_char(crtdate,'yyyymm'),'\ПН\ПН ',nmr,' від ',to_char(crtdate,'dd mm yyyy'),'.pdf') 
                WHEN ftype = 1 THEN
                    concat(cptin,'\' , to_char(crtdate,'yyyymm'), '\ПН\ПН ',corrnmr,' від ',(SELECT to_char(crtdate,'dd mm yyyy') FROM t_tax_cabinet_erpn_api AS src WHERE src.code = api.crcode),' РК ',nmr,' ',to_char(crtdate,'dd mm yyyy'),'.pdf') 
            END AS file_name,
            concat('https://cabinet.tax.gov.ua/ws/api/file/nlnkhd/pdf?code=',api.code,'&impdate=',impdate) AS url,
            *
        FROM t_tax_cabinet_erpn_api AS api
            INNER JOIN (    
                SELECT 
                    code AS block_code
                FROM doc_pn
                UNION ALL 
                SELECT 
                    crcode
                FROM doc_rk
            ) AS api_block
            ON api.code = api_block.block_code
    )
    SELECT DISTINCT
        file_name,
        url,
        code,
        impdate,
        0 AS kvt_number
    FROM nn_rk AS api 
    UNION ALL 
    SELECT DISTINCT
        REPLACE(file_name,'.pdf',' KVT1.pdf') file_name,
        concat('https://cabinet.tax.gov.ua/ws/api/file/nlnkhd/pdf/kvt?code=',nn_rk.code,'&impdate=',impdate) AS url
        ,code,
        impdate,
        1 AS kvt_number
    FROM nn_rk
    UNION ALL 
    SELECT DISTINCT
        REPLACE(file_name,'.pdf',' KVT2.pdf') file_name,
        concat('https://cabinet.tax.gov.ua/ws/api/file/nlnkhd/pdf/kvt2?code=',api.code,'&impdate=',impdate) AS url    
        ,code,
        impdate,
        2 AS kvt_number
    FROM nn_rk AS api 
    WHERE api.kvt2 <> 0
    UNION ALL 
    SELECT DISTINCT
        REPLACE(file_name,'.pdf',' KVT3.pdf') file_name,
        concat('https://cabinet.tax.gov.ua/ws/api/file/nlnkhd/pdf/kvt3?code=',api.code,'&impdate=',impdate) AS url    
        ,code,
        impdate,
        3 AS kvt_number
    FROM nn_rk AS api 
    WHERE api.kvt3 <> 0
    UNION ALL 
    SELECT DISTINCT
        REPLACE(file_name,'.pdf',' KVT4.pdf') file_name,
        concat('https://cabinet.tax.gov.ua/ws/api/file/nlnkhd/pdf/kvt4?code=',api.code,'&impdate=',impdate) AS url    
        ,code,
        impdate,
        4 AS kvt_number
    FROM nn_rk AS api 
    WHERE api.kvt4 <> 0
    ORDER BY file_name
    ;

"""


async def download_pdf_async(session, url, headers, file_path, params_with_page):
    try:
        path_file = os.path.dirname(file_path)
        file_name = os.path.basename(file_path)
        path_file = os.path.join(DOWNLOADS_DIR, path_file)
        os.makedirs(path_file, exist_ok=True)
    except Exception as e:
        print(f"🔥 Ошибка: Не удалось создать директорию для файла {path_file}: {e}")
        return

    file_path = os.path.join(path_file, file_name)

    for attempt in range(5):
        try:
            async with session.get(url, headers=headers, params=params_with_page, timeout=45) as response:
                if response.status == 200:
                    with open(file_path, 'wb') as f:
                        f.write(await response.read())
                    print(f"  ✅ Файл сохранен: {os.path.basename(file_path)}")
                    return
                else:
                    print(f"  ❌ Ошибка {response.status} при скачивании {os.path.basename(file_path)}")
                    if 400 <= response.status < 500: break
        except Exception as e:
            pass
            # print(f"  ❌ Ошибка сети при скачивании {os.path.basename(file_path)}: {e}")

        await asyncio.sleep(2 ** attempt)
    print(f"🔥 Не удалось скачать файл: {os.path.basename(file_path)}")


async def main():
    print("🚀 Запуск скрипта для загрузки PDF документов...")
    print("Получение токена аутентификации...")
    driver, token = get_token()
    if not token:
        print("🔥 Критическая ошибка: Не удалось получить токен. Завершение работы.")
        if driver: driver.quit()
        return
    print("✅ Токен успешно получен.")
    headers = {"Authorization": token, "Content-Type": "application/json"}

    async with aiohttp.ClientSession() as session:
        download_tasks = []

        df = await sql_to_df_async(SQL)
        for _, row in df.iterrows():

            params_with_page = {
                'code': row['code'],
                'impdate': row['impdate'].strftime('%Y-%m-%d %H:%M:%S')
            }

            kvt_number = row['kvt_number']
            file_path = row['file_name']
            if kvt_number == 0:
                url = URL_PDF_DOC
            elif kvt_number == 1:
                url = URL_PDF_RECEIPT1
            elif kvt_number == 2:
                url = URL_PDF_RECEIPT2
            elif kvt_number == 3:
                url = URL_PDF_RECEIPT3
            elif kvt_number == 4:
                url = URL_PDF_RECEIPT4

            download_tasks.append(download_pdf_async(session, url, headers, file_path, params_with_page))

        if download_tasks:
            await asyncio.gather(*download_tasks)

    if driver:
        driver.quit()
    print("\n✅ Все задачи выполнены. Скрипт завершил работу.")


if __name__ == "__main__":
    start_time = datetime.now()
    asyncio.run(main())
    end_time = datetime.now()
    print(f"⏱️ Общее время выполнения: {end_time - start_time}")