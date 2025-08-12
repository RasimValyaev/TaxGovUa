# -*- coding: utf-8 -*-
"""
https://aistudio.google.com/prompts/16kCR0IFokp02JaI8_RwQ02oBS7EXeLWm
Создает PDF файлы из страниц исходных файлов, указанных в базе данных.
PDF_create_from_pg.py
Данные для отбора страниц берутся из базы данных. метод: fetch_scan_data через SQL запрос.
"""

import fitz  # PyMuPDF
import os
import logging
from typing import List, Tuple, Dict
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from datetime import datetime

# --- Настройка ---

load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Конфигурация из .env ---
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST_LOCAL")
PG_PORT = os.getenv("PG_PORT")
PG_DBNAME = os.getenv("PG_DBNAME")

BASE_INPUT_PATH = Path(os.getenv("BASE_INPUT_PATH", "input_folder"))
BASE_OUTPUT_PATH = Path(os.getenv("BASE_OUTPUT_PATH", "output_folder"))

DPI = 200
JPEG_QUALITY = 20


# --- Вспомогательная функция для очистки имени папки ---
def sanitize_foldername(name: str) -> str:
    """Удаляет символы, недопустимые в именах папок Windows/Linux."""
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        name = name.replace(char, '')
    return name.strip()


# --- Функции обработки PDF (без изменений) ---

def is_scan(page: fitz.Page) -> bool:
    if page.get_text().strip():
        return False
    return len(page.get_images()) > 0


def extract_and_compress_pages(
    input_path: str,
    output_path: str,
    page_numbers: List[int],
    dpi: int = 100,
    jpeg_quality: int = 20,
) -> Tuple[bool, Dict]:
    if not os.path.exists(input_path):
        logger.error(f"Входной файл не найден: {input_path}")
        return False, {"error": "Входной файл не существует"}

    doc = None
    new_doc = None
    try:
        doc = fitz.open(input_path)
        new_doc = fitz.open()
        total_pages = len(doc)
        invalid_pages = [p for p in page_numbers if p < 1 or p > total_pages]
        if invalid_pages:
            error_msg = f"Страницы {invalid_pages} не существуют в документе '{os.path.basename(input_path)}' (всего {total_pages})"
            logger.error(error_msg)
            return False, {"error": error_msg}

        scan_pages = 0
        vector_pages = 0
        for page_num in sorted(page_numbers):
            page = doc.load_page(page_num - 1)
            if is_scan(page):
                pix = page.get_pixmap(dpi=dpi)
                img_bytes = pix.tobytes("jpeg", jpg_quality=jpeg_quality)
                new_page = new_doc.new_page(width=page.rect.width, height=page.rect.height)
                new_page.insert_image(page.rect, stream=img_bytes)
                scan_pages += 1
            else:
                new_doc.insert_pdf(doc, from_page=page_num - 1, to_page=page_num - 1)
                vector_pages += 1
        
        if len(new_doc) > 0:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            new_doc.save(output_path, garbage=4, deflate=True, clean=True)
            return True, {"processed_pages": len(page_numbers)}
        else:
            return False, {"error": "Нет страниц для обработки"}
    except Exception as e:
        logger.error(f"Ошибка при обработке {input_path}: {str(e)}", exc_info=True)
        return False, {"error": str(e)}
    finally:
        if doc: doc.close()
        if new_doc: new_doc.close()


# --- Логика для работы с БД ---

def fetch_scan_data() -> pd.DataFrame:
    conn_string = f"dbname='{PG_DBNAME}' user='{PG_USER}' host='{PG_HOST}' password='{PG_PASSWORD}' port='{PG_PORT}'"
    try:
        with psycopg2.connect(conn_string) as conn:
            logger.info("Успешное подключение к базе данных.")
            
            # --- ИЗМЕНЕННЫЙ SQL-ЗАПРОС ---
            sql = """
                SELECT DISTINCT ON (doc_type, doc_date, doc_number, buyer_name, buyer_code, invoices_numbers, page_type)
                    doc_type,
                    doc_date,
                    doc_number,
                    buyer_name,
                    buyer_code,
                    page_number,
                    page_type,
                    invoices_numbers,
                    file_name
                FROM 
                    t_scan_documents
                WHERE external_id = 1170
--                    doc_date >= '01.10.2024'::date
--                    AND doc_date < '01.12.2024'::date
                ORDER BY
                    -- Поля, по которым ищем дубликаты (должны совпадать с DISTINCT ON)
                    doc_type, doc_date, doc_number, buyer_name, buyer_code, invoices_numbers, page_type,
                    -- Поле, по которому выбираем "лучшую" запись из дубликатов (берем первую по номеру страницы)
                    page_number ASC                    
                ;
            """
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            df = pd.read_sql_query(sql, conn)
            logger.info(f"Загружено {len(df)} уникальных записей из t_scan_documents.")
            return df
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}", exc_info=True)
        return pd.DataFrame()


def process_documents():
    """
    Основная функция: получает данные, группирует их и обрабатывает каждый документ.
    """
    df = fetch_scan_data()
    if df.empty:
        logger.warning("Нет данных для обработки. Завершение работы.")
        return

    # --- БЛОК СОХРАНЕНИЯ EXCEL (без изменений) ---
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_filename = f"Scan_Report_{timestamp}.xlsx"
        excel_output_path = BASE_OUTPUT_PATH / excel_filename
        df.to_excel(excel_output_path, index=False)
        logger.info(f"Все загруженные данные сохранены в Excel: {excel_output_path}")
    except Exception as e:
        logger.error(f"Не удалось сохранить общий Excel-отчет. Ошибка: {e}")
    # --- КОНЕЦ БЛОКА ---

    df['doc_date'] = pd.to_datetime(df['doc_date'])
    
    # Группировка теперь будет работать с уже уникальными данными
    grouping_keys = ['doc_type', 'doc_date', 'doc_number', 'buyer_name', 'buyer_code']
    grouped = df.groupby(grouping_keys, sort=False)

    logger.info(f"Найдено {len(grouped)} уникальных документов для обработки.")

    for name, group in grouped:
        doc_type, doc_date, doc_number, buyer_name, buyer_code = name

        sanitized_buyer_name = sanitize_foldername(buyer_name)
        client_folder_name = f"{buyer_code}_{sanitized_buyer_name}"
        output_dir = BASE_OUTPUT_PATH / client_folder_name / str(doc_type)

        formatted_date = doc_date.strftime('%d %m %Y')
        output_filename = f"{doc_type} {doc_number} {formatted_date}.pdf"
        full_output_path = output_dir / output_filename

        source_filename = group['file_name'].iloc[0]
        full_input_path = BASE_INPUT_PATH / source_filename
        page_numbers = group['page_number'].astype(int).tolist()

        logger.info(f"--- Обработка группы: {doc_type} №{doc_number} от {formatted_date} ---")
        logger.info(f"Исходный файл: {full_input_path}")
        logger.info(f"Страницы для извлечения: {sorted(page_numbers)}")
        logger.info(f"Выходной файл: {full_output_path}")

        success, stats = extract_and_compress_pages(
            input_path=str(full_input_path),
            output_path=str(full_output_path),
            page_numbers=page_numbers,
        )

        if success:
            logger.info(f"Документ успешно создан: {stats['processed_pages']} страниц обработано.\n")
        else:
            logger.error(f"Не удалось создать документ. Ошибка: {stats.get('error')}\n")


if __name__ == "__main__":
    if not BASE_INPUT_PATH.exists():
        logger.error(f"Критическая ошибка: Папка с исходными файлами не найдена: {BASE_INPUT_PATH}")
    elif not BASE_OUTPUT_PATH.exists():
        logger.error(f"Критическая ошибка: Папка для сохранения результатов не найдена: {BASE_OUTPUT_PATH}")
    else:
        process_documents()