# -*- coding: utf-8 -*-
"""
Скрипт для извлечения отдельных документов из скан-копий PDF.
Страницы для скачивания определяются SQL-запросом.
"""

import sys
import subprocess

# Проверка и установка необходимых модулей
required_modules = ["dotenv", "PyPDF2", "pdfplumber", "fitz", "numpy"]
for module in required_modules:
    try:
        __import__(module)
    except ImportError:
        print(f"Модуль '{module}' не найден. Устанавливаю...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", module])


import fitz  # PyMuPDF
import numpy as np
import pdfplumber
import re
from PyPDF2 import PdfReader, PdfWriter
from datetime import datetime
from collections import defaultdict
        

ukrainian_months = {
    'січня': 1, 'лютого': 2, 'березня': 3,
    'квітня': 4, 'травня': 5, 'червня': 6,
    'липня': 7, 'серпня': 8, 'вересня': 9,
    'жовтня': 10, 'листопада': 11, 'грудня': 12
}

TTN_END_KEYWORDS = [
    r'\bвантаж\w*', r'\bгабарит\w*', r'\bваг[аіу]', 
    r'\bавтомобіл\w*', r'\bпричіп\w*', r'\bприбутт\w*',
    r'\bдовжин\w*', r'\bширин\w*', r'\bвисот\w*'
]

VN_KEYWORDS = [
    r'\bвидаткова\b', r'\bпост(а|ачальник)\w*', 
    r'\bпокуп(ець|ця)\w*', r'\bдоговір\w*',
    r'\bугод[ауі]\w*', r'\bзамовлення\w*'
]

class DocumentProcessor:
    def __init__(self):
        self.vn_registry = {}
        self.other_counter = 1
        self.total_pages = 0
        self.blank_pages_count = 0
        self.processed_pages = set()
        self.non_blank_pages = []
        self.other_pages = []
        self.ttn_map = defaultdict(list)

    def is_blank(self, doc, page_num, threshold=0.99, white_level=250):
        """Проверка страницы на пустоту"""
        page = doc.load_page(page_num)
        pix = page.get_pixmap()
        pixels = np.frombuffer(pix.samples, dtype=np.uint8)
        return np.sum(pixels >= white_level)/pixels.size > threshold

    def parse_date(self, date_str):
        """Парсинг даты из текста"""
        try:
            day, month, year = re.findall(r'(\d{1,2})\s+([а-яіїє]+)\s+(\d{4})', date_str, re.I)[0]
            return datetime(int(year), ukrainian_months[month.lower()], int(day))
        except:
            for fmt in ['%d.%m.%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str, fmt)
                except: 
                    pass
        return None

    def is_vn_page(self, text):
        """Проверка на страницу ВН по ключевым словам"""
        matches = 0
        for pattern in VN_KEYWORDS:
            if re.search(pattern, text, re.I | re.UNICODE):
                matches += 1
        return matches >= 4

    def is_ttn_end_page(self, text):
        """Проверка на последнюю страницу ТТН"""
        matches = 0
        for pattern in TTN_END_KEYWORDS:
            if re.search(pattern, text, re.I | re.UNICODE):
                matches += 1
        return matches >= 5  # Снижено до 5 совпадений для гибкости

    def detect_document_type(self, text):
        """Определение типа документа"""
        if re.search(r'видаткова\s+накладна\s+№?\s*\d+', text, re.I | re.UNICODE):
            return "ВН"
        if self.is_vn_page(text):
            return "ВН"
        if re.search(r'товарно-транспортна\s+накладна', text, re.I | re.UNICODE):
            return "ТТН"
        return "Другой"

    def process_vn(self, text, pnum):
        """Обработка Видаткової накладної"""
        if match := re.search(r'видаткова\s+накладна\s+№?\s*(\d+)\s+від\s+([^\n]+)', text, re.I | re.UNICODE):
            number = match.group(1)
            date = self.parse_date(match.group(2))
            if date:
                self.vn_registry[number] = date
                self.processed_pages.add(pnum)
                return f"ВН {number} {date.year} {date.month:02d} {date.day:02d}.pdf"
        return None

    def process_ttn(self, text, start_page, end_page, reader, ttn_number):
        """Обработка Товарно-транспортної накладної"""
        vn_ref = re.search(r'видаткова\s+накладна\s+№?\s*(\d+)', text, re.I | re.UNICODE)
        date = self.vn_registry.get(vn_ref.group(1)) if vn_ref else None
        
        writer = PdfWriter()
        for pnum in range(start_page, end_page + 1):
            writer.add_page(reader.pages[pnum])
            self.processed_pages.add(pnum)
        
        date_str = f"{date.year} {date.month:02d} {date.day:02d}" if date else ""
        filename = f"ТТН {ttn_number} {date_str}".strip() + ".pdf"
        
        with open(filename, 'wb') as f:
            writer.write(f)

    def process_other(self, pnum, reader):
        """Обработка прочих документов"""
        writer = PdfWriter()
        writer.add_page(reader.pages[pnum])
        filename = f"Другой_{self.other_counter}.pdf"
        self.other_counter += 1
        self.processed_pages.add(pnum)
        with open(filename, 'wb') as f:
            writer.write(f)

    def find_ttn_start(self, pdf, end_page):
        """Поиск начала ТТН в обратном направлении"""
        for i in range(end_page-1, max(0, end_page-50), -1):
            text = pdf.pages[i].extract_text() or ''
            if re.search(r'товарно-транспортна\s+накладна', text, re.I | re.UNICODE):
                return i
            if i in self.processed_pages:
                break
        return None

    def process_ttn_by_range(self, start, end, reader):
        """Обработка диапазона страниц как ТТН"""
        text = reader.pages[start].extract_text() or ''
        if match := re.search(r'товарно-транспортна\s+накладна.*?№?\s*(\d+)', text, re.I | re.UNICODE | re.DOTALL):
            ttn_number = match.group(1)
            self.process_ttn(text, start, end, reader, ttn_number)

    def post_process_other(self, pdf, reader):
        """Повторная обработка 'Других' страниц"""
        for pnum in self.other_pages.copy():
            text = pdf.pages[pnum].extract_text() or ''
            
            # Проверка на ТТН
            if self.is_ttn_end_page(text):
                start_page = self.find_ttn_start(pdf, pnum)
                if start_page is not None:
                    self.process_ttn_by_range(start_page, pnum, reader)
                    self.other_pages = [x for x in self.other_pages if not (start_page <= x <= pnum)]
                    continue
                    
            # Проверка на ВН
            if self.is_vn_page(text):
                if filename := self.process_vn(text, pnum):
                    writer = PdfWriter()
                    writer.add_page(reader.pages[pnum])
                    with open(filename, 'wb') as f:
                        writer.write(f)
                    self.other_pages.remove(pnum)

    def find_ttn_end(self, pdf, start_pnum, ttn_number):
        """Поиск конца ТТН"""
        end_page = start_pnum
        for j in range(start_pnum + 1, len(pdf.pages)):
            if j not in self.non_blank_pages:
                continue
            text = pdf.pages[j].extract_text() or ''
            if self.detect_document_type(text) != "ТТН":
                return j - 1
            if re.search(rf'короб\s+{ttn_number}\b', text, re.I | re.UNICODE):
                return j
        return len(pdf.pages) - 1

    def process_pdf(self, input_pdf):
        """Основной метод обработки PDF"""
        # Определение пустых страниц
        doc = fitz.open(input_pdf)
        self.total_pages = len(doc)
        self.non_blank_pages = []
        for page_num in range(self.total_pages):
            if not self.is_blank(doc, page_num):
                self.non_blank_pages.append(page_num)
        self.blank_pages_count = self.total_pages - len(self.non_blank_pages)
        doc.close()

        with pdfplumber.open(input_pdf) as pdf:
            reader = PdfReader(input_pdf)
            
            # Первичная обработка
            i = 0
            while i < len(self.non_blank_pages):
                pnum = self.non_blank_pages[i]
                if pnum in self.processed_pages:
                    i += 1
                    continue
                
                text = pdf.pages[pnum].extract_text() or ''
                doc_type = self.detect_document_type(text)
                
                if doc_type == "ВН":
                    if filename := self.process_vn(text, pnum):
                        writer = PdfWriter()
                        writer.add_page(reader.pages[pnum])
                        with open(filename, 'wb') as f:
                            writer.write(f)
                    i += 1
                
                elif doc_type == "ТТН":
                    if match := re.search(r'товарно-транспортна\s+накладна.*?№?\s*(\d+)', 
                                         text, re.I | re.UNICODE | re.DOTALL):
                        ttn_number = match.group(1)
                        end_page = self.find_ttn_end(pdf, pnum, ttn_number)
                        
                        if end_page >= pnum:
                            self.process_ttn(text, pnum, end_page, reader, ttn_number)
                            while i < len(self.non_blank_pages) and self.non_blank_pages[i] <= end_page:
                                i += 1
                            continue
                else:
                    self.other_pages.append(pnum)
                    i += 1

            # Постобработка
            self.post_process_other(pdf, reader)

        # Финализация "Других" документов
        for pnum in self.other_pages:
            if pnum not in self.processed_pages:
                self.process_other(pnum, reader)

        # Статистика
        processed_count = len(self.processed_pages)
        unprocessed_count = len(self.non_blank_pages) - processed_count
        total_processed = self.blank_pages_count + processed_count + unprocessed_count
        
        print("\n=== Статистика обработки ===")
        print(f"Всего страниц: {self.total_pages}")
        print(f"Пропущено пустых: {self.blank_pages_count}")
        print(f"Обработано документов: {processed_count}")
        print(f"Необработанных непустых: {unprocessed_count}")
        print(f"Сумма: {total_processed}")
        
        if total_processed != self.total_pages:
            print("\n[ОШИБКА] Сумма показателей не совпадает с общим числом страниц!")
        else:
            print("\n[УСПЕХ] Проверка пройдена успешно!")

# Пример использования
if __name__ == "__main__":
    pdf_path = r"c:\Users\madocs\Desktop\Сверка\Scan\ВЕРЕСЕНЬ 2024\Parsed\ScanTest_ocred.pdf" 
    processor = DocumentProcessor()
    processor.process_pdf(pdf_path)