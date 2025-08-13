#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Если страница — скан, она не включается в результат.
Если текстовая — текст добавляется в общий результат с пометкой номера страницы.
Вывод: None (если все страницы сканы) или объединённый текст всех текстовых страниц.
"""

import os
import sys
import fitz  # PyMuPDF
print(f"fitz.__file__", fitz.__file__)

TEXT_THRESHOLD = 300      # минимум символов для признания страницы текстовой
IMAGE_COVERAGE = 0.8     # >=80% покрытия изображениями → скан


def extract_text_if_not_scan(pdf_path):
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"Файл не найден: {pdf_path}")

    doc = fitz.open(pdf_path)
    collected_texts = []

    for page_number, page in enumerate(doc, start=1):
        text = (page.get_text("text") or "").strip()
        text_len = len(text)

        # Определяем покрытие изображениями
        blocks = page.get_text("dict").get("blocks", [])
        img_blocks = [b for b in blocks if b.get("type") == 1]
        page_area = page.rect.width * page.rect.height
        img_area = sum((b["bbox"][2] - b["bbox"][0]) * (b["bbox"][3] - b["bbox"][1])
                       for b in img_blocks)
        coverage = img_area / page_area if page_area > 0 else 0

        # Логика: если текст достаточный — добавляем, иначе считаем сканом
        if text_len >= TEXT_THRESHOLD or coverage < IMAGE_COVERAGE:
            collected_texts.append(f"=== Страница {page_number} ===\n{text}")
    
    doc.close()
    if collected_texts:
        return "\n\n".join(collected_texts)
    else:
        return None


def main_pdf_scan_detector(pdf_path):
    try:
        return extract_text_if_not_scan(pdf_path)
    except Exception as e:
        print(f"[Error]: pdf_scan_detector ошибка извлечения текста: {pdf_path}\n{e}")
        return None


if __name__ == "__main__":
    # scan
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10663 від 14 09 2024.pdf"     
    
    # EDI text
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10662 від 14 09 2024.pdf" 
    
    # Medoc 3 печати на 1 стр
    pdf_path = r"C:\Rasim\Python\Medoc\31316718\202411\ПН\ПН 122320 20 11 2024.pdf"
    result = main_pdf_scan_detector(pdf_path)
    print(result)
    
