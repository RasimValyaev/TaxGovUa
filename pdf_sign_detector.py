#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# pdf_sign_detector.py
Скрипт для проверки наличия подписи покупателя в документе.
Работает на вугруженных из EDIN PDF файлах - расходных накладных.
Исполуется pdf_scan_detector.py для извлечения текста из PDF.
"""

import re
from pdf_scan_detector import main_pdf_scan_detector


def get_buyer_code(text):
    recipient_match = re.search(r'одержувач', text, re.IGNORECASE)
    if not recipient_match:
        print("Рядок 'Одержувач' не знайдено")
    else:
        # Начинаем поиск с позиции после найденного слова
        start_pos = recipient_match.end()
        number_match = re.search(r'\d{8}', text[start_pos:])
        
        if number_match:
            return number_match.group()
        else:
            print("Код одержувача не знайдено")
    return None


def is_refused(text):
    if "відмовлено" in text.lower():
        return True
    return False


def is_buyer_signed(text):
    # проверка, если buyer_code есть в "Власник"
    # если есть, значит ЕЦП покупателем подписан
    buyer_code = get_buyer_code(text)
    if not buyer_code: return False  # Если код не найден, сразу возвращаем False

    owners = re.finditer(r'власник', text, re.IGNORECASE)

    for owner_match in owners:
        # Начинаем поиск с позиции после найденного слова
        start_pos = owner_match.end()
        number_match = re.search(r'\d{8}', text[start_pos:])
        
        if number_match and number_match.group() == buyer_code:
            return True
    return False


def main_pdf_sign_detector(pdf_file_path):
    text = main_pdf_scan_detector(pdf_file_path)
    if not text:
        print("Не вдалося отримати текст з PDF.")
        return "Error"
    
    if is_refused(text):
        print("Refused")
        return "Refused"
    
    result = is_buyer_signed(text)

    if result:
        print("signed")
        return "signed"
    
    print("NoSign")
    return "NoSign"

    
if __name__ == "__main__":
    # scan
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10663 від 14 09 2024.pdf"     
    
    # EDI text
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10662 від 14 09 2024.pdf" 
    
    # Medoc 3 печати на 1 стр
    # pdf_path = r"C:\Rasim\Python\Medoc\31316718\202411\ПН\ПН 122320 20 11 2024.pdf"
    
    # EDIN возврат
    pdf_path = r"C:\Rasim\Python\TaxGovUa\32490244\202409\Накладна на повернення\Накладна на повернення №Впб_PL-0003587_PL-0035259 від 12 09 2024 NoSign.pdf"
    result = main_pdf_sign_detector(pdf_path)
    print(f"Результат: {result}")