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
    # === ИЩЕМ КОД ПОСЛЕ "ОДЕРЖУВАЧ" ===
    buyer_code = ""
    recipient_start = text.find("Одержувач:")
    if recipient_start == -1:
        print("Рядок 'Одержувач:' не знайдено")
    else:
        # Ищем ПЕРВОЕ 8-значное число после "Одержувач:"
        match = re.search(r'\d{8}', text[recipient_start:])
        if match:
            buyer_code = match.group()
            # print(f"ЄДРПОУ одержувача: {buyer_code}")
        else:
            print("Код одержувача не знайдено")
    return buyer_code


def is_refused(text):
    if "відмовлено" in text.lower():
        return True
    return False


def is_buyer_signed(text):
    # проверка, если buyer_code есть в "Власник"
    # если есть, значит ЕЦП покупателем подписан
    buyer_code = get_buyer_code(text)
    if not buyer_code: return False  # Если код не найден, сразу возвращаем False

    owner_positions = [m.start() for m in re.finditer(r'Власник', text)]

    for i, pos in enumerate(owner_positions, 1):
        # Ищем ПЕРВОЕ 8-значное число после каждого "Власник"
        match = re.search(r'\d{8}', text[pos:])
        if match:
            current_code = match.group()  # Получаем найденное 8-значное число
            if current_code == buyer_code:
                return True
        else:
            print(f"{i}. Код не знайдено")
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
    pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10663 від 14 09 2024.pdf"     
    
    # EDI text
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10662 від 14 09 2024.pdf" 
    
    # Medoc 3 печати на 1 стр
    # pdf_path = r"C:\Rasim\Python\Medoc\31316718\202411\ПН\ПН 122320 20 11 2024.pdf"
    result = main_pdf_sign_detector(pdf_path)
    print(f"Результат: {result}")