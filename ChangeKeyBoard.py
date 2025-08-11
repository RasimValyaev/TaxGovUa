# -*- coding: utf-8 -*-
"""
Скрипт для автоматического переключения раскладки клавиатуры на английскую.
"""

import ctypes
from ctypes import wintypes

# Определение необходимых функций и констант из библиотеки user32
user32 = ctypes.WinDLL('user32', use_last_error=True)

# Определение типов для функций
GetForegroundWindow = user32.GetForegroundWindow
GetWindowThreadProcessId = user32.GetWindowThreadProcessId
GetKeyboardLayout = user32.GetKeyboardLayout
PostMessageW = user32.PostMessageW

# Константы
WM_INPUTLANGCHANGEREQUEST = 0x0050
HKL_NEXT = 0x00000001

# Функция для получения текущей раскладки клавиатуры
def get_keyboard_layout():
    hwnd = GetForegroundWindow()  # Получаем идентификатор активного окна
    thread_id = wintypes.DWORD()
    GetWindowThreadProcessId(hwnd, ctypes.byref(thread_id))  # Получаем идентификатор потока окна
    layout_id = GetKeyboardLayout(thread_id.value)  # Получаем раскладку клавиатуры
    return layout_id & 0xFFFF  # Возвращаем идентификатор раскладки

# Функция для изменения раскладки клавиатуры
def set_keyboard_layout(layout=0x0409):  # 0x0409 — код английской раскладки
    hwnd = GetForegroundWindow()  # Получаем идентификатор активного окна
    thread_id = wintypes.DWORD()
    GetWindowThreadProcessId(hwnd, ctypes.byref(thread_id))  # Получаем идентификатор потока окна
    PostMessageW(hwnd, WM_INPUTLANGCHANGEREQUEST, 0, layout)

# Основная логика: проверка текущей раскладки и переключение на английскую, если необходимо
current_layout = get_keyboard_layout()
if current_layout != 0x0409:  # Если текущая раскладка не английская
    set_keyboard_layout()  # Меняем на английскую
    print("Переключено на английскую раскладку.")
else:
    print("Раскладка уже английская.")
