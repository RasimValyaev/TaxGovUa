# -*- coding: utf-8 -*-
"""
# GeminiDocSignNo.py
Скрипт для проверки, есть ли подпись и/или печать покупателя в документе. В т.ч цифровая подпись.
Для работы использует Gemini.
Актуально для документов "Видаткова накладна" скачанных из EDIN и сканированных документов.
"""


# pip install google-generativeai
# pip install google-genai

import base64
import os
from os.path import exists
import time
# import google.generativeai as genai
from google import genai
from google.genai import types
from os.path import exists

from dotenv import load_dotenv

load_dotenv()


def encode_pdf(pdf_path: str):
    """Encode the pdf to base64."""
    try:
      if not os.path.exists(pdf_path):
        print(f"Файл не найден: {pdf_path}")
        return None
      with open(pdf_path, "rb") as pdf_file:
          return base64.b64encode(pdf_file.read()).decode("utf-8")
    except FileNotFoundError:
        print(f"Error: The file {pdf_path} was not found.")
        return None
    except Exception as e:  # Added general exception handling
        print(f"Error: {e}")
        return None


def get_pages_count(file_path):
    if get_file_extension(file_path) in ['.pdf']:
        # количество страниц в pdf
        doc = fitz.open(file_path)
        pages_count = len(doc)
        doc.close()
        return pages_count
    else:
        return 1


def get_file_extension(file_path: str) -> str:
    if not exists(file_path):
        print(f"Файл не найден: {file_path}")
        return None

    _, ext = os.path.splitext(file_path)
    return ext.lower().lstrip('.')


def get_mime_type(file_path: str) -> str:
    ext = get_file_extension(file_path)
    if ext == 'pdf':
        return 'application/pdf'
    elif ext in ['png', 'bmp', 'tiff']:
        return f'image/{ext}'
    elif ext in ['jpg', 'jpeg']:
        return f'image/jpeg'

    return 'text/plain'


def extract_entity_by_gemini(pdf_path: str=None, pdf_decoded: str=None):
    if not pdf_decoded and not pdf_path:
        print("Нет данных для обработки.")
        return None
    
    if pdf_decoded and not pdf_path:
        mime_type = 'application/pdf'
        
    if not pdf_decoded and pdf_path:
        pdf_decoded = encode_pdf(pdf_path)
        mime_type = get_mime_type(pdf_path)
    
    if not pdf_decoded:
        print("Не получен pdf_decoded.")
        return None
    
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
    )

    # time.sleep(5)
    
    model = "gemini-2.0-flash" # "gemini-2.0-flash"  # limit 15RPM 1500 RPD
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_bytes(
                    mime_type=mime_type,
                    data=base64.b64decode(pdf_decoded),
                ),],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        temperature=0,
        response_mime_type="application/json",
        response_schema=genai.types.Schema(
            type = genai.types.Type.OBJECT,
            required = ["doc_date", "doc_number", "doc_type", "sign"],  # , "status"],
            properties = {
                "doc_date": genai.types.Schema(
                    type = genai.types.Type.STRING,
                    description = "дата документа в формате dd.mm.yyyy",
                ),
                "doc_number": genai.types.Schema(
                    type = genai.types.Type.STRING,
                    description = "номер документа",
                ),
                "doc_type": genai.types.Schema(
                    type = genai.types.Type.STRING,
                    description = "Тип документа. не путай 'Замовлення' и 'Підтвердження замовлення'. Будь предельно внимателен.",
                ),
                "sign": genai.types.Schema(
                    type = genai.types.Type.BOOLEAN,
                    description = "True - Есть-ли подпись/печать получателя. НЕ ПУТАТЬ ПОДПИСЬ С ПОКУПАТЕЛЯ С ПОДПИСЬЮ ПОСТАВЩИКА.",
                ),
            },
        ),
    )
    print(f"Количество токенов входящих: {client.models.count_tokens(model=model, contents=contents)}")
    response = client.models.generate_content(model=model, contents=contents, config=generate_content_config)
    print(f'prompt_token_count: {response.usage_metadata.prompt_token_count}')
    print(f'candidates_token_count: {response.usage_metadata.candidates_token_count}')
    print(f"total_token_count: {response.usage_metadata.total_token_count}")
    return response.text


if __name__ == "__main__":
    # EDI refused
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\No Sign\Видаткова накладна №10896 від 21 09 2024 Refused.pdf" 
    
    # scan
    # pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10658 від 14 09 2024.pdf" 
    
    # EDI sign
    pdf_path = r"c:\Users\Rasim\Desktop\Разблокировка\32490244_ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ ЕПІЦЕНТР К\202409\Видаткова накладна\Видаткова накладна №10448 від 05 09 2024.pdf" 
    
    result = extract_entity_by_gemini(pdf_path)
    print(result)
