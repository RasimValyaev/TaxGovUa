# -*- coding: utf-8 -*-
"""
# GeminiDocSignNo.py
Скрипт для проверки, есть ли подпись и/или печать покупателя в документе. В т.ч цифровая подпись.
Для работы использует Gemini.
Актуально для документов "Видаткова накладна" скачанных из EDIN и сканированных документов.
"""


# pip install google-genai

import base64
import os
from os.path import exists

from google import genai
from google.genai import types
from os.path import exists

from dotenv import load_dotenv

load_dotenv()


def encode_pdf(pdf_path: str):
    """Encode the pdf to base64."""
    try:
      if not os.path.exists(pdf_path):
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
    if not pdf_decoded and pdf_path:
        pdf_decoded = encode_pdf(pdf_path)
    
    if pdf_decoded:
        mime_type = 'application/pdf'
    else:
        mime_type = get_mime_type(pdf_path)
    
    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY_PRESTIGE"),
    )

    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_bytes(
                    mime_type=mime_type,
                    data=base64.b64decode(pdf_decoded),
                ),                
                types.Part.from_text(text="""**Твоя задача:** Очень внимательно проанализировать документ на украинском языке и извлечь данные строго по правилам ниже.

### **Шаг 1: Идентификация Покупателя (Ключевой шаг)**

1.  Найди раздел \"Покупець:\" (или \"Одержувач:\").
2.  Извлеки и **запомни** полное наименование и **код ЄДРПОУ покупателя**. Этот код является **ключевым идентификатором** для последующих проверок.
3.  Если код ЄДРПОУ покупателя не указан явно рядом с его наименованием, но есть в документе в другом месте, **не используй его**, если он не связан напрямую с покупателем в контексте его идентификации.

### **Шаг 2: Определение, получен ли товар**

**КРИТИЧЕСКИ ВАЖНО:** Анализ получения товара выполняется в два этапа. Применяй эти правила **СТРОГО ПО ПОРЯДКУ**. Если правило №1 дает однозначный ответ (товар получен или не получен), **не переходи** к правилу №2.

---

#### **Правило №1: Проверка по ЄДРПОУ в разделе \"Власник\" (или аналогичном в подвале)**

Это правило имеет наивысший приоритет.

*   **Как проверять:** Сравни ЄДРПОУ, найденный в этом разделе, с **кодом ЄДРПОУ покупателя**, который ты определил на Шаге 1.

*   **Условие для \"товар получен\":**
    *   Если в этом разделе **явно и недвусмысленно указан ЄДРПОУ ПОКУПАТЕЛЯ** (тот, что был определен на Шаге 1).
    *   **Результат:** `sign: true`. На этом проверка завершается.

*   **Условие для \"товар не получен\":**
    *   Если в этом разделе **явно указан только ЄДРПОУ ПОСТАВЩИКА** (и отсутствует ЄДРПОУ покупателя).
    *   **Результат:** `sign: false`. На этом проверка завершается.

*   **Условие для игнорирования правила:**
    *   Если в разделе нет релевантных кодов ЄДРПОУ (ни покупателя, ни поставщика), или информация неоднозначна.
    *   **Действие:** Игнорируй это правило и переходи к Правилу №2.

*   **ЗАПРЕЩЕНО:**
    *   **НИКОГДА** не принимай ЄДРПОУ поставщика за подтверждение получения. Наличие ЄДРПОУ поставщика в этом разделе, при отсутствии ЄДРПОУ покупателя, является прямым указанием на то, что товар **НЕ ПОЛУЧЕН** по этому правилу.

---

#### **Правило №2: Проверка наличия физической печати в разделе \"Отримав(ла)\"**

Это правило применяется **только если Правило №1 было проигнорировано**.

*   **Условие для \"товар получен\":**
    *   Если в поле \"Отримав(ла)\" стоит **физическая печать** (четкий оттиск, а не просто текст или подпись).
    *   **Результат:** `sign: true`.

*   **Условие для \"товар не получен\":**
    *   Если в поле \"Отримав(ла)\" **отсутствует физическая печать** (поле пустое, содержит только подпись, текст или прочерк).
    *   **Результат:** `sign: false`.

---

### **Шаг 3: Финальный вывод**

*   Если результат Правила №1 - \"товар получен\" -> `\"sign\": true`.
*   Если результат Правила №1 - \"товар не получен\" -> `\"sign\": false`.
*   Если Правило №1 было проигнорировано, и результат Правила №2 - \"товар получен\" -> `\"sign\": true`.
*   Если Правило №1 было проигнорировано, и результат Правила №2 - \"товар не получен\" -> `\"sign\": false`.
*   Если оба правила не дали результата (например, оба раздела отсутствуют) -> `\"sign\": false`.

### **Вывод: строго в формате валидного JSON**

```json
[
  {
    \"doc_date\": \"дата документа в формате dd.mm.yyyy\",
    \"doc_number\": \"номер документа\",
    \"doc_type\": \"тип документа\", // не путай "Замовлення" и "Підтвердження замовлення". Будь предельно внимателен.
    \"sign\": True|False // (строго по правилам выше)
  }
]
```"""),
            ],
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
                    description = "в формате dd.mm.yyyy",
                ),
                "doc_number": genai.types.Schema(
                    type = genai.types.Type.STRING,
                ),
                "doc_type": genai.types.Schema(
                    type = genai.types.Type.STRING,
                    description = "Тип документа. не путай 'Замовлення' и 'Підтвердження замовлення'. Будь предельно внимателен.",
                ),
                "sign": genai.types.Schema(
                    type = genai.types.Type.BOOLEAN,
                    description = "True - Есть-ли подпись получателя",
                ),
                # "status": genai.types.Schema(
                #     type = genai.types.Type.STRING,
                #     description = "Коротко, Если есть у документа 'статус'",
                # ),
            },
        ),
    )

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        print(chunk.text, end="")


if __name__ == "__main__":
    pdf_path = r"C:\Rasim\Python\TaxGovUa\37470510\202412\Підтвердження замовлення\Підтвердження замовлення №67-0007226 від 06 12 2024.pdf"
    extract_entity_by_gemini(pdf_path)
