# -*- coding: utf-8 -*-
"""
Скрипт для автоматического обновления ChromeDriver до версии, соответствующей установленной версии Chrome.
"""

import os
import re
import sys
import zipfile
import subprocess
import requests
from pathlib import Path

def get_chrome_version():
    """
    Получает версию Chrome, установленную на компьютере.
    Возвращает строку с версией (например, '135.0.7049.43').
    """
    try:
        # Для Windows
        if sys.platform == 'win32':
            # Путь к Chrome по умолчанию
            chrome_path = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
            if not os.path.exists(chrome_path):
                # Альтернативный путь
                chrome_path = r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'
            
            # Получаем версию Chrome с помощью команды wmic
            chrome_path_escaped = chrome_path.replace("\\", "\\\\")
            command = 'wmic datafile where name="' + chrome_path_escaped + '" get Version /value'
            output = subprocess.check_output(command, shell=True).decode('utf-8')
            
            # Извлекаем версию из вывода
            match = re.search(r'Version=(\d+\.\d+\.\d+\.\d+)', output)
            if match:
                return match.group(1)
        
        # Для Linux
        elif sys.platform.startswith('linux'):
            output = subprocess.check_output(['google-chrome', '--version']).decode('utf-8')
            match = re.search(r'Chrome\s+(\d+\.\d+\.\d+\.\d+)', output)
            if match:
                return match.group(1)
        
        # Для macOS
        elif sys.platform == 'darwin':
            output = subprocess.check_output(['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '--version']).decode('utf-8')
            match = re.search(r'Chrome\s+(\d+\.\d+\.\d+\.\d+)', output)
            if match:
                return match.group(1)
    
    except Exception as e:
        print(f"Ошибка при получении версии Chrome: {e}")
    
    return None

def get_major_version(version):
    """
    Извлекает основную версию из полной версии.
    Например, из '135.0.7049.43' получаем '135'.
    """
    if version:
        return version.split('.')[0]
    return None

def download_chromedriver(chrome_version):
    """
    Скачивает ChromeDriver, соответствующий версии Chrome.
    
    Args:
        chrome_version: Полная версия Chrome (например, '135.0.7049.43')
    
    Returns:
        Путь к скачанному файлу или None в случае ошибки
    """
    try:
        # Получаем основную версию Chrome
        major_version = get_major_version(chrome_version)
        
        # Для Chrome 115+ используем Chrome for Testing
        if int(major_version) >= 115:
            # Получаем список доступных версий ChromeDriver
            response = requests.get('https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json')
            versions_data = response.json()
            
            # Находим подходящую версию для нашей основной версии Chrome
            matching_versions = []
            for version_info in versions_data['versions']:
                if version_info['version'].startswith(f"{major_version}."):
                    matching_versions.append(version_info)
            
            if not matching_versions:
                print(f"Не найдены подходящие версии ChromeDriver для Chrome {major_version}")
                return None
            
            # Берем последнюю подходящую версию
            latest_version = matching_versions[-1]
            
            # Определяем платформу
            platform = 'win64' if sys.platform == 'win32' else 'linux64' if sys.platform.startswith('linux') else 'mac-arm64' if sys.platform == 'darwin' and os.uname().machine == 'arm64' else 'mac-x64'
            
            # Находим URL для скачивания ChromeDriver
            download_url = None
            for download in latest_version['downloads'].get('chromedriver', []):
                if download['platform'] == platform:
                    download_url = download['url']
                    break
            
            if not download_url:
                print(f"Не найден URL для скачивания ChromeDriver для платформы {platform}")
                return None
            
            # Скачиваем ChromeDriver
            print(f"Скачиваем ChromeDriver версии {latest_version['version']} для Chrome {major_version}...")
            zip_path = os.path.join(os.getcwd(), "chromedriver-temp.zip")
            response = requests.get(download_url, stream=True)
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            return zip_path
        
        # Для более старых версий Chrome используем старый метод
        else:
            print(f"Версия Chrome {major_version} не поддерживается этим скриптом. Требуется Chrome 115+.")
            return None
    
    except Exception as e:
        print(f"Ошибка при скачивании ChromeDriver: {e}")
        return None

def extract_chromedriver(zip_path, target_dir):
    """
    Распаковывает ChromeDriver из zip-архива в указанную директорию.
    
    Args:
        zip_path: Путь к zip-архиву с ChromeDriver
        target_dir: Директория, куда нужно распаковать ChromeDriver
    
    Returns:
        True в случае успеха, False в случае ошибки
    """
    try:
        # Создаем временную директорию для распаковки
        temp_dir = os.path.join(os.getcwd(), "chromedriver-temp")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Распаковываем архив
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Находим chromedriver.exe в распакованных файлах
        chromedriver_exe = None
        for root, dirs, files in os.walk(temp_dir):
            if "chromedriver.exe" in files:
                chromedriver_exe = os.path.join(root, "chromedriver.exe")
                break
        
        if not chromedriver_exe:
            print("Не удалось найти chromedriver.exe в распакованных файлах")
            return False
        
        # Копируем chromedriver.exe в целевую директорию
        import shutil
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, "chromedriver.exe")
        shutil.copy2(chromedriver_exe, target_path)
        
        print(f"ChromeDriver успешно установлен в {target_path}")
        
        # Удаляем временные файлы
        try:
            os.remove(zip_path)
            shutil.rmtree(temp_dir)
        except:
            pass
        
        return True
    
    except Exception as e:
        print(f"Ошибка при распаковке ChromeDriver: {e}")
        return False

def update_chromedriver_if_needed():
    """
    Проверяет соответствие версий Chrome и ChromeDriver.
    Если версии не соответствуют, скачивает и устанавливает подходящий ChromeDriver.
    
    Returns:
        True, если ChromeDriver соответствует версии Chrome или был успешно обновлен.
        False в случае ошибки.
    """
    try:
        # Получаем текущую директорию скрипта
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Путь к ChromeDriver
        chromedriver_dir = os.path.join(Path(cur_dir).parent.__str__(), "chromedriver-win64")
        chromedriver_path = os.path.join(chromedriver_dir, "chromedriver.exe")
        
        # Получаем версию Chrome
        chrome_version = get_chrome_version()
        if not chrome_version:
            print("Не удалось определить версию Chrome")
            return False
        
        print(f"Обнаружена версия Chrome: {chrome_version}")
        
        # Проверяем, существует ли ChromeDriver
        if not os.path.exists(chromedriver_path):
            print(f"ChromeDriver не найден по пути {chromedriver_path}")
            # Скачиваем и устанавливаем ChromeDriver
            zip_path = download_chromedriver(chrome_version)
            if zip_path:
                return extract_chromedriver(zip_path, chromedriver_dir)
            return False
        
        # Проверяем версию ChromeDriver
        try:
            output = subprocess.check_output([chromedriver_path, "--version"]).decode('utf-8')
            match = re.search(r'ChromeDriver\s+(\d+\.\d+\.\d+\.\d+)', output)
            if match:
                chromedriver_version = match.group(1)
                print(f"Обнаружена версия ChromeDriver: {chromedriver_version}")
                
                # Проверяем соответствие основных версий
                chrome_major = get_major_version(chrome_version)
                chromedriver_major = get_major_version(chromedriver_version)
                
                if chrome_major == chromedriver_major:
                    print("Версии Chrome и ChromeDriver совпадают")
                    return True
                else:
                    print(f"Версии не совпадают: Chrome {chrome_major}, ChromeDriver {chromedriver_major}")
                    # Скачиваем и устанавливаем подходящий ChromeDriver
                    zip_path = download_chromedriver(chrome_version)
                    if zip_path:
                        return extract_chromedriver(zip_path, chromedriver_dir)
        except Exception as e:
            print(f"Ошибка при проверке версии ChromeDriver: {e}")
            # Скачиваем и устанавливаем ChromeDriver
            zip_path = download_chromedriver(chrome_version)
            if zip_path:
                return extract_chromedriver(zip_path, chromedriver_dir)
        
        return False
    
    except Exception as e:
        print(f"Ошибка при обновлении ChromeDriver: {e}")
        return False

if __name__ == "__main__":
    update_chromedriver_if_needed()
