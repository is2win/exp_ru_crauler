#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для удобного запуска краулера
"""

import os
import sys
import time
from datetime import datetime
import logging

# Проверяем наличие файла .env
if not os.path.exists('.env'):
    print("Ошибка: Файл .env не найден!")
    print("Создаю файл .env с примером настройки...")
    with open('.env', 'w', encoding='utf-8') as f:
        f.write("SEARCH_DIR=/path/to/your/folder\n")
        f.write("DB_PATH=/path/to/database.db\n")
    print("Пожалуйста, отредактируйте файл .env и укажите правильный путь к папке с файлами Excel.")
    sys.exit(1)
else:
    # Проверяем, содержит ли .env необходимые переменные
    with open('.env', 'r', encoding='utf-8') as f:
        env_content = f.read()
    
    if 'SEARCH_DIR=' not in env_content:
        print("Внимание: В файле .env не найдена переменная SEARCH_DIR!")
        print("Добавляем переменную SEARCH_DIR в .env...")
        with open('.env', 'a', encoding='utf-8') as f:
            f.write("\nSEARCH_DIR=/path/to/your/folder\n")
        print("Пожалуйста, отредактируйте файл .env и укажите правильный путь к папке с файлами Excel.")
    
    if 'DB_PATH=' not in env_content:
        print("Внимание: В файле .env не найдена переменная DB_PATH!")
        print("Добавляем переменную DB_PATH в .env...")
        with open('.env', 'a', encoding='utf-8') as f:
            f.write("\nDB_PATH=/path/to/database.db\n")
        print("При необходимости отредактируйте путь к базе данных в файле .env.")

# Импортируем основной модуль
try:
    from crawler import main
except ImportError:
    print("Ошибка: Не найден модуль crawler.py!")
    sys.exit(1)

if __name__ == "__main__":
    print(f"Запуск краулера {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\nРабота краулера прервана пользователем.")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    
    end_time = time.time()
    print(f"Время выполнения: {end_time - start_time:.2f} секунд")
    print("Для подробностей смотрите логи в папке 'logs'") 