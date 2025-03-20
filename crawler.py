#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import pandas as pd
import sqlite3
from dotenv import load_dotenv
import json
import hashlib
import logging
import sys
import time
from datetime import datetime

# Настройка логирования
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Загрузка переменных окружения из .env
load_dotenv()

def find_excel_files(directory):
    """
    Находит все Excel файлы, содержащие 'РР_исполнения-ДС' в имени, в указанной директории.
    """
    excel_files = []
    pattern = re.compile(r'.*РР_исполнения-ДС.*\.(xlsx|xls)$', re.IGNORECASE)
    
    try:
        for file in os.listdir(directory):
            if pattern.match(file):
                excel_files.append(os.path.join(directory, file))
        
        logging.info(f"Найдено {len(excel_files)} файлов Excel с 'РР_исполнения-ДС' в имени.")
        return excel_files
    except Exception as e:
        logging.error(f"Ошибка при поиске файлов Excel: {e}")
        return []

def sanitize_column_name(name):
    """
    Очищает имя столбца для использования в SQL.
    """
    # Заменяем недопустимые символы на подчеркивание
    name = re.sub(r'[^\w\d_]', '_', str(name))
    # Если имя начинается с цифры, добавляем префикс
    if name and name[0].isdigit():
        name = 'col_' + name
    # Если имя пустое, используем значение по умолчанию
    if not name:
        name = 'unnamed_column'
    return name

def create_table_for_sheet(conn, sheet_name, df):
    """
    Создает таблицу для листа Excel с динамическими столбцами.
    """
    cursor = conn.cursor()
    
    # Получаем имена столбцов и очищаем их для SQL
    columns = [sanitize_column_name(col) for col in df.columns]
    
    # Проверяем, существует ли таблица
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{sheet_name}'")
    if cursor.fetchone() is None:
        # Создаем таблицу, если она не существует
        column_defs = [f'"{col}" TEXT' for col in columns]
        create_sql = f'''
        CREATE TABLE {sheet_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            row_hash TEXT,
            {', '.join(column_defs)}
        )
        '''
        cursor.execute(create_sql)
        
        # Создаем индекс для row_hash для быстрого поиска дубликатов
        cursor.execute(f"CREATE INDEX idx_{sheet_name}_row_hash ON {sheet_name}(row_hash)")
        
        logging.info(f"Таблица {sheet_name} успешно создана.")
    else:
        # Проверяем, соответствует ли структура таблицы текущим данным
        cursor.execute(f"PRAGMA table_info({sheet_name})")
        existing_columns = [row[1] for row in cursor.fetchall() if row[1] not in ('id', 'file_name', 'row_hash')]
        
        # Находим новые столбцы
        new_columns = [col for col in columns if f'"{col}"' not in existing_columns]
        
        # Добавляем новые столбцы, если они есть
        for col in new_columns:
            try:
                cursor.execute(f'ALTER TABLE {sheet_name} ADD COLUMN "{col}" TEXT')
                logging.info(f"Добавлен новый столбец '{col}' в таблицу {sheet_name}.")
            except Exception as e:
                logging.error(f"Ошибка при добавлении столбца '{col}': {e}")
    
    conn.commit()

def calculate_row_hash(row_data):
    """
    Вычисляет хеш строки для проверки уникальности.
    """
    row_str = json.dumps(row_data, sort_keys=True)
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def is_duplicate(conn, table_name, row_hash):
    """
    Проверяет, существует ли уже такая строка в таблице по хешу.
    """
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {table_name} WHERE row_hash = ?', (row_hash,))
    count = cursor.fetchone()[0]
    return count > 0

def process_excel_file(file_path, conn):
    """
    Обрабатывает Excel файл, читает все 4 листа и сохраняет данные в SQLite.
    """
    try:
        file_name = os.path.basename(file_path)
        logging.info(f"Обработка файла: {file_name}")
        
        # Читаем Excel файл
        excel_file = pd.ExcelFile(file_path)
        
        # Проверяем, что есть хотя бы 4 листа
        sheet_names = excel_file.sheet_names
        if len(sheet_names) < 4:
            logging.warning(f"Внимание: Файл {file_name} содержит меньше 4 листов ({len(sheet_names)}).")
        
        # Обрабатываем до 4 листов
        for i, sheet_name in enumerate(sheet_names[:4], 1):
            logging.info(f"  Обработка листа {i}: {sheet_name}")
            
            try:
                # Читаем лист
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Пропускаем пустые листы
                if df.empty:
                    logging.warning(f"  Лист {sheet_name} пуст. Пропускаем.")
                    continue
                
                # Преобразуем все данные в строковый формат и заменяем NaN на пустые строки
                df = df.astype(str).replace('nan', '')
                
                # Создаем или обновляем таблицу для этого листа
                table_name = f'sheet_{i}'
                create_table_for_sheet(conn, table_name, df)
                
                # Получаем очищенные имена столбцов
                columns = [sanitize_column_name(col) for col in df.columns]
                
                # Обрабатываем каждую строку
                cursor = conn.cursor()
                rows_total = 0
                rows_added = 0
                
                for _, row in df.iterrows():
                    rows_total += 1
                    
                    # Преобразуем данные строки в словарь
                    row_data = row.to_dict()
                    
                    # Вычисляем хеш для строки
                    row_hash = calculate_row_hash(row_data)
                    
                    # Проверяем на дубликаты
                    if not is_duplicate(conn, table_name, row_hash):
                        try:
                            # Подготавливаем значения для вставки
                            column_names = ', '.join([f'"{col}"' for col in columns])
                            placeholders = ', '.join(['?' for _ in columns])
                            values = [row_data[col] for col in df.columns]
                            
                            # Формируем SQL запрос для вставки
                            sql = f'''
                            INSERT INTO {table_name} (file_name, row_hash, {column_names})
                            VALUES (?, ?, {placeholders})
                            '''
                            
                            # Выполняем вставку
                            cursor.execute(sql, (file_name, row_hash, *values))
                            rows_added += 1
                        except Exception as e:
                            logging.error(f"Ошибка при вставке строки в таблицу {table_name}: {e}")
                
                conn.commit()
                logging.info(f"    Добавлено {rows_added} из {rows_total} строк в таблицу {table_name}.")
            
            except Exception as e:
                logging.error(f"Ошибка при обработке листа {sheet_name}: {e}")
        
        return True
    except Exception as e:
        logging.error(f"Ошибка при обработке файла {file_path}: {e}")
        return False

def main():
    start_time = time.time()
    logging.info("Начало работы краулера")
    
    # Получаем путь к директории из переменных окружения
    search_dir = os.getenv("SEARCH_DIR")
    if not search_dir:
        logging.error("Ошибка: Не указан путь к директории в файле .env (SEARCH_DIR)")
        return
    
    # Проверяем, существует ли директория
    if not os.path.exists(search_dir) or not os.path.isdir(search_dir):
        logging.error(f"Ошибка: Директория не существует: {search_dir}")
        return
    
    logging.info(f"Поиск Excel файлов в директории: {search_dir}")
    
    # Получаем путь к базе данных
    db_path = os.getenv("DB_PATH", "database.db")
    
    # Если путь содержит директории, проверяем их существование
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir)
            logging.info(f"Создана директория для базы данных: {db_dir}")
        except Exception as e:
            logging.error(f"Ошибка при создании директории для базы данных: {e}")
            db_path = "database.db"  # Используем дефолтный путь в случае ошибки
            logging.info(f"Используется путь по умолчанию: {db_path}")
    
    logging.info(f"База данных будет сохранена в: {os.path.abspath(db_path)}")
    
    # Подключаемся к базе данных SQLite
    try:
        conn = sqlite3.connect(db_path)
        
        # Находим все подходящие Excel файлы
        excel_files = find_excel_files(search_dir)
        
        if not excel_files:
            logging.warning("Не найдено подходящих Excel файлов.")
            return
        
        # Обрабатываем каждый файл
        success_count = 0
        for i, file_path in enumerate(excel_files, 1):
            logging.info(f"Обработка файла {i} из {len(excel_files)}: {os.path.basename(file_path)}")
            if process_excel_file(file_path, conn):
                success_count += 1
        
        # Выводим статистику
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Обработка завершена за {elapsed_time:.2f} секунд.")
        logging.info(f"Успешно обработано {success_count} из {len(excel_files)} файлов.")
        logging.info(f"База данных сохранена в файл: {os.path.abspath(db_path)}")
        
        # Закрываем соединение с базой данных
        conn.close()
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Работа краулера прервана пользователем.")
    except Exception as e:
        logging.critical(f"Критическая ошибка: {e}")
        raise 