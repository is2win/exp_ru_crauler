#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для выполнения запросов к базе данных SQLite.
Позволяет выбрать данные из всех листов, используя фильтрацию по одному столбцу.
Результаты выводятся в табличном виде для каждого листа отдельно и сохраняются в файл.
"""

import os
import sqlite3
import argparse
from dotenv import load_dotenv
from tabulate import tabulate
import pandas as pd
import sys
import datetime

# Загрузка переменных окружения из .env
load_dotenv()

# Словарь с названиями листов
SHEET_NAMES = {
    'sheet_1': 'Исполнения ДС',
    'sheet_2': 'DELIV исполнение ДС',
    'sheet_3': 'Купоны',
    'sheet_4': 'zero купоны'
}

def get_db_path():
    """Получает путь к базе данных из .env файла или использует значение по умолчанию."""
    db_path = os.getenv("DB_PATH", "database.db")
    if not os.path.exists(db_path):
        print(f"Ошибка: Файл базы данных не найден по пути: {db_path}")
        sys.exit(1)
    return db_path

def get_table_info(conn, table_name):
    """Получает информацию о столбцах таблицы."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall() if row[1] not in ('id', 'file_name', 'row_hash')]
    return columns

def get_common_columns(conn, tables):
    """Находит столбцы, общие для всех указанных таблиц."""
    common_columns = None
    
    for table in tables:
        columns = set(get_table_info(conn, table))
        if common_columns is None:
            common_columns = columns
        else:
            common_columns = common_columns.intersection(columns)
    
    return list(common_columns) if common_columns else []

def execute_query(conn, table_name, column_name, search_value, exact_match=True):
    """Выполняет запрос к указанной таблице с фильтрацией по столбцу."""
    cursor = conn.cursor()
    
    # Проверяем существование таблицы
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        print(f"Таблица {table_name} не существует в базе данных.")
        return None
    
    # Проверяем существование столбца в таблице
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name not in columns:
        print(f"Столбец '{column_name}' не существует в таблице {table_name}.")
        return None
    
    # Формируем запрос в зависимости от типа соответствия
    if exact_match:
        query = f'SELECT * FROM {table_name} WHERE "{column_name}" = ?'
    else:
        query = f'SELECT * FROM {table_name} WHERE "{column_name}" LIKE ?'
        search_value = f'%{search_value}%'
    
    # Выполняем запрос
    try:
        cursor.execute(query, (search_value,))
        rows = cursor.fetchall()
        
        # Если есть результаты, создаем DataFrame
        if rows:
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            return df
        else:
            return None
    except sqlite3.Error as e:
        print(f"Ошибка при выполнении запроса к таблице {table_name}: {e}")
        return None

def print_results(df, table_name):
    """Выводит результаты запроса в виде таблицы."""
    sheet_display_name = SHEET_NAMES.get(table_name, table_name)
    
    if df is not None and not df.empty:
        print(f"\n=== Результаты из листа: {sheet_display_name} ===")
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
        print(f"Всего строк: {len(df)}")
    else:
        print(f"\nВ листе '{sheet_display_name}' не найдено записей, соответствующих критерию.")

def save_results_to_file(results, column_name, search_value, exact_match):
    """Сохраняет результаты в текстовый файл."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    match_type = "exact" if exact_match else "partial"
    filename = f"search_results_{column_name}_{match_type}_{timestamp}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"Результаты поиска по столбцу: {column_name}\n")
        f.write(f"Искомое значение: {search_value}\n")
        f.write(f"Тип поиска: {'Точное соответствие' if exact_match else 'Частичное соответствие'}\n")
        f.write(f"Дата и время поиска: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*60 + "\n\n")
        
        results_found = False
        
        for table_name, df in results.items():
            sheet_display_name = SHEET_NAMES.get(table_name, table_name)
            
            if df is not None and not df.empty:
                results_found = True
                f.write(f"### Лист: {sheet_display_name} ###\n\n")
                
                # Выбираем только интересующие колонки (без служебных)
                columns_to_display = [col for col in df.columns if col not in ('id', 'row_hash')]
                
                # Для каждой строки выводим данные в формате "колонка: значение"
                for idx, row in df.iterrows():
                    for col in columns_to_display:
                        value = str(row[col]).strip()
                        if value and value != 'nan':  # Пропускаем пустые значения
                            f.write(f"{col}: {value}\n")
                    f.write("\n")  # Пустая строка между записями
                
                f.write("-"*60 + "\n\n")
        
        if not results_found:
            f.write("Не найдено записей, соответствующих критерию поиска.\n")
    
    print(f"\nРезультаты сохранены в файл: {filename}")
    return filename

def interactive_mode():
    """Запускает интерактивный режим запросов."""
    print("\n=== Интерактивный режим поиска в базе данных ===\n")
    
    # Получаем путь к базе данных
    db_path = get_db_path()
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        
        # Получаем список таблиц
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sheet_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("В базе данных не найдено таблиц с данными листов (sheet_*).")
            return
        
        # Получаем общие столбцы для всех таблиц
        common_cols = get_common_columns(conn, tables)
        
        # Выводим список общих столбцов
        if common_cols:
            print("Доступные столбцы для поиска:")
            for i, col in enumerate(common_cols, 1):
                print(f"{i}. {col}")
        else:
            print("Не найдено общих столбцов для всех таблиц. Показываем столбцы первой таблицы:")
            columns = get_table_info(conn, tables[0])
            for i, col in enumerate(columns, 1):
                print(f"{i}. {col}")
            common_cols = columns
        
        # Запрашиваем столбец для поиска
        while True:
            col_input = input("\nВведите номер или имя столбца для поиска: ")
            
            if col_input.isdigit() and 1 <= int(col_input) <= len(common_cols):
                column_name = common_cols[int(col_input) - 1]
                break
            elif col_input in common_cols:
                column_name = col_input
                break
            else:
                print("Некорректный ввод. Пожалуйста, введите номер или точное имя столбца из списка.")
        
        # Запрашиваем значение для поиска
        search_value = input(f"\nВведите значение для поиска в столбце '{column_name}': ")
        
        # Запрашиваем тип соответствия
        match_type = input("\nВыберите тип поиска (1 - Точное соответствие, 2 - Частичное совпадение): ")
        exact_match = match_type != '2'  # По умолчанию используем точное соответствие
        
        print(f"\nВыполняется поиск {'точного' if exact_match else 'частичного'} соответствия '{search_value}' в столбце '{column_name}'...")
        
        # Выполняем запросы к каждой таблице
        results = {}
        results_found = False
        
        for table in tables:
            df = execute_query(conn, table, column_name, search_value, exact_match)
            results[table] = df
            print_results(df, table)
            if df is not None and not df.empty:
                results_found = True
        
        if not results_found:
            print(f"\nНе найдено записей, соответствующих критерию '{column_name} = {search_value}' ни в одной таблице.")
        
        # Спрашиваем, нужно ли сохранить результаты в файл
        save_option = input("\nСохранить результаты в файл? (y/n): ")
        if save_option.lower() in ('y', 'д', 'yes', 'да'):
            filename = save_results_to_file(results, column_name, search_value, exact_match)
            print(f"Файл {filename} успешно создан.")
        
    except sqlite3.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        if conn:
            conn.close()

def main():
    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Поиск данных в базе SQLite по всем листам.')
    parser.add_argument('--column', '-c', type=str, help='Имя столбца для поиска')
    parser.add_argument('--value', '-v', type=str, help='Значение для поиска')
    parser.add_argument('--partial', '-p', action='store_true', help='Использовать частичное соответствие (LIKE)')
    parser.add_argument('--tables', '-t', type=str, nargs='+', default=['sheet_1', 'sheet_2', 'sheet_3', 'sheet_4'], 
                        help='Имена таблиц для поиска (по умолчанию все 4 листа)')
    parser.add_argument('--list-columns', '-l', action='store_true', help='Показать список столбцов в таблицах')
    parser.add_argument('--common', action='store_true', help='Показать общие столбцы для всех таблиц')
    parser.add_argument('--interactive', '-i', action='store_true', help='Запустить в интерактивном режиме')
    parser.add_argument('--save', '-s', action='store_true', help='Сохранить результаты в файл')
    
    args = parser.parse_args()
    
    # Запускаем интерактивный режим, если указан флаг или не заданы обязательные параметры
    if args.interactive or (not args.column and not args.value and not args.list_columns and not args.common):
        interactive_mode()
        return
    
    # Получаем путь к базе данных
    db_path = get_db_path()
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        
        # Если нужно только вывести список столбцов
        if args.list_columns:
            for table in args.tables:
                columns = get_table_info(conn, table)
                sheet_display_name = SHEET_NAMES.get(table, table)
                print(f"\nСтолбцы листа '{sheet_display_name}':")
                for i, col in enumerate(columns, 1):
                    print(f"{i}. {col}")
            return
        
        # Если нужно вывести общие столбцы
        if args.common:
            common_cols = get_common_columns(conn, args.tables)
            if common_cols:
                print("\nОбщие столбцы для всех листов:")
                for i, col in enumerate(common_cols, 1):
                    print(f"{i}. {col}")
            else:
                print("\nНет общих столбцов для указанных листов.")
            return
        
        # Проверяем обязательные параметры
        if not args.column or not args.value:
            print("Ошибка: Не указаны обязательные параметры --column и --value.")
            print("Используйте --help для получения справки или запустите скрипт без параметров для интерактивного режима.")
            return
        
        # Выполняем запросы к каждой таблице
        results = {}
        results_found = False
        
        for table in args.tables:
            df = execute_query(conn, table, args.column, args.value, not args.partial)
            results[table] = df
            print_results(df, table)
            if df is not None and not df.empty:
                results_found = True
        
        if not results_found:
            print(f"\nНе найдено записей, соответствующих критерию '{args.column} = {args.value}' ни в одной таблице.")
        
        # Сохраняем результаты в файл, если указан флаг
        if args.save and results_found:
            filename = save_results_to_file(results, args.column, args.value, not args.partial)
            print(f"Файл {filename} успешно создан.")
        
    except sqlite3.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 