#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для выполнения запросов к базе данных SQLite.
Позволяет выбрать данные из всех листов, используя фильтрацию по одному столбцу.
Результаты выводятся в табличном виде для каждого листа отдельно.
"""

import os
import sqlite3
import argparse
from dotenv import load_dotenv
from tabulate import tabulate
import pandas as pd
import sys

# Загрузка переменных окружения из .env
load_dotenv()

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
    if df is not None and not df.empty:
        print(f"\n=== Результаты из таблицы {table_name} ===")
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
        print(f"Всего строк: {len(df)}")
    else:
        print(f"\nВ таблице {table_name} не найдено записей, соответствующих критерию.")

def main():
    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Поиск данных в базе SQLite по всем листам.')
    parser.add_argument('--column', '-c', type=str, required=True, help='Имя столбца для поиска')
    parser.add_argument('--value', '-v', type=str, required=True, help='Значение для поиска')
    parser.add_argument('--partial', '-p', action='store_true', help='Использовать частичное соответствие (LIKE)')
    parser.add_argument('--tables', '-t', type=str, nargs='+', default=['sheet_1', 'sheet_2', 'sheet_3', 'sheet_4'], 
                        help='Имена таблиц для поиска (по умолчанию все 4 листа)')
    parser.add_argument('--list-columns', '-l', action='store_true', help='Показать список столбцов в таблицах')
    parser.add_argument('--common', action='store_true', help='Показать общие столбцы для всех таблиц')
    
    args = parser.parse_args()
    
    # Получаем путь к базе данных
    db_path = get_db_path()
    
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect(db_path)
        
        # Если нужно только вывести список столбцов
        if args.list_columns:
            for table in args.tables:
                columns = get_table_info(conn, table)
                print(f"\nСтолбцы таблицы {table}:")
                for i, col in enumerate(columns, 1):
                    print(f"{i}. {col}")
            return
        
        # Если нужно вывести общие столбцы
        if args.common:
            common_cols = get_common_columns(conn, args.tables)
            if common_cols:
                print("\nОбщие столбцы для всех таблиц:")
                for i, col in enumerate(common_cols, 1):
                    print(f"{i}. {col}")
            else:
                print("\nНет общих столбцов для указанных таблиц.")
            return
        
        # Выполняем запросы к каждой таблице
        results_found = False
        for table in args.tables:
            df = execute_query(conn, table, args.column, args.value, not args.partial)
            print_results(df, table)
            if df is not None and not df.empty:
                results_found = True
        
        if not results_found:
            print(f"\nНе найдено записей, соответствующих критерию '{args.column} = {args.value}' ни в одной таблице.")
        
    except sqlite3.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 