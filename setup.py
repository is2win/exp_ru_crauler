#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="excel_crawler",
    version="1.0.0",
    description="Краулер для обработки Excel файлов и сохранения данных в SQLite",
    author="Excel Crawler Team",
    py_modules=["crawler"],
    install_requires=[
        "python-dotenv>=1.0.0",
        "pandas>=2.0.3",
        "openpyxl>=3.1.2",
        "numpy>=1.24.3",
        "SQLAlchemy>=2.0.25",
    ],
    entry_points={
        "console_scripts": [
            "excel_crawler=crawler:main",
        ],
    },
) 