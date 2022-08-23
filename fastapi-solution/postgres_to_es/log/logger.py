"""Модуль для логирования"""
import logging

logging.basicConfig(filename="log/pg_elastic.log", level=logging.INFO)
log = logging.getLogger('base')
