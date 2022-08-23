"""Модуль по загрузке данных из postgres"""
import dataclasses
from collections.abc import Iterator
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from config import PostgresSettings
from database.backoff import backoff
from database.database import DatabaseAdapter
from log.logger import log


class NoMoreDataInPG(Exception):
    ...


class PostgresExtractor(DatabaseAdapter):
    """Класс для загрузки данных из postgres"""
    def __init__(self, pg_conn: PostgresSettings, cursor_array_size: int) -> None:
        """Конструктор класса.

        Args:
            pg_conn: Dataclass с параметрами для подключения к postgres
            cursor_array_size: Размер данных в курсоре
        """
        self.pg_conn = pg_conn
        self._connection = None
        self.cursor_array_size = cursor_array_size

    @property
    def _conn(self) -> dict:
        if isinstance(self.pg_conn, PostgresSettings):
            return self.pg_conn.dict()

    def connected(self) -> bool:
        """Функция для проверки соединения"""
        return self._connection and self._connection.closed == 0

    def connect(self):
        """Функция для установки соединения"""
        self.close()
        self._connection = psycopg2.connect(**self._conn, cursor_factory=DictCursor)

    def close(self):
        """Функция для закрытия соединения

        Exceptions:
            Exception: Текст ошибки
        """

        if self.connected():
            try:
                self._connection.close()
            except Exception:
                log.info('datetime: {0}   Ошибка при закрытии соединения'.format(datetime.now()))
        self._connection = None

    @backoff(logger=log, try_count=20, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def get_data(self, query, **kwargs) -> Iterator:
        """Функция получения данных из БД.

        Args:
            query: Текст запроса
            kwargs: Параметры запроса

        Returns:
            (Iterator): Результат из БД

        Exceptions:
            Exception: Текст ошибки
        """
        curs = self._connection.cursor()
        curs.execute(query, kwargs)

        while data := curs.fetchmany(self.cursor_array_size):
            yield from data
        curs.close()
