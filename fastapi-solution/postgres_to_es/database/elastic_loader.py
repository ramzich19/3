"""Модуль по загрузке данных в Elastic"""
from collections.abc import Iterator
from datetime import datetime
from time import sleep
from typing import Dict, Tuple

from elasticsearch import Elasticsearch, helpers

from storage.storage import State
from database.backoff import backoff
from database.database import DatabaseAdapter
from database.postgres_extractor import NoMoreDataInPG
from log.logger import log


class ElasticLoader(DatabaseAdapter):
    """Класс для загрузки данных в elastic"""
    def __init__(self, host: str) -> None:
        """Конструктор класса.

        Args:
            host: Host:port
        """
        self._host = host
        self._elastic = None

    def connected(self) -> bool:
        """Функция для проверки соединения"""
        return self._elastic and self._elastic.ping()

    def connect(self):
        """Функция для установки соединения"""
        self.close()
        self._elastic = Elasticsearch(self._host)

    def close(self):
        """Функция для закрытия соединения

        Exceptions:
            Exception: Текст ошибки
        """
        if self.connected():
            try:
                self._elastic.transport.close()
            except Exception:
                log.info('datetime: %s   Ошибка при закрытии соединения', datetime.now())

        self._elastic = None

    @backoff(logger=log, try_count=20, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def load_data_into_elastic(self, data: Iterator, index_name: str, state: State) -> None:
        """Функция по загрузке данных в elastic.

        Args:
            data: Кинопроизведения для загрузки в ES
            index_name: Название индекса
            state: Объект класса для сохранения состояния работы программы
        """
        try:
            actions = [
                {
                    '_index': index_name,
                    '_id': item['id'],
                    '_source': item,
                }
                for item in data
            ]
            helpers.bulk(self._elastic, actions)
            sleep(5)

            if hasattr(state, 'modified'):
                state.set_state('modified', state.modified)
                del state.modified
            if hasattr(state, 'fw_id'):
                state.set_state('fw_id', state.fw_id)
                del state.fw_id

        except NoMoreDataInPG as e:
            log.error('Ошибка при ЗАПИСИ В ES \n  %s', e)
            sleep(10)

    @backoff(logger=log, try_count=20, start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def create_indexes(self, indexes_es: Tuple[Dict[str, dict]]) -> None:
        """Функция создания индеков в elastic.

        Args:
            indexes_es: Список индексов для создания
        """
        for index in indexes_es:
            log.info('Проверка наличия индекса:  %s', index['name'])
            if not self._elastic.indices.exists(index=index['name']):
                log.info('Попытка создания индекса:  %s', index['name'])
                self._elastic.indices.create(
                    index=index['name'],
                    settings=index['index']['settings'],
                    mappings=index['index']['mappings']
                )
                log.info('Создан индекс:  %s', index['name'])
            else:
                log.info('Индекс:  %s уже существует', index['name'])

