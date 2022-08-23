"""Модуль с описанием необходимых методов для работы с БД"""
import abc


class DatabaseAdapter:
    """Класс для описания методов по работе с БД"""

    @abc.abstractmethod
    def connected(self) -> bool:
        pass

    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass
