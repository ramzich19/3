"""Модуль с декоратором для backoff механики"""
from functools import wraps
from time import sleep
from typing import Callable
from datetime import datetime, time

from database.database import DatabaseAdapter


class MyException(Exception):
    ...


def backoff(logger=None, try_count=20, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param logger: объект логера
    :param try_count: количество попыток подключения к БД
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func: Callable):
        @wraps(func)
        def inner(storage: DatabaseAdapter, *args, **kwargs):
            cnt = 0
            while cnt < try_count:
                try:
                    if not storage.connected():
                        storage.connect()
                    return func(storage, *args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.info("Отсутствует соединение с базой. Функция: %s \n %s", func.__name__, e)
                    sleep_time = start_sleep_time * factor**cnt
                    if sleep_time < border_sleep_time:
                        sleep(sleep_time)
                    else:
                        sleep(border_sleep_time)
                    cnt += 1
            raise MyException("Превышен лимит подключения к БД")  # ToDo Сделать кастомный Exception + описать в комментах

        return inner

    return func_wrapper
