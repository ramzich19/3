"""Модуль с настройками"""
import os

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class PostgresSettings(BaseModel):
    """Класс с настройками подключения для PG"""
    dbname: str
    user: str
    password: str
    host: str
    port: int


class ElasticSettings(BaseModel):
    """Класс с настройками подключения для ES"""
    es_host: str


class BaseSettings(BaseModel):
    """Класс с базовыми настройками приложения"""
    cursor_array_size: int
    limit_count: int


pg_settings = PostgresSettings(
    dbname=os.environ.get('DB_NAME'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
    host=os.environ.get('DB_HOST'),
    port=os.environ.get('DB_PORT')
)

es_settings = ElasticSettings(es_host=os.environ.get('ES_HOST'))

base_settings = BaseSettings(
    cursor_array_size=os.environ.get('CURSOR_ARRAY_SIZE'),
    limit_count=os.environ.get('LIMIT_COUNT')
)
