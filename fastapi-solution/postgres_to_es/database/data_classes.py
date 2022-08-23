"""Модуль для объявления классов с данными"""
from dataclasses import dataclass
from datetime import datetime


@dataclass
class FilmWorkElastic:
    """Класс для загрузки данных в Elastic"""
    fw_id: str
    title: str
    creation_date: datetime
    description: str
    rating: float
    type: str
    created: datetime
    modified: datetime
    role: str
    id: str
    full_name: str
    name: str
    g_id: str


@dataclass
class PersonElastic:
    """Класс для загрузки данных по персоналиям в Elastic"""
    person_id: str
    full_name: str
    modified: datetime


@dataclass
class GenreElastic:
    """Класс для загрузки данных по жанрам в Elastic"""
    genre_id: str
    name: str
    description: str
    modified: datetime
