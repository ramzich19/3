"""Основной модуль программы"""
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any, Dict, List, Union
from time import sleep

from elasticsearch import ConnectionError, TransportError
from psycopg2 import OperationalError

from config import base_settings, es_settings, pg_settings
from database.data_classes import FilmWorkElastic, PersonElastic, GenreElastic
from database.elastic_loader import ElasticLoader
from log.logger import log
from database.postgres_extractor import PostgresExtractor, NoMoreDataInPG
from storage.storage import JsonFileStorage, State
from queries import queries
from indexes import genre_index, person_index, movie_index


def load_films(elastic: ElasticLoader, postgres: PostgresExtractor):
    """Метод для преобразования данных в формат для Elastic

        Args:
            elastic: Класс для работы с ES
            postgres: Класс для работы с PG
    """
    log.info('datetime: %s   Start loading from PG', datetime.now())
    movies = load_from_postgres(postgres)

    log.info('datetime: %s   Start transform data', datetime.now())
    movies_for_elastic = transform_data(movies)
    log.info('datetime: %s   Start loading to ES', datetime.now())
    elastic.load_data_into_elastic(movies_for_elastic, 'movies', state)


def load_from_postgres(postgres: PostgresExtractor) -> Iterator:
    """Основной метод загрузки данных из Postgres

    Args:
        postgres: Объект класса для загрузки данных из postgres

    Returns:
        (Iterator): Список фильмов для Elastic
    """
    modified = state.get_state('modified') or date(1970, 7, 1)
    fw_id = state.get_state('fw_id')

    if fw_id:
        query_film_works_id = queries.query_template_film_works_id\
            .format(base_settings.limit_count)\
            .replace('<**>', 'and fw.id > %(fw_id)s')
    else:
        query_film_works_id = queries.query_template_film_works_id\
            .format(base_settings.limit_count)\
            .replace('<**>', '')

    try:
        film_works_id = postgres.get_data(query_film_works_id, modified=modified, fw_id=fw_id)
        ids = [row[0] for row in film_works_id]

        if ids:
            film_works_for_elastic = postgres.get_data(queries.query_film_works_elastic, ids=tuple(ids))
            film_works = [FilmWorkElastic(**movie) for movie in film_works_for_elastic]
        else:
            raise NoMoreDataInPG('Отсутствуют новые данные в PG')

        state.modified = film_works[-1].modified.isoformat()
        state.fw_id = ids[-1]

        yield from film_works

    finally:
        log.info('datetime: %s   Close PG connection', datetime.now())


def transform_data(movies_pg: Iterator[FilmWorkElastic]) -> Iterator:
    """Метод для преобразования данных в формат для Elastic

    Args:
        movies_pg: Фильмы из PG

    Returns:
        (List[Dict[str, Any]]): Список фильмов для Elastic
    """

    compare_fw_id = None
    movie_es = None
    movies_es = []

    for movie in movies_pg:
        if compare_fw_id is None or compare_fw_id != movie.fw_id:
            if movie_es:
                movies_es.append(movie_es)

            if movie.role == 'director':
                directors_names = [movie.full_name]
                directors = [{"id": movie.id, "name": movie.full_name}]
            else:
                directors_names = []
                directors = []

            if movie.role == 'actor':
                actors_names = [movie.full_name]
                actors = [{"id": movie.id, "name": movie.full_name}]
            else:
                actors_names = []
                actors = []

            if movie.role == 'writer':
                writers_names = [movie.full_name]
                writers = [{"id": movie.id, "name": movie.full_name}]
            else:
                writers_names = []
                writers = []

            movie_es = {
                "id": movie.fw_id,
                "imdb_rating": movie.rating,
                "genre": [movie.name],
                "genres": [{"id": movie.g_id, "name": movie.name}],
                "creation_date": movie.creation_date,
                "title": movie.title,
                "description": movie.description,
                "directors_names": directors_names,
                "actors_names": actors_names,
                "writers_names": writers_names,
                "directors": directors,
                "actors": actors,
                "writers": writers
            }
            compare_fw_id = movie.fw_id

        elif compare_fw_id == movie.fw_id:
            if movie.name not in movie_es['genre']:
                movie_es['genre'].append(movie.name)
                movie_es['genres'].append({"id": movie.g_id, "name": movie.name})
            match movie.role:
                case 'actor':
                    if movie.full_name not in movie_es['directors_names']:
                        movie_es['directors_names'].append(movie.full_name)
                        movie_es['directors'].append({"id": movie.id, "name": movie.full_name})
                case 'actor':
                    if movie.full_name not in movie_es['actors_names']:
                        movie_es['actors_names'].append(movie.full_name)
                        movie_es['actors'].append({"id": movie.id, "name": movie.full_name})
                case 'writer':
                    if movie.full_name not in movie_es['writers_names']:
                        movie_es['writers_names'].append(movie.full_name)
                        movie_es['writers'].append({"id": movie.id, "name": movie.full_name})

    movies_es.append(movie_es)

    yield from movies_es

    log.info('datetime: %s   Transform ending', datetime.now())


def load_persons(elastic: ElasticLoader, postgres: PostgresExtractor):
    """Метод для преобразования данных в формат для Elastic

        Args:
            elastic: Класс для работы с ES
            postgres: Класс для работы с PG
    """
    state_persons = State(JsonFileStorage('storage/PersonsStorage.json'))
    persons = load_data_from_postgres(postgres, state_persons, queries.query_persons, PersonElastic)
    persons_for_elastic = transform_persons_data(persons)
    elastic.load_data_into_elastic(persons_for_elastic, 'persons', state_persons)


def load_data_from_postgres(
        postgres: PostgresExtractor,
        state_obj: State,
        query: str,
        class_name: GenreElastic or PersonElastic
) -> Iterator:
    """Основной метод загрузки данных из Postgres

    Args:
        postgres: Объект класса для загрузки данных из postgres
        state_obj: Объект класса для работы с состоянием
        query: Текст запроса к БД
        class_name: Датакласса для вывода данных

    Yields:
        (Iterator): Список данных для Elastic
    """
    modified = state_obj.get_state('modified') or date(1970, 7, 1)
    data_for_elastic = postgres.get_data(query, modified=modified)
    objects = [class_name(**obj) for obj in data_for_elastic]
    state_obj.modified = objects[-1].modified.isoformat()

    yield from objects


def transform_persons_data(persons_pg: Iterator[PersonElastic]) -> Iterator:
    """Метод для преобразования данных по персонам в формат для Elastic

    Args:
        persons_pg: Персоны из PG

    Yields:
        (Iterator): Список персон для Elastic
    """
    persons_es = [{"id": person.person_id, "full_name": person.full_name} for person in persons_pg]

    yield from persons_es


def load_genres(elastic: ElasticLoader, postgres: PostgresExtractor):
    """Метод для перекладывания данных о жанрах из PG в Elastic

        Args:
            elastic: Класс для работы с ES
            postgres: Класс для работы с PG
    """
    state_genres = State(JsonFileStorage('storage/GenresStorage.json'))
    genres = load_data_from_postgres(postgres, state_genres, queries.query_genres, GenreElastic)
    genres_for_elastic = transform_genres_data(genres)
    elastic.load_data_into_elastic(genres_for_elastic, 'genres', state_genres)


def transform_genres_data(genres_pg: Iterator[GenreElastic]) -> Iterator:
    """Метод для преобразования данных по жанрам в формат для Elastic

    Args:
        genres_pg: Жанры из PG

    Yields:
        (Iterator): Список жанров для Elastic
    """
    genres_es = [{"id": genre.genre_id, "name": genre.name, "description": genre.description} for genre in genres_pg]

    yield from genres_es


if __name__ == '__main__':
    log.info('Start datetime: %s', datetime.now())

    state = State(JsonFileStorage())
    is_run = state.get_state('is_run')

    if is_run == 'False' or is_run is None:
        state.set_state('is_run', 'True')
    else:
        exit()

    try:
        sleep(7)
        indexes_es = (genre_index.genre, person_index.person, movie_index.movie)
        elastic_index_creator = ElasticLoader(es_settings.es_host)
        elastic_index_creator.create_indexes(indexes_es)
        elastic_index_creator.close()
        while True:
            elastic_loader = ElasticLoader(es_settings.es_host)
            postgres_extractor = PostgresExtractor(pg_settings, base_settings.cursor_array_size)
            
            load_films(elastic_loader, postgres_extractor)
            load_persons(elastic_loader, postgres_extractor)
            load_genres(elastic_loader, postgres_extractor)

            postgres_extractor.close()
            elastic_loader.close()
    except OperationalError as e:
        log.error('datetime: %s   Ошибка при работе с PG', datetime.now())

    except (TransportError, ConnectionError):
        log.error('datetime: %s   Ошибка при работе с ES', datetime.now())

    finally:
        state.set_state('is_run', 'False')
