from functools import lru_cache
from http import HTTPStatus
from typing import Optional

import orjson
from aioredis import Redis
from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends, HTTPException
from models.film import ESFilm, ListResponseFilm
from models.person import DetailResponsePerson, ElasticPerson
from services.mixins import ServiceMixin
from services.pagination import get_by_pagination
from services.utils import create_hash_key, get_hits


class PersonService(ServiceMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.person_films: int = 0

    async def get_person_films_count(self) -> int:
        return self.person_films

    async def set_person_films_count(self, value: int):
        self.person_films = value

    async def get_person(self, person_id: str):
        person = await self.get_by_id(target_id=person_id, schema=ElasticPerson)
        if not person:
            """Если персона не найдена, отдаём 404 статус"""
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND, detail="person not found"
            )
        return person

    async def get_person_films(
        self, person_id: str, page: int, page_size: int
    ) -> Optional[dict]:
        """Получаем число фильмов персоны из стейт"""
        state_total: int = await self.get_person_films_count()
        body: dict = {
            "size": page_size,
            "from": (page - 1) * page_size,
            "query": {
                "nested": {
                    "path": "writers",
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {"writers.id": person_id}},
                                {"match": {"actors.id": person_id}},
                                {"match": {"directors.id": person_id}}
                              ]
                        }
                    }
                }
            }
        }
        state_key: str = "person_films"
        params: str = f"{state_total}{page}{page_size}{body}"
        """ Пытаемся получить фильмы персоны из кэша """
        instance = await self._get_result_from_cache(
            key=create_hash_key(index=self.index, params=params)
        )
        if not instance:
            docs: Optional[dict] = await self.search_in_elastic(
                body=body, _index="movies"
            )
            """ Получаем фильмы персоны из ES """
            hits = get_hits(docs=docs, schema=ESFilm)
            """ Получаем число фильмов персоны """
            total: int = int(docs.get("hits").get("total").get("value", 0))
            """ Прогоняем данные через pydantic """
            person_films: list[ListResponseFilm] = [
                ListResponseFilm(
                    uuid=film.id, title=film.title, imdb_rating=film.imdb_rating
                )
                for film in hits
            ]
            data = orjson.dumps([i.dict() for i in person_films])
            new_param: str = f"{total}{page}{body}{page_size}"
            await self._put_data_to_cache(
                key=create_hash_key(index=state_key, params=new_param), instance=data
            )
            """ Сохраняем число персон в стейт """
            await self.set_person_films_count(value=total)
            return get_by_pagination(
                name="films",
                db_objects=person_films,
                total=total,
                page=page,
                page_size=page_size,
            )
        person_films: list[ListResponseFilm] = [
            ListResponseFilm(**row) for row in orjson.loads(instance)
        ]
        return get_by_pagination(
            name="films",
            db_objects=person_films,
            total=state_total,
            page=page,
            page_size=page_size,
        )

    async def get_person_detail(self, person_id):
        instance = await self._get_result_from_cache(
            key=person_id
        )

        if not instance:
            body: dict = {
                "query": {
                    "nested": {
                        "path": "writers",
                        "query": {
                            "bool": {
                                "should": [
                                    {"match": {"writers.id": person_id}},
                                    {"match": {"actors.id": person_id}},
                                    {"match": {"directors.id": person_id}}
                                ]
                            }
                        }
                    }
                }
            }
            person = await self.get_person(person_id=person_id)
            docs: Optional[dict] = await self.search_in_elastic(body=body, _index="movies")

            if not docs:
                return None
            hits = get_hits(docs=docs, schema=ESFilm)

            film_ids = []
            role = []
            for el in hits:
                if el.id not in film_ids:
                    film_ids.append(el.id)
                for actor_dict in el.actors:
                    if str(person.id) == actor_dict['id'] and 'actor' not in role:
                        role.append('actor')
                for writer_dict in el.writers:
                    if str(person.id) == writer_dict['id'] and 'writer' not in role:
                        role.append('writer')
                for director_dict in el.directors:
                    if str(person.id) == director_dict['id'] and 'director' not in role:
                        role.append('director')

            instance = ElasticPerson(
                id=person.id,
                full_name=person.full_name,
                roles=role,
                film_ids=film_ids
            )

            await self._put_data_to_cache(key=instance.id, instance=instance.json())
            return instance

        return ElasticPerson.parse_raw(instance)

    async def search_person(
        self, query: str, page: int, page_size: int
    ) -> Optional[dict]:
        person_body = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"full_name": query}}
                                ]
                            }
                        }
                    }
        body: dict = {
            "size": page_size,
            "from": (page - 1) * page_size,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"writers_names": query}},
                        {"match": {"actors_names": query}},
                        {"match": {"directors_names": query}}
                      ]
                }
            }
        }
        """ Получаем число персон из стейт """
        state_total: int = await self.get_total_count()
        params: str = f"{state_total}{page}{page_size}{body}"
        """ Пытаемся получить данные из кэша """
        instance = await self._get_result_from_cache(
            key=create_hash_key(index=self.index, params=params)
        )

        if not instance:
            persons_docs: Optional[dict] = await self.search_in_elastic(body=person_body, _index="persons")
            docs: Optional[dict] = await self.search_in_elastic(body=body, _index="movies")
            if not docs:
                return None
            person_hits = get_hits(docs=persons_docs, schema=ElasticPerson)
            persons: list[DetailResponsePerson] = [
                DetailResponsePerson(
                    uuid=es_person.id,
                    full_name=es_person.full_name,
                    role='Main Role',
                    film_ids=[]
                )
                for es_person in person_hits
            ]
            """ Получаем персон из ES """
            hits = get_hits(docs=docs, schema=ESFilm)
            for person in persons:
                for es_person in hits:
                    for actor_dict in es_person.actors:
                        if str(person.uuid) == actor_dict['id'] and es_person.id not in person.film_ids:
                            person.film_ids.append(es_person.id)
                            person.role = 'Actor'
                    for writer_dict in es_person.writers:
                        if str(person.uuid) == writer_dict['id'] and es_person.id not in person.film_ids:
                            person.film_ids.append(es_person.id)
                            person.role = 'Writer'
                    for director_dict in es_person.directors:
                        if str(person.uuid) == director_dict['id'] and es_person.id not in person.film_ids:
                            person.film_ids.append(es_person.id)
                            person.role = 'Director'

            """ Получаем число персон """
            total: int = int(persons_docs.get("hits").get("total").get("value", 0))

            """ Сохраняем персон в кеш """
            data = orjson.dumps([i.dict() for i in persons])
            new_param: str = f"{total}{page}{body}{page_size}"
            await self._put_data_to_cache(
                key=create_hash_key(index=self.index, params=new_param), instance=data
            )
            """ Сохраняем число персон в стейт """
            await self.set_total_count(value=total)
            return get_by_pagination(
                name="persons",
                db_objects=persons,
                total=total,
                page=page,
                page_size=page_size,
            )
        persons: list[DetailResponsePerson] = [
            DetailResponsePerson(**row) for row in orjson.loads(instance)
        ]
        return get_by_pagination(
            name="persons",
            db_objects=persons,
            total=state_total,
            page=page,
            page_size=page_size,
        )


# get_person_service — это провайдер PersonService. Синглтон
@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis=redis, elastic=elastic, index="persons")
