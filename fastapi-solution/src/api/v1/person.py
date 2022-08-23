from http import HTTPStatus
from typing import Optional

from api.v1.utils import PersonSearchParam
from fastapi import APIRouter, Depends, HTTPException
from models.film import FilmPagination
from models.person import ElasticPerson, PersonPagination
from services.person import PersonService, get_person_service

router = APIRouter()


@router.get(
    path="/search",
    response_model=PersonPagination,
    summary="Поиск персоны по его имени",
    description="Поиск персоны по его имени",
    response_description="Список персон с их именем, ролью и фильмографией",
    tags=["person_service"],
)
async def person_search(
    params: PersonSearchParam = Depends(),
    person_service: PersonService = Depends(get_person_service),
    page: int = 1,
    page_size: int = 10,
) -> PersonPagination:
    persons: Optional[dict] = await person_service.search_person(
        query=params.query, page=page, page_size=page_size
    )
    if not persons:
        """Если персоны не найдены, отдаём 404 статус"""
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="persons not found"
        )
    return PersonPagination(**persons)


@router.get(
    path="/{person_id}",
    response_model=ElasticPerson,
    summary="Поиск персоны по ID",
    description="Поиск персоны по ID",
    response_description="Имя, роль и фильмография персоны",
    tags=["person_service"],
)
async def person_details(
    person_id: str, person_service: PersonService = Depends(get_person_service)
) -> ElasticPerson:
    person = await person_service.get_person_detail(person_id=person_id)
    if not person:
        """Если персоны не найдены, отдаём 404 статус"""
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="persons not found"
        )
    return person


@router.get(
    path="/{person_id}/film/",
    response_model=FilmPagination,
    summary="Поиск персоны по его ID и выдача всех его кинопроизведений",
    description="Поиск персоны по его ID и выдача всех его кинопроизведений,"
    "в которых он принимал участие",
    response_description="Название жанра",
    tags=["person_service"],
)
async def all_person_films(
    person_id: str,
    person_service: PersonService = Depends(get_person_service),
    page: int = 1,
    page_size: int = 10,
) -> FilmPagination:
    person_films = await person_service.get_person_films(
        person_id=person_id, page=page, page_size=page_size
    )
    if not person_films:
        """Если персона не найдена, отдаём 404 статус"""
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="person's films not found"
        )
    return FilmPagination(**person_films)
