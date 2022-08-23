query_template_film_works_id = \
    '''select un.id
         from(
       select fw.id, fw.modified
         from content.film_work fw
        inner join content.person_film_work pfw
           on pfw.film_work_id = fw.id
        inner join content.person as p
           on p.id = pfw.person_id
        where p.modified > %(modified)s or (p.modified = %(modified)s <**>)
       union
       select fw.id, fw.modified
         from content.film_work fw
        inner join content.genre_film_work jfw
           on jfw.film_work_id = fw.id
        inner join content.genre as j
           on j.id = jfw.genre_id
        where j.modified > %(modified)s or (j.modified = %(modified)s <**>)
       union
       select fw.id, fw.modified
         from content.film_work fw
        where fw.modified > %(modified)s or (fw.modified = %(modified)s <**>)) as un
        order by un.modified, un.id
        limit {0};'''


query_film_works_elastic = \
    '''select
           fw.id as fw_id
         , fw.title
         , fw.creation_date
         , fw.description
         , fw.rating
         , fw.type
         , fw.created
         , fw.modified
         , pfw.role
         , p.id
         , p.full_name
         , g.name
         , g.id as g_id
         from content.film_work fw
         left join content.person_film_work pfw
           on pfw.film_work_id = fw.id
         left join content.person p
           on p.id = pfw.person_id
         left join content.genre_film_work gfw
           on gfw.film_work_id = fw.id
         left join content.genre g
           on g.id = gfw.genre_id
        where fw.id in %(ids)s
        order by fw.modified, fw.id;'''

query_persons = '''
       select distinct id as person_id, full_name, modified
         from content.person
        where modified > %(modified)s
'''
query_genres = '''
       select distinct id as genre_id, name, description, modified
         from content.genre
        where modified > %(modified)s
'''