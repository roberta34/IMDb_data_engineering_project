select
    m.genre_name
from {{ ref('mart_person_genre_performance') }} m
left join {{ ref('dim_genre') }} g
    on m.genre_name = g.genre_name
where g.genre_name is null
