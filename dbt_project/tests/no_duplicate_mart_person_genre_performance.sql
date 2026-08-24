select
    person_id,
    role,
    genre_name,
    count(*) as row_count
from {{ ref('mart_person_genre_performance') }}
group by
    person_id,
    role,
    genre_name
having count(*) > 1