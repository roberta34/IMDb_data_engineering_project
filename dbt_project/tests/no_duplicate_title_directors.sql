select
    title_id,
    director_id,
    count(*) as cnt
from {{ ref('int_title_directors') }}
group by title_id, director_id
having count(*) > 1