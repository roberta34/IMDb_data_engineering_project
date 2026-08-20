select
    title_id,
    genre_name,
    count(*) as cnt
from {{ ref('stg_title_genres') }}
group by title_id, genre_name
having count(*)>1