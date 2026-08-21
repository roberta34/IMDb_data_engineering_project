select
    title_id,
    genre_key,
    count(*) as row_count
from {{ ref('bridge_title_genre') }}
group by
    title_id,
    genre_key
having count(*) > 1