select
    title_id,
    writer_id,
    count(*) as cnt
from {{ ref('int_title_writers') }}
group by title_id, writer_id
having count(*) > 1