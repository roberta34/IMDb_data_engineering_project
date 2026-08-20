select
    title_id,
    person_id,
    role,
    count(*) as row_count
from {{ ref('bridge_title_crew') }}
group by
    title_id,
    person_id,
    role
having count(*) > 1