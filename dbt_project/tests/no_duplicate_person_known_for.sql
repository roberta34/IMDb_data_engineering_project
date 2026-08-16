select
    person_id,
    title_id,
    count(*) as cnt
from {{ ref('int_person_known_for') }}
group by person_id, title_id
having count(*) > 1