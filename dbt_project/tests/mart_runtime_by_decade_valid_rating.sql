select *
from {{ ref('mart_runtime_by_decade') }}
where average_rating < 0
    or average_rating > 10