select *
from {{ ref('mart_top_directors') }}
where rank < 1
    or rank > 10