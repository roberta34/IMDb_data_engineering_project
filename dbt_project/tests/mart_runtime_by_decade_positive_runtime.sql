select *
from {{ ref('mart_runtime_by_decade') }}
where average_runtime <= 0