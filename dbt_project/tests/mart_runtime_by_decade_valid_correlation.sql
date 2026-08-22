select *
from {{ ref('mart_runtime_by_decade') }}
where runtime_rating_correlation < -1
    or runtime_rating_correlation > 1