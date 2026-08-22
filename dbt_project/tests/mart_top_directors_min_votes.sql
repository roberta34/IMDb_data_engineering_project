select *
from {{ ref('mart_top_directors') }}
where combined_votes < 1000