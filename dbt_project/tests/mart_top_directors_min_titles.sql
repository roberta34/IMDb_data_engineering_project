select *
from {{ ref('mart_top_directors') }}
where title_count < 5