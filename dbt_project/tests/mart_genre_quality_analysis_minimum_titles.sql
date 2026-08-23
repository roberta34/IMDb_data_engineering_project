select *
from {{ ref('mart_genre_quality_analysis') }}
where title_count < 100