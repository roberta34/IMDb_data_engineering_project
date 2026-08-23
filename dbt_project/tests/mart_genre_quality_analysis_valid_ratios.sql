select *
from {{ ref('mart_genre_quality_analysis') }}
where hidden_gem_ratio < 0
   or hidden_gem_ratio > 1
   or overrated_ratio < 0
   or overrated_ratio > 1