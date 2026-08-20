select *
from {{ ref('fct_title_ratings') }}
where num_votes < 0