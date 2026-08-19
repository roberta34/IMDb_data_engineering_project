select *
from {{ ref('fct_title_ratings') }}
where average_rating < 0
    or average_rating > 10