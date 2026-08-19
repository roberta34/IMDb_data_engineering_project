
select *
from {{ ref('fct_title_ratings') }}
where runtime_minutes is not null
  and runtime_minutes <= 0