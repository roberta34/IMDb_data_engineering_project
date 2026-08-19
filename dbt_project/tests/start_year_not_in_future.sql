
select *
from {{ ref('dim_title') }}
where start_year > extract(year from current_date)