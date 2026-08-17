with title_years as (
    select
        start_year as year
    from {{ ref('stg_title_basics') }}

    where start_year is not null

    union

    select
        end_year as year
    from {{ ref('stg_title_basics') }}

    where end_year is not null
),

distinct_years as (
    select distinct
        year
    from title_years
),

final as (
    select
        year as year_key,
        year,
        floor(year / 10) * 10 as decade
    from distinct_years
)

select *
from final