with movies as(

    select
        f.title_id,
        y.decade,
        f.runtime_minutes,
        f.average_rating

    from {{ ref('fct_title_ratings') }} f

    inner join {{ ref('dim_title') }} t
        on f.title_id = t.title_id

    inner join {{ ref('dim_year') }} y
        on f.year_key = y.year_key

    where t.title_type = 'movie'
        and f.runtime_minutes is not null
        and f.runtime_minutes > 0
        and f.average_rating is not null
        and y.decade is not null
),

aggregated as (

    select
        decade,
        count(distinct title_id) as movie_count,
        avg(runtime_minutes) as average_runtime,
        avg(average_rating) as average_rating,
        corr(runtime_minutes, average_rating) as runtime_rating_correlation

    from movies

    group by decade
)

select *
from aggregated
order by decade
