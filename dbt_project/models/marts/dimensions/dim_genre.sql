with title_genres as (

    select *
    from {{ ref('stg_title_genres') }}

),

distinct_genres as (

    select distinct
        genre_name
    from title_genres

),

final as (

    select
        dense_rank() over (
            order by genre_name
        ) as genre_key,
        genre_name

    from distinct_genres

)

select *
from final