with source as (
    select
        title_id,
        genres
    from {{ ref('stg_title_basics') }}
),

exploded as (
    select title_id,
        unnest(string_split(genres, ',')) as genre_name
    from source
),

cleaned as(

    select
        title_id,
        trim(genre_name) as genre_name
    from exploded
    where genre_name is not null
        and trim(genre_name) <> ''
        and trim(genre_name) <> '\N'
)

select distinct
    title_id,
    genre_name
from cleaned
