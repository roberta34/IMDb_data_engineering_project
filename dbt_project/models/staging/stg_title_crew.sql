with source as (
    select *
    from {{ source('imdb_raw', 'title_crew') }}
),

renamed as (
    select
        cast(tconst as varchar) as title_id,
        cast(directors as varchar) as directors,
        cast(writers as varchar) as writers

    from source
)

select *
from renamed