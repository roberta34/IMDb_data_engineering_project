with source as (
    select *
    from {{ source('imdb_raw', 'title_episode') }}
),

renamed as (

    select
        cast(tconst as varchar) as title_id,
        cast(parentTconst as varchar) as parent_title_id,
        cast(seasonNumber as integer) as season_number,
        cast(episodeNumber as integer) as episode_number

    from source
)

select *
from renamed