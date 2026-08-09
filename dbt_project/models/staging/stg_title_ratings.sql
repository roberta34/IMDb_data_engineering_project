with source as (
    select *
    from {{ source('imdb_raw', 'title_ratings')}}
),

renamed as (

    select
        cast(tconst as varchar) as title_id,
        cast(averageRating as double) as average_rating,
        cast(numVotes as bigint) as num_votes

    from source
)

select *
from renamed