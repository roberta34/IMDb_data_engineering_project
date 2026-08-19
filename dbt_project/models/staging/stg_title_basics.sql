with source as (
    select *
    from {{ source('imdb_raw', 'title_basics') }}
),

renamed as (

    select
        cast(tconst as varchar) as title_id,
        cast(titleType as varchar) as title_type,
        cast(primaryTitle as varchar) as primary_title,
        cast(originalTitle as varchar) as original_title,
        cast(isAdult as integer) as is_adult,
        cast(startYear as integer) as start_year,
        cast(endYear as integer) as end_year,
        case
            when cast(runtimeMinutes as integer) > 0
                then cast(runtimeMinutes as integer)
            else null
        end as runtime_minutes,
        cast(genres as varchar) as genres

    from source
)

select *
from renamed