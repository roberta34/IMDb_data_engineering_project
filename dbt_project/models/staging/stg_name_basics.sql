with source as (
    select *
    from {{ source('imdb_raw', 'name_basics') }}
),

renamed as (

    select
        cast(nconst as varchar) as person_id,
        cast(primaryName as varchar) as primary_name,
        cast(birthYear as integer) as birth_year,
        cast(deathYear as integer) as death_year,
        cast(primaryProfession as varchar) as primary_professions,
        cast(knownForTitles as varchar) as known_for_titles

    from source
)

select *
from renamed