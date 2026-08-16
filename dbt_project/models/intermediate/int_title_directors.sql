with source as (
    select
        title_id,
        directors
    from {{ ref('stg_title_crew') }}
),

exploded as (
    select
        title_id,
        unnest(string_split(directors, ',')) as director_id
    from source
),

cleaned as (
    select
        title_id,
        trim(director_id) as director_id
    from exploded
    where director_id is not null
        and trim(director_id) <> ''
        and trim(director_id) <> '\N'
)

select distinct
    title_id,
    director_id
from cleaned