with source as (
    select
        person_id,
        known_for_titles
    from {{ ref('stg_name_basics')}}
),

exploded as (
    select
        person_id,
        unnest(string_split(known_for_titles, ',')) as title_id
    from source
),

cleaned as (
    select
        person_id,
        trim(title_id) as title_id
    from exploded
    where title_id is not null
        and trim(title_id) != ''
        and trim(title_id) != '\N'
)

select distinct
    person_id,
    title_id
from cleaned