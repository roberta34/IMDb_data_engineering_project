with source as (
    select
        title_id,
        writers
    from {{ ref('stg_title_crew') }}
),

exploded as (
    select
        title_id,
        unnest(string_split(writers, ',')) as writer_id
    from source
),

cleaned as (

    select
        title_id,
        trim(writer_id) as writer_id
    from exploded
    where writer_id is not null
        and trim(writer_id) != ''
        and trim(writer_id) != '\N'
)

select distinct
    title_id,
    writer_id
from cleaned