{{ config(
    materialized='table'
) }}

with crew_directors as (

    select
        title_id,
        trim(person_id) as person_id,
        'director' as role,
        null as job,
        null as characters,
        null as ordering

    from {{ ref('stg_title_crew') }},
    unnest(string_split(directors, ',')) as t(person_id)

    where directors is not null
      and trim(directors) <> ''
      and trim(directors) <> '\N'

),

crew_writers as (

    select
        title_id,
        trim(person_id) as person_id,
        'writer' as role,
        null as job,
        null as characters,
        null as ordering

    from {{ ref('stg_title_crew') }},
    unnest(string_split(writers, ',')) as t(person_id)

    where writers is not null
      and trim(writers) <> ''
      and trim(writers) <> '\N'

),

principal_actors as (

    select
        title_id,
        person_id,
        'actor' as role,
        any_value(job) as job,
        any_value(characters) as characters,
        min(ordering) as ordering

    from {{ ref('stg_title_principals') }}

    where category in ('actor', 'actress')
      and person_id is not null
      and trim(person_id) <> ''
      and trim(person_id) <> '\N'

    group by
        title_id,
        person_id

),

combined as (

    select * from crew_directors

    union all

    select * from crew_writers

    union all

    select * from principal_actors

),

valid_people as (

    select
        person_id
    from {{ ref('dim_person') }}

),

final as (

    select
        c.title_id,
        c.person_id,
        c.role,
        c.job,
        c.characters,
        c.ordering

    from combined c

    inner join valid_people p
        on c.person_id = p.person_id

)

select *
from final