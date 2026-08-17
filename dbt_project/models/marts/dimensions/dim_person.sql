with name_basics as (
    select *
    from {{ ref('stg_name_basics') }}
),

final as (
    select
        person_id,
        primary_name,
        birth_year,
        death_year,
        primary_professions

    from name_basics
)

select *
from final

