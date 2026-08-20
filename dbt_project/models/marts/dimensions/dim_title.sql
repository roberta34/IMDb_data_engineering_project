with title_basics as (
    select *
    from {{ ref('stg_title_basics') }}
),

final as (

    select
        title_id,
        primary_title,
        original_title,
        title_type,
        is_adult,
        case
            when start_year <= extract(year from current_date)
                then start_year
            else null
        end as start_year,
        end_year,
        runtime_minutes,
        genres

    from title_basics

)

select *
from final
