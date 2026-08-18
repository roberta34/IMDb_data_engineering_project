with ratings as (
    select
        title_id,
        average_rating,
        num_votes
    from {{ ref('stg_title_ratings') }}
),

titles as (
    select
        title_id,
        start_year,
        runtime_minutes
    from {{ ref('stg_title_basics') }}
),

final as (
    select
        r.title_id,
        t.start_year as year_key,
        r.average_rating,
        r.num_votes,
        t.runtime_minutes
    from ratings r
    left join titles t
        on r.title_id = t.title_id
)

select *
    from final