with director_stats as (

    select
        btc.person_id,
        dp.primary_name as director_name,
        count(distinct btc.title_id) as title_count,
        sum(f.num_votes) as combined_votes,
        avg(f.average_rating) as average_rating,
        sum(f.average_rating * f.num_votes) / sum(f.num_votes) as weighted_average_rating

    from {{ ref('bridge_title_crew') }} btc

    join {{ ref('dim_person') }} dp
        on btc.person_id = dp.person_id

    join {{ ref('fct_title_ratings') }} f
        on btc.title_id = f.title_id

    where btc.role = 'director'

    group by
        btc.person_id,
        dp.primary_name

    having
        count(distinct btc.title_id) >= 5
        and sum(f.num_votes) >= 1000
),

ranked as (

    select
        *,
        row_number() over (
            order by
                average_rating desc,
                combined_votes desc,
                person_id
        ) as rank

    from director_stats
)

select
    person_id,
    director_name,
    title_count,
    combined_votes,
    average_rating,
    weighted_average_rating,
    rank

from ranked

where rank <= 10

order by rank