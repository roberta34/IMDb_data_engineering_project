{{ config(
    materialized='view'
) }}

with person_genre_base as (

    select
        c.person_id,
        p.primary_name,
        c.role,
        g.genre_name,
        f.title_id,
        f.average_rating,
        f.num_votes,
        f.year_key

    from {{ ref('bridge_title_crew') }} c

    inner join {{ ref('dim_person') }} p
        on c.person_id = p.person_id

    inner join {{ ref('fct_title_ratings') }} f
        on c.title_id = f.title_id

    inner join {{ ref('bridge_title_genre') }} bg
        on c.title_id = bg.title_id

    inner join {{ ref('dim_genre') }} g
        on bg.genre_key = g.genre_key

    where c.role in ('actor', 'director')
      and f.year_key between extract(year from current_date) - 19
                         and extract(year from current_date)

),

person_genre_aggregated as (

    select
        person_id,
        primary_name,
        role,
        genre_name,

        count(distinct title_id) as title_count,

        sum(num_votes) as combined_votes,

        avg(average_rating) as average_rating,

        sum(average_rating * num_votes)
            / nullif(sum(num_votes), 0)
            as weighted_average_rating

    from person_genre_base

    group by
        person_id,
        primary_name,
        role,
        genre_name

    having count(distinct title_id) >= 5
       and sum(num_votes) >= 1000

)

select
    person_id,
    primary_name,
    role,
    genre_name,
    title_count,
    combined_votes,
    average_rating,
    weighted_average_rating

from person_genre_aggregated