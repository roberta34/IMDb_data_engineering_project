with genre_titles as (

    select
        g.genre_name,
        f.title_id,
        f.average_rating,
        f.num_votes

    from {{ ref('fct_title_ratings') }} f

    join {{ ref('bridge_title_genre') }} bg
        on f.title_id = bg.title_id

    join {{ ref('dim_genre') }} g
        on bg.genre_key = g.genre_key
),

genre_quality as (
    select
        genre_name,
        count(distinct title_id) as title_count,
        count(distinct case
            when average_rating >= 8.0
                and num_votes >= 100
                and num_votes <= 10000
            then title_id
        end) as hidden_gem_count,

        count(distinct case
            when average_rating <= 6.0
                and num_votes >=10000
            then title_id
        end) as overrated_count

    from genre_titles
    group by genre_name
    having count(distinct title_id) >= 100
)

select
    genre_name,
    title_count,
    hidden_gem_count,
    overrated_count,
    hidden_gem_count::double / title_count as hidden_gem_ratio,
    overrated_count::double / title_count as overrated_ratio
from genre_quality
order by hidden_gem_ratio desc, overrated_ratio desc