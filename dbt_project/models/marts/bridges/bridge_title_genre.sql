{{ config(
    materialized='table'
) }}
with title_genres as (
    select
        title_id,
        genre_name

    from {{ ref('stg_title_genres') }}
),

genres as (
    select
        genre_key,
        genre_name
    from {{ ref('dim_genre') }}
),

final as (
    select
        tg.title_id,
        g.genre_key
    from title_genres tg

    join genres g
        on tg.genre_name = g.genre_name
)

select distinct
    title_id,
    genre_key
from final