{% snapshot ratings_snapshot %}

{{
    config(
        target_schema='main',
        unique_key='title_id',
        strategy='check',
        check_cols=['average_rating', 'num_votes']
    )
}}

select
    title_id,
    average_rating,
    num_votes
from {{ ref('stg_title_ratings') }}

{% endsnapshot %}