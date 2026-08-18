{% snapshot dim_title_snapshot %}

{{
    config(
        target_schema='main',
        unique_key='title_id',
        strategy='check',
        check_cols=['primary_title', 'genres', 'runtime_minutes']
    )
}}

select
    title_id,
    primary_title,
    genres,
    runtime_minutes
from {{ ref('dim_title') }}

{% endsnapshot %}