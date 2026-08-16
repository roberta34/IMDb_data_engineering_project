with source as (
    select *
    from {{ source('imdb_raw', 'title_akas') }}
),

renamed as (

    select
        cast(titleId as varchar) as title_id,
        cast(ordering as integer) as ordering,
        cast(title as varchar) as alternative_title,
        cast(region as varchar) as region,
        cast(language as varchar) as language,
        cast(types as varchar) as types,
        cast(attributes as varchar) as attributes,
        cast(isOriginalTitle as integer) as is_original_title

    from source
)

select *
from renamed