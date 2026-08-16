with source as (
    select *
    from {{ source('imdb_raw', 'title_principals') }}
),

renamed as (
    select
        cast(tconst as varchar) as title_id,
        cast(ordering as integer) as ordering,
        cast(nconst as varchar) as person_id,
        cast(category as varchar) as category,
        cast(job as varchar) as job,
        cast(characters as varchar) as characters

       from source
)

select * from renamed