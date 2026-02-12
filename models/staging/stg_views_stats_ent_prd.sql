with source as (
    select * from {{ source('tableau_ent_prd', 'views_stats') }}
),
renamed as (
    select
        id as view_stats_id,
        user_id, 
        view_id,
        nviews as total_views,
        time as user_id_last_viewed_at,
        site_id,
        device_type
    from source
)
select * from renamed