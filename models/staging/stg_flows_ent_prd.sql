with source as (
    select *
    from {{ source('tableau_ent_prd', 'flows') }}
),
renamed as (
    select
        id as flow_id,
        name as flow_name,
        luid as flow_luid,
        project_id as project_id,
        site_id ,
        owner_id as flow_owner_id,
        try_to_timestamp(created_at) as created_at,
        try_to_timestamp(updated_at) as updated_at,
        size as flow_size,
        try_to_timestamp(last_published_at) as last_published_at
    from source
)
select *
from renamed