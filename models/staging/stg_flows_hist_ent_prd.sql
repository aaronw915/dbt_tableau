with source1 as (
    select *
    from {{ source('tableau_ent_prd', 'flows') }}
),
source2 as (
    select *
    from {{ source('tableau_ent_prd', 'hist_flows') }}
),
renamed1 as (
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
    from source1
),
renamed2 as (
    select
        id as hist_flow_id,
        flow_id
    from source2
),
joined as (
    select
        renamed1.*,
        renamed2.hist_flow_id
    from renamed1
    left join renamed2
    on renamed1.flow_id = renamed2.flow_id
)
select * from joined