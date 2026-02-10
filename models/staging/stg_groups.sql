with source1 as (
    select *
    from {{ source('tableau_ent_prd', 'groups') }}
),
source2 as (
    select *
    from {{ source('tableau_ent_prd', 'hist_groups') }}
),
renamed_source1 as (
    select
        id as group_id,
        site_id,
        name as group_name,
        try_to_timestamp(created_at) as created_at,
        try_to_timestamp(updated_at) as updated_at,
        luid as group_luid
    from source1
),
renamed_source2 as (
    select
        id hist_group_id,
        group_id,
    from source2
), joined_sources as (
    select
        rs1.group_id,
        rs1.site_id,
        rs1.group_name,
        rs1.created_at,
        rs1.updated_at,
        rs1.group_luid,
        rs2.hist_group_id
    from renamed_source1 rs1
    left join renamed_source2 rs2
        on rs1.group_id = rs2.group_id
)
select * from joined_sources