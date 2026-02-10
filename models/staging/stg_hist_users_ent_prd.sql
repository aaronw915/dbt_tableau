with source as (
  select *
  from {{ source('tableau_ent_prd', 'hist_users') }}
),
renamed as (
    select id as hist_user_id, user_id as site_user_id, system_user_id, pipeline_start_date
    from source
)
select * from renamed