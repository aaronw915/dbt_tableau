with events as (
    select *
    from {{ ref('stg_historical_events_ent_prd') }}
),

sites as (
    select *
    from {{ ref('stg_sites_hist_ent_prd') }}
),

users as (
    select *
    from {{ ref('stg_users_hist_ent_prd') }}
),

groups as (
    select *
    from {{ ref('stg_groups_hist_ent_prd') }}
),

workbooks as (
    select *
    from {{ ref('stg_workbooks_hist_ent_prd') }}
),

views as (
    select *
    from {{ ref('stg_views_ent_hist_prd') }}
),

projects as (
    select *
    from {{ ref('stg_projects_hist_ent_prd') }}
),

datasources as (
    select *
    from {{ ref('stg_datasources_hist_ent_prd') }}
),

flows as (
    select *
    from {{ ref('stg_flows_hist_ent_prd') }}
)

select distinct
    e.hist_event_id,
    e.created_date_timestamp,
    e.created_date,
    e.hist_event_action_type,
    e.hist_event_name,

    -- actor site
    e.hist_actor_site_id,
    s_actor.site_name as actor_site_name,

    -- target site
    e.hist_target_site_id,
    s_target.site_name as target_site_name,

    -- users
    e.hist_actor_user_id,
    u1.site_role_id as actor_site_role_id,
    u1.system_user_id as actor_system_user_id,
    u1.display_name as actor_display_name,

    e.hist_target_user_id,
    u2.system_user_id as target_system_user_id,
    u2.display_name as target_display_name,

    -- objects
    e.hist_workbook_id,
    w.workbook_name,

    e.hist_view_id,
    v.view_name,

    e.hist_group_id,
    g.group_name,

    e.hist_project_id,
    p.project_name,

    e.hist_datasource_id,
    d.datasource_name,

    e.hist_flow_id,
    f.flow_name

from events e

left join sites s_actor 
    on s_actor.hist_site_id = e.hist_actor_site_id

left join sites s_target 
    on s_target.hist_site_id = e.hist_target_site_id

left join users u1 
    on e.hist_actor_user_id = u1.hist_user_id

left join users u2 
    on e.hist_target_user_id = u2.hist_user_id

left join groups g 
    on e.hist_group_id = g.hist_group_id

left join workbooks w 
    on e.hist_workbook_id = w.hist_workbook_id

left join views v 
    on e.hist_view_id = v.hist_view_id

left join projects p 
    on e.hist_project_id = p.hist_project_id

left join datasources d 
    on e.hist_datasource_id = d.hist_datasource_id

left join flows f 
    on e.hist_flow_id = f.hist_flow_id