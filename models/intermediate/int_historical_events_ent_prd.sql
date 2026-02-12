WITH events AS (
    SELECT
        *,
        coalesce(hist_actor_site_id, hist_target_site_id) AS site_id
    FROM
        {{ ref('stg_historical_events_ent_prd') }}
),

sites AS (
    SELECT *
    FROM
        {{ ref('stg_sites_hist_ent_prd') }}
),

users AS (
    SELECT *
    FROM
        {{ ref('stg_users_hist_ent_prd') }}
),

groups AS (
    SELECT *
    FROM
        {{ ref('stg_groups_hist_ent_prd') }}
),

workbooks AS (
    SELECT *
    FROM
        {{ ref('stg_workbooks_hist_ent_prd') }}
),

views AS (
    SELECT *
    FROM
        {{ ref('stg_views_ent_hist_prd') }}
),

projects AS (
    SELECT *
    FROM
        {{ ref('stg_projects_hist_ent_prd') }}
),

datasources AS (
    SELECT *
    FROM
        {{ ref('stg_datasources_hist_ent_prd') }}
),

flows AS (
    SELECT *
    FROM
        {{ ref('stg_flows_hist_ent_prd') }}
)

SELECT
    e.site_id,
    s.site_name,
    e.hist_event_action_type,
    e.hist_event_name,
    e.hist_event_id,
    hist_event_created_at,
    u1.system_user_id AS actor_system_user_id,
    e.hist_actor_user_id,
    u1.display_name AS actor_display_name,
    e.hist_target_user_id,
    u2.system_user_id AS target_system_user_id,
    u2.display_name AS target_display_name,
    --workbooks
    e.hist_workbook_id,
    w.workbook_name,
    --views
    e.hist_view_id,
    v.view_name,
    --groups
    e.hist_group_id,
    g.group_name,
    --projects
    e.hist_project_id,
    p.project_name,
    --datasources
    e.hist_datasource_id,
    d.datasource_name,
    --flows
    e.hist_flow_id,
    f.flow_name
FROM
    events e
LEFT JOIN sites s ON s.hist_site_id = e.site_id
LEFT JOIN users u1 ON e.hist_actor_user_id = u1.hist_user_id
LEFT JOIN users u2 ON e.hist_target_user_id = u2.hist_user_id
LEFT JOIN groups g ON e.hist_group_id = g.hist_group_id
LEFT JOIN workbooks w ON e.hist_workbook_id = w.hist_workbook_id
LEFT JOIN views v ON e.hist_view_id = v.hist_view_id
LEFT JOIN projects p ON e.hist_project_id = p.hist_project_id
LEFT JOIN datasources d ON e.hist_datasource_id = d.hist_datasource_id
LEFT JOIN flows f ON e.hist_flow_id = f.hist_flow_id