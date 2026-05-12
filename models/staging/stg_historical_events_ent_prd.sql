WITH source_events AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_events') }}
),

source_event_types AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_event_types') }}
),

events AS (
    SELECT
        id AS hist_event_id,
        historical_event_type_id AS hist_event_type_id,
        created_at AS hist_event_created_at,

        hist_actor_user_id,
        hist_target_user_id,
        hist_actor_site_id,
        hist_target_site_id,

        hist_project_id,
        hist_workbook_id,
        hist_view_id,
        hist_datasource_id,
        hist_data_connection_id,
        hist_comment_id,
        hist_tag_id,
        hist_group_id,
        hist_flow_id,

        pipeline_start_date
    FROM source_events
),

event_types AS (
    SELECT
        type_id AS hist_event_type_id,
        name AS hist_event_name,
        action_type AS hist_event_action_type
    FROM source_event_types
)

SELECT
    e.hist_event_id,
    e.hist_event_type_id,
    et.hist_event_name,
    et.hist_event_action_type,

    e.hist_event_created_at AS created_date_timestamp,
    to_date(e.hist_event_created_at) AS created_date,

    e.hist_actor_user_id,
    e.hist_target_user_id,
    e.hist_actor_site_id,
    e.hist_target_site_id,

    e.hist_project_id,
    e.hist_workbook_id,
    e.hist_view_id,
    e.hist_datasource_id,
    e.hist_data_connection_id,
    e.hist_comment_id,
    e.hist_tag_id,
    e.hist_group_id,
    e.hist_flow_id,

    e.pipeline_start_date

FROM events e
LEFT JOIN event_types et
    ON e.hist_event_type_id = et.hist_event_type_id
--where datediff('day',to_date(e.hist_event_created_at),current_date())<=365 -- filter to only include events from the last 365 days