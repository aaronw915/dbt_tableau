WITH source1 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_events') }}
),

source2 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_event_types') }}
),

renamed1 AS (
    SELECT
        id AS hist_event_id,
        historical_event_type_id,
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
    FROM source1
),

rename2 AS (
    SELECT
        type_id AS hist_event_type_id,
        name AS hist_event_name,
        action_type AS hist_event_action_type
    FROM source2
),

joined_sources AS (
    SELECT
        r1.hist_event_id,
        r1.historical_event_type_id,
        r2.hist_event_name,
        r2.hist_event_action_type,
        r1.hist_event_created_at,
        r1.hist_actor_user_id,
        r1.hist_target_user_id,
        r1.hist_actor_site_id,
        r1.hist_target_site_id,
        r1.hist_project_id,
        r1.hist_workbook_id,
        r1.hist_view_id,
        r1.hist_datasource_id,
        r1.hist_data_connection_id,
        r1.hist_comment_id,
        r1.hist_tag_id,
        r1.hist_group_id,
        r1.hist_flow_id,
        r1.pipeline_start_date
    FROM renamed1 r1
    LEFT JOIN rename2 r2
        ON r1.historical_event_type_id = r2.hist_event_type_id
)

SELECT * FROM joined_sources
