WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_events') }}
),

renamed AS (
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
    FROM source
)

SELECT * FROM renamed
