WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'historical_event_types') }}
),

renamed AS (
    SELECT
        type_id AS hist_event_type_id,
        name AS hist_event_name,
        action_type AS hist_event_action_type,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed
