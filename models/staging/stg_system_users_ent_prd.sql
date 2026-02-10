WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'system_users') }}
),

renamed AS (
    SELECT
        id AS system_user_id,
        name AS system_user_name,
        email AS system_user_email,
        friendly_name AS display_name,
        try_to_timestamp(created_at) AS system_user_created_at,
        try_to_timestamp(updated_at) AS system_user_updated_at,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed