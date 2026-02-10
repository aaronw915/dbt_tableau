WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'users') }}
),

renamed AS (
    SELECT
        id AS site_user_id,
        try_to_timestamp(login_at) AS login_at,
        site_id,
        system_user_id,
        luid AS user_luid,
        site_role_id,
        try_to_timestamp(created_at) AS site_user_created_at,
        try_to_timestamp(updated_at) AS site_user_updated_at,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed