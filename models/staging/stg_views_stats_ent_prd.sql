WITH source AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'views_stats') }}
),

renamed AS (
    SELECT
        id AS view_stats_id,
        user_id,
        view_id,
        nviews AS total_views,
        time AS user_id_last_viewed_at,
        site_id,
        device_type
    FROM source
)

SELECT * FROM renamed