WITH source AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'sites') }}
),

renamed AS (
    SELECT
        id AS site_id,
        name AS site_name,
        url_namespace AS site_url_namespace,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed