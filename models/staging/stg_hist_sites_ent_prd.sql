WITH source AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'hist_sites') }}
),

renamed AS (
    SELECT
        id AS hist_site_id,
        site_id,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed
