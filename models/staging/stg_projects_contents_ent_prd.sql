WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'projects_contents') }}
),

renamed AS (
    SELECT
        id AS projects_contents_id,
        project_id,
        site_id,
        content_id,
        content_type,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed