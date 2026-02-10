WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'projects') }}
),

renamed AS (
    SELECT
        id AS project_id,
        name AS project_name,
        owner_id AS project_owner_id,
        try_to_timestamp(created_at) AS project_created_at,
        try_to_timestamp(updated_at) AS project_updated_at,
        site_id AS project_site_id,
        luid AS project_luid,
        parent_project_id,
        pipeline_start_date
    FROM source
)

SELECT * FROM renamed