WITH source1 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'projects') }}
),

source2 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'hist_projects') }}
),

renamed1 AS (
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
    FROM source1
),

renamed2 AS (
    SELECT
        id AS hist_project_id,
        project_id
    FROM source2
),

joined AS (
    SELECT
        renamed1.*,
        renamed2.hist_project_id
    FROM renamed1
    LEFT JOIN renamed2
        ON renamed1.project_id = renamed2.project_id
)

SELECT * FROM joined