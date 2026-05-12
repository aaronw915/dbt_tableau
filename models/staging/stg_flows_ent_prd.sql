WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'flows') }}
),

renamed AS (
    SELECT
        id AS flow_id,
        name AS flow_name,
        luid AS flow_luid,
        project_id AS project_id,
        site_id,
        owner_id AS flow_owner_id,
        try_to_timestamp(created_at) AS created_at,
        try_to_timestamp(updated_at) AS updated_at,
        size AS flow_size,
        try_to_timestamp(last_published_at) AS last_published_at
    FROM source
)

SELECT *
FROM renamed