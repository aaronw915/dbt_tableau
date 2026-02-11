WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'groups') }}
),

renamed AS (
    SELECT
        id AS group_id,
        site_id,
        name AS group_name,
        try_to_timestamp(created_at) AS created_at,
        try_to_timestamp(updated_at) AS updated_at,
        luid AS group_luid
    FROM source
)

SELECT * FROM renamed