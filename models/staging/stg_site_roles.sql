WITH source AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'site_roles') }}
),

renamed AS (
    SELECT
        id AS role_id,
        name AS role_name,
        display_name,
        licensing_rank
    FROM source
)

 SELECT * FROM renamed