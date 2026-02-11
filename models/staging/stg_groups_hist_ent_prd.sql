WITH source1 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'groups') }}
),

source2 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'hist_groups') }}
),

renamed_source1 AS (
    SELECT
        id AS group_id,
        site_id,
        name AS group_name,
        try_to_timestamp(created_at) AS created_at,
        try_to_timestamp(updated_at) AS updated_at,
        luid AS group_luid
    FROM source1
),

renamed_source2 AS (
    SELECT
        id hist_group_id,
        group_id
    FROM source2
),

joined_sources AS (
    SELECT
        rs1.group_id,
        rs1.site_id,
        rs1.group_name,
        rs1.created_at,
        rs1.updated_at,
        rs1.group_luid,
        rs2.hist_group_id
    FROM renamed_source1 rs1
    LEFT JOIN renamed_source2 rs2
        ON rs1.group_id = rs2.group_id
)

SELECT * FROM joined_sources