WITH source1 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'flows') }}
),

source2 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'hist_flows') }}
),

renamed1 AS (
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
    FROM source1
),

renamed2 AS (
    SELECT
        id AS hist_flow_id,
        flow_id
    FROM source2
),

joined AS (
    SELECT
        renamed1.*,
        renamed2.hist_flow_id
    FROM renamed1
    LEFT JOIN renamed2
        ON renamed1.flow_id = renamed2.flow_id
)

SELECT * FROM joined