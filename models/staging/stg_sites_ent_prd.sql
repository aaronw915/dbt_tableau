WITH source1 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'sites') }}
),
source2 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'hist_sites') }}
),
renamed1 AS (
    SELECT
        id AS site_id,
        name AS site_name,
        url_namespace AS site_url_namespace,
        pipeline_start_date
    FROM source1
), renamed2 AS (
    SELECT
        id AS hist_site_id,
        site_id
    FROM source2
), joined_sources AS (
    SELECT
        r1.site_id,
        r1.site_name,
        r1.site_url_namespace,
        r1.pipeline_start_date,
        r2.hist_site_id
    FROM renamed1 r1
    LEFT JOIN renamed2 r2
        ON r1.site_id = r2.site_id
)

SELECT * FROM joined_sources
