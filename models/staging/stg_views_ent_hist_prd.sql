WITH source1 AS (

    SELECT *
    FROM {{ source('tableau_ent_prd', 'views') }}

),

source2 AS (

    SELECT *
    FROM {{ source('tableau_ent_prd', 'hist_views') }}

),

renamed1 AS (

    SELECT
        id AS view_id,
        name AS view_name,
        try_to_timestamp(created_at) AS created_at,
        published,
        datasource_id,
        workbook_id,
        "index" AS view_index,
        try_to_timestamp(updated_at) AS view_updated_at,
        owner_id,
        title,
        sheet_id,
        sheettype,
        site_id,
        try_to_timestamp(first_published_at) AS first_published_at,
        revision,
        luid AS view_luid
    FROM source1

),

renamed2 AS (

    SELECT
        id AS hist_view_id,
        view_id
    FROM source2

),

joined AS (

    SELECT
        r1.*,
        r2.hist_view_id
    FROM renamed1 r1
    LEFT JOIN renamed2 r2
        ON r1.view_id = r2.view_id

)

SELECT *
FROM joined
