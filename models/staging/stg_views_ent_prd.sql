WITH source AS (

    SELECT *
    FROM {{ source('tableau_ent_prd', 'views') }}

),

renamed AS (

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
    FROM source

)

SELECT *
FROM renamed
