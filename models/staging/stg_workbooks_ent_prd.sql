WITH source AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'workbooks') }}
),

renamed AS (
    SELECT
        id AS workbook_id,
        name AS workbook_name,
        try_to_timestamp(created_at) AS workbook_created_at,
        try_to_timestamp(updated_at) AS workbook_updated_at,
        owner_id AS workbook_owner_id,
        size AS workbook_size,
        project_id AS workbook_project_id,
        refreshable_extracts,
        try_to_timestamp(extracts_refreshed_at) AS workbook_extracts_refreshed_at,
        site_id AS workbook_site_id,
        revision AS workbook_revision,
        try_to_timestamp(first_published_at) AS workbook_first_published_at,
        try_to_timestamp(last_published_at) AS workbook_last_published_at,
        modified_by_user_id,
        luid AS workbook_luid,
        pipeline_start_date
    FROM source
)

  SELECT * FROM renamed
