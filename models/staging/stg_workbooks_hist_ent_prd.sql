WITH source1 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'workbooks') }}
),
source2 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'hist_workbooks') }}
),

renamed1 AS (
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
    FROM source1
)
,
renamed2 AS (
    SELECT
        id AS hist_workbook_id,
        workbook_id
    FROM source2
),
joined AS (
    SELECT
        r1.workbook_id,
        r1.workbook_name,
        r1.workbook_created_at,
        r1.workbook_updated_at,
        r1.workbook_owner_id,
        r1.workbook_size,
        r1.workbook_project_id,
        r1.refreshable_extracts,
        r1.workbook_extracts_refreshed_at,
        r1.workbook_site_id,
        r1.workbook_revision,
        r1.workbook_first_published_at,
        r1.workbook_last_published_at,
        r1.modified_by_user_id,
        r1.workbook_luid,
        r1.pipeline_start_date,
        r2.hist_workbook_id
    FROM renamed1 r1
    LEFT JOIN renamed2 r2
    ON r1.workbook_id = r2.workbook_id
)
select * from joined