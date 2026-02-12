WITH source1 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'datasources') }}
)
,
source2 AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'hist_datasources') }}
),
renamed1 AS (
    SELECT
        id AS datasource_id,
        name AS datasource_name,
        try_to_timestamp(created_at) AS datasource_created_at,
        try_to_timestamp(updated_at) AS datasource_updated_at,
        owner_id AS datasource_owner_id,
        size AS datasource_size,
        project_id AS datasource_project_id,
        refreshable_extracts,
        try_to_timestamp(extracts_refreshed_at) AS datasource_extracts_refreshed_at,
        connectable,
        parent_workbook_id,
        site_id AS datasource_site_id,
        revision AS datasource_revision,
        try_to_timestamp(first_published_at) AS datasource_first_published_at,
        try_to_timestamp(last_published_at) AS datasource_last_published_at,
        modified_by_user_id,
        is_certified,
        certification_note,
        certifier_user_id,
        certifier_details,
        db_class,
        luid AS datasource_luid,
        tds_luid,
        pipeline_start_date
    FROM source1
),
renamed2 AS (
    SELECT
        id AS hist_datasource_id,
        datasource_id
    FROM source2
),
joined AS (
    SELECT
        r1.datasource_id,
        r1.datasource_name,
        r1.datasource_created_at,   
        r1.datasource_updated_at,
        r1.datasource_owner_id,
        r1.datasource_size,
        r1.datasource_project_id,
        r1.refreshable_extracts,
        r1.datasource_extracts_refreshed_at,
        r1.connectable,
        r1.parent_workbook_id,
        r1.datasource_site_id,
        r1.datasource_revision,
        r1.datasource_first_published_at,
        r1.datasource_last_published_at,
        r1.modified_by_user_id,
        r1.is_certified,
        r1.certification_note,
        r1.certifier_user_id,
        r1.certifier_details,
        r1.db_class,
        r1.datasource_luid,
        r1.tds_luid,
        r1.pipeline_start_date, 
        r2.hist_datasource_id
    FROM renamed1 AS r1
    LEFT JOIN renamed2 AS r2
    ON r1.datasource_id = r2.datasource_id
)
select * from joined