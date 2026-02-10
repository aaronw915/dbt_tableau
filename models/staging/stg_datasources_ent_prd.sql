WITH source AS (
    SELECT * FROM {{ source('tableau_ent_prd', 'datasources') }}
),

renamed AS (
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
    FROM source
)

SELECT * FROM renamed
