WITH source1 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'users') }}
),
source2 AS (
    SELECT *
    FROM {{ source('tableau_ent_prd', 'hist_users') }}
),
renamed1 AS (
    SELECT
        id AS site_user_id,
        try_to_timestamp(login_at) AS login_at,
        site_id,
        system_user_id,
        luid AS user_luid,
        site_role_id,
        try_to_timestamp(created_at) AS site_user_created_at,
        try_to_timestamp(updated_at) AS site_user_updated_at,
        pipeline_start_date
    FROM source1
),
rename2 as  (
    SELECT
        id AS hist_user_id,
        user_id AS site_user_id,
        system_user_id
    FROM source2
),

joined_sources AS (
    SELECT
        r1.site_user_id,
        r1.login_at,
        r1.site_id,
        r1.system_user_id,
        r1.user_luid,
        r1.site_role_id,
        r1.site_user_created_at,
        r1.site_user_updated_at,
        r1.pipeline_start_date,
        r2.hist_user_id
    FROM renamed1 r1
   LEFT JOIN rename2 r2
        ON (
            r2.site_user_id IS NOT NULL
            AND r1.site_user_id = r2.site_user_id
        )
        OR (
            r2.site_user_id IS NULL
            AND r1.system_user_id = r2.system_user_id
        )
)


SELECT * FROM joined_sources