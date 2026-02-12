WITH base AS (

    SELECT
        sites.site_id,
        sites.site_name,
        sr.display_name AS site_role_name,
        su.system_user_id,
        su.system_user_name,
        su.display_name AS system_user_display_name,
        su.system_user_created_at,
        u.site_user_id,
        u.login_at,
        u.site_user_created_at

    FROM {{ ref('stg_system_users_ent_prd') }} su
    JOIN {{ ref('stg_users_ent_prd') }} u
        ON su.system_user_id = u.system_user_id
    JOIN {{ ref('stg_sites_ent_prd') }} sites
        ON sites.site_id = u.site_id
    JOIN {{ ref('stg_site_roles') }} sr
        ON sr.role_id = u.site_role_id
    WHERE su.system_user_id NOT IN (1)

),

metrics AS (

    SELECT
        *,

        CASE
            WHEN login_at IS null THEN null
            ELSE datediff(DAY, login_at, current_timestamp)
        END AS days_since_login,

        datediff(DAY, site_user_created_at, current_timestamp)
            AS days_since_created

    FROM base

)

SELECT
    *,

    CASE
        WHEN days_since_login IS null THEN 'Never'
        WHEN days_since_login <= 7 THEN '0-7'
        WHEN days_since_login <= 30 THEN '8-30'
        WHEN days_since_login <= 90 THEN '31-90'
        WHEN days_since_login <= 180 THEN '91-180'
        WHEN days_since_login <= 270 THEN '181-270'
        WHEN days_since_login <= 365 THEN '271-365'
        ELSE 'Over 365 days'
    END AS login_recency_bucket,

    CASE
        WHEN days_since_login IS null THEN -1
        WHEN days_since_login <= 7 THEN 1
        WHEN days_since_login <= 30 THEN 2
        WHEN days_since_login <= 90 THEN 3
        WHEN days_since_login <= 180 THEN 4
        WHEN days_since_login <= 270 THEN 5
        WHEN days_since_login <= 365 THEN 6
        ELSE 7
    END AS login_recency_bucket_id

FROM metrics
