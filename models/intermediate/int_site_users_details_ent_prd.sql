with base as (

    select
        sites.site_id,
        sites.site_name,
        u.site_role_id,
        sr.display_name as site_role_name,

        su.system_user_id,
        su.system_user_name,
        su.display_name as system_user_display_name,
        su.system_user_created_at,
        u.site_user_id,
        u.login_at,
        u.site_user_created_at

    from {{ ref('stg_system_users_ent_prd') }} su
    join {{ ref('stg_users_ent_prd') }} u
        on su.system_user_id = u.system_user_id
    join {{ ref('stg_sites_ent_prd') }} sites
        on sites.site_id = u.site_id
    join {{ ref('stg_site_roles') }} sr
        on sr.role_id = u.site_role_id

    where su.system_user_id not in (1, 233)

),

metrics as (

    select
        *,

        case
            when login_at is null then null
            else datediff(day, login_at, current_timestamp)
        end as days_since_login,

        datediff(day, site_user_created_at, current_timestamp)
            as days_since_created

    from base

)

select
    *,

    case
        when days_since_login is null then 'Never'
        when days_since_login <= 7 then '0-7'
        when days_since_login <= 30 then '8-30'
        when days_since_login <= 90 then '31-90'
        when days_since_login <= 180 then '91-180'
        when days_since_login <= 270 then '181-270'
        when days_since_login <= 365 then '271-365'
        else 'Over 365 days'
    end as login_recency_bucket,

    case
        when days_since_login is null then -1
        when days_since_login <= 7 then 1
        when days_since_login <= 30 then 2
        when days_since_login <= 90 then 3
        when days_since_login <= 180 then 4
        when days_since_login <= 270 then 5
        when days_since_login <= 365 then 6
        else 7
    end as login_recency_bucket_id

from metrics
