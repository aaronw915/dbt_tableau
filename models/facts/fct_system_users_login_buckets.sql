WITH last_login AS (

    SELECT
        system_user_id,
        to_date(login_at) AS last_login,

        datediff(
            'day',
            to_date(login_at),
            current_date()
        ) AS days_since_login,

        row_number() OVER (
            PARTITION BY system_user_id
            ORDER BY login_at DESC NULLS LAST
        ) AS rn

    FROM {{ ref('stg_users_ent_prd') }}

),

user_login_buckets AS (

    SELECT
        system_user_id,

        CASE
            WHEN days_since_login IS null THEN 'Never'
            WHEN days_since_login <= 7 THEN '0-7 Days'
            WHEN days_since_login <= 30 THEN '8-30 Days'
            WHEN days_since_login <= 90 THEN '31-90 Days'
            WHEN days_since_login <= 180 THEN '91-180 Days'
            WHEN days_since_login <= 270 THEN '181-270 Days'
            WHEN days_since_login <= 365 THEN '271-365 Days'
            ELSE 'Over 365 Days'
        END AS login_bucket,

        CASE
            WHEN days_since_login <= 7 THEN 1
            WHEN days_since_login <= 30 THEN 2
            WHEN days_since_login <= 90 THEN 3
            WHEN days_since_login <= 180 THEN 4
            WHEN days_since_login <= 270 THEN 5
            WHEN days_since_login <= 365 THEN 6
            WHEN days_since_login IS null THEN 8
            ELSE 7
        END AS login_bucket_id

    FROM last_login

    WHERE rn = 1

),

bucket_counts AS (

    SELECT
        login_bucket,
        login_bucket_id,
        count(*) AS user_count

    FROM user_login_buckets

    GROUP BY 1, 2

)

SELECT
    login_bucket,
    login_bucket_id,
    user_count,

    round(
        user_count / sum(user_count) OVER (),
        2
    ) AS pct_of_total_users

FROM bucket_counts

ORDER BY login_bucket_id