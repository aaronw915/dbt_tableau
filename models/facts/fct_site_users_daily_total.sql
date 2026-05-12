WITH user_daily AS (
    SELECT
        TO_DATE(system_user_created_at) AS date,
        COUNT(system_user_id) AS new_users
    FROM {{ ref('stg_system_users_ent_prd') }}
    WHERE system_user_id NOT IN (1, 2)
    GROUP BY 1
),

calendar AS (
    SELECT date
    FROM {{ source('calendar', 'calendar') }}
    WHERE date >= DATEADD(DAY, -730, CURRENT_DATE())
),

baseline AS (
    SELECT COUNT(*) AS baseline_users
    FROM {{ ref('stg_system_users_ent_prd') }}
    WHERE TO_DATE(system_user_created_at) < DATEADD(DAY, -730, CURRENT_DATE())
)

SELECT
    c.date,

    /* total users */
    b.baseline_users
    + SUM(COALESCE(u.new_users, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS total_users,

    /* daily new users */
    COALESCE(u.new_users, 0) AS user_diff,

    /* 30-day growth */
    SUM(COALESCE(u.new_users, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS diff_30_days,

    /* 365-day growth */
    SUM(COALESCE(u.new_users, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS diff_365_days

FROM calendar c
LEFT JOIN user_daily u
    ON c.date = u.date
CROSS JOIN baseline b
ORDER BY c.date