WITH month_end AS (
    SELECT
        site_id,
        date_trunc('month', date) AS month,
        date,
        total_users,

        row_number() OVER (
            PARTITION BY site_id, date_trunc('month', date)
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('fct_site_users_daily_total') }}
),

final AS (
    SELECT
        site_id,
        month,
        date AS month_end_date,
        total_users AS ending_users,

        -- previous month
        lag(total_users) OVER (
            PARTITION BY site_id
            ORDER BY month
        ) AS prev_month_users,

        -- previous year
        lag(total_users, 12) OVER (
            PARTITION BY site_id
            ORDER BY month
        ) AS prev_year_users

    FROM month_end
    WHERE rn = 1
)

SELECT
    site_id,
    month,
    month_end_date,
    ending_users,

    prev_month_users,
    ending_users - prev_month_users AS mom_change,

    CASE
        WHEN prev_month_users = 0 THEN null
        ELSE ending_users * 1.0 / prev_month_users - 1
    END AS mom_growth_rate,

    prev_year_users,
    ending_users - prev_year_users AS yoy_change,

    CASE
        WHEN prev_year_users = 0 THEN null
        ELSE ending_users * 1.0 / prev_year_users - 1
    END AS yoy_growth_rate

FROM final

ORDER BY site_id, month