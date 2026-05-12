WITH site_daily AS (
    SELECT
        TO_DATE(site_created_date) AS date,
        COUNT(*) AS new_sites
    FROM {{ ref('stg_sites_ent_prd') }}
    WHERE site_id <> 1
    GROUP BY 1
),

calendar AS (
    SELECT date
    FROM {{ source('calendar', 'calendar') }}
    WHERE date >= DATEADD(DAY, -730, CURRENT_DATE())
),

/* 👇 this is the fix */
baseline AS (
    SELECT COUNT(*) AS baseline_sites
    FROM {{ ref('stg_sites_ent_prd') }}
    WHERE
        site_id <> 1
        AND TO_DATE(site_created_date) < DATEADD(DAY, -730, CURRENT_DATE())
)

SELECT
    c.date,

    /* running total + baseline */
    b.baseline_sites
    + SUM(COALESCE(s.new_sites, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS total_sites,

    /* daily change */
    COALESCE(s.new_sites, 0) AS site_diff,

    /* 30-day growth */
    SUM(COALESCE(s.new_sites, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS diff_30_days,

    /* 365-day growth */
    SUM(COALESCE(s.new_sites, 0)) OVER (
        ORDER BY c.date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS diff_365_days

FROM calendar c
LEFT JOIN site_daily s
    ON c.date = s.date
CROSS JOIN baseline b
ORDER BY c.date