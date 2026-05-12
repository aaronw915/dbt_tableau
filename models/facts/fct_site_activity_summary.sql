WITH event_agg AS (
    SELECT
        created_date AS event_date,
        hist_actor_site_id AS site_id,
        count(*) AS event_count
    FROM stg_historical_events_ent_prd
    WHERE
        hist_actor_site_id IS NOT null
        AND created_date >= current_date - 365
    GROUP BY
        created_date,
        hist_actor_site_id
),

sites AS (
    SELECT
        hist_site_id AS hist_site_id,
        site_id,
        site_name
    FROM {{ ref('stg_sites_hist_ent_prd') }}
),

calendar AS (
    SELECT date
    FROM {{ source('calendar','calendar') }}
    WHERE date >= current_date - 365
)

SELECT
    cal.date,
    s.site_id,
    s.site_name,
    coalesce(e.event_count, 0) AS event_count
FROM calendar cal
CROSS JOIN sites s
LEFT JOIN event_agg e
    ON
        e.event_date = cal.date
        AND e.site_id = s.site_id
ORDER BY
    cal.date DESC,
    s.site_id