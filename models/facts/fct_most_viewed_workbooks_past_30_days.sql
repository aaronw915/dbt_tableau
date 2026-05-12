WITH base AS (
    SELECT
        ihe.actor_site_name,
        ihe.workbook_name,
        count(DISTINCT ihe.hist_event_id) AS total_views
    FROM {{ ref('int_historical_events_ent_prd') }} ihe
    JOIN {{ ref('stg_workbooks_hist_ent_prd') }} stg
        ON ihe.hist_workbook_id = stg.hist_workbook_id
    WHERE
        ihe.hist_event_action_type = 'Access'
        --and stg.workbook_id = 15553
        --and ihe.view_name = 'DB Table 3 Details'
        AND datediff('day', created_date, current_date()) <= 30
    GROUP BY 1, 2
),

ranking AS (
    SELECT
        *,
        row_number() OVER (ORDER BY total_views DESC NULLS LAST) AS rn
    FROM base
)

SELECT *
FROM ranking
WHERE rn <= 10