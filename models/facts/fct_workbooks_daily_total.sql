WITH calendar AS (

    SELECT date
    FROM {{ source('calendar', 'calendar') }}
    WHERE date >= DATE '2016-02-02'

),

workbooks_created AS (

    SELECT
        to_date(workbook_created_at) AS created_date,
        count(DISTINCT workbook_id) AS workbooks_created
    FROM {{ ref('stg_workbooks_ent_prd') }}
    GROUP BY 1

),

/*
    First publish event per workbook
    Prevents republish inflation
*/

workbook_first_publish AS (

    SELECT
        workbook_id,
        created_date

    FROM (

        SELECT
            hw.workbook_id,

            ihe.created_date,

            row_number() OVER (
                PARTITION BY hw.workbook_id
                ORDER BY ihe.created_date_timestamp ASC
            ) AS rn

        FROM {{ ref('stg_workbooks_hist_ent_prd') }} hw

        INNER JOIN {{ ref('int_historical_events_ent_prd') }} ihe
            ON hw.hist_workbook_id = ihe.hist_workbook_id

        WHERE
            ihe.hist_event_action_type = 'Publish'
            AND ihe.hist_event_name = 'Publish Workbook'

    ) x

    WHERE rn = 1

),

audit_create AS (

    SELECT
        created_date,
        count(DISTINCT workbook_id) AS audit_create_count
    FROM workbook_first_publish
    GROUP BY 1

),

audit_delete AS (

    SELECT
        created_date,
        count(DISTINCT hist_workbook_id) AS audit_delete_count
    FROM {{ ref('int_historical_events_ent_prd') }}
    WHERE
        hist_event_action_type = 'Delete'
        AND hist_event_name = 'Delete Workbook'
    GROUP BY 1

),

parameters AS (

    SELECT

        current_workbook_count
        - total_creates
        + total_deletes AS opening_balance,

        DATE '2024-07-31' AS audit_start_date

    FROM (

        SELECT

            (
                SELECT count(DISTINCT workbook_id)
                FROM {{ ref('stg_workbooks_ent_prd') }}
            ) AS current_workbook_count,

            (
                SELECT count(DISTINCT workbook_id)
                FROM workbook_first_publish
            ) AS total_creates,

            (
                SELECT count(DISTINCT hist_workbook_id)
                FROM {{ ref('int_historical_events_ent_prd') }}
                WHERE
                    hist_event_action_type = 'Delete'
                    AND hist_event_name = 'Delete Workbook'
            ) AS total_deletes

    ) x

),

daily_activity AS (

    SELECT
        c.date,

        coalesce(wc.workbooks_created, 0) AS workbooks_created,

        coalesce(ac.audit_create_count, 0) AS audit_create_count,

        coalesce(ad.audit_delete_count, 0) AS audit_delete_count,

        coalesce(ac.audit_create_count, 0)
        -
        coalesce(ad.audit_delete_count, 0)
            AS net_daily_change

    FROM calendar c

    LEFT JOIN workbooks_created wc
        ON c.date = wc.created_date

    LEFT JOIN audit_create ac
        ON c.date = ac.created_date

    LEFT JOIN audit_delete ad
        ON c.date = ad.created_date

),

final AS (

    SELECT
        d.date,

        d.workbooks_created,

        d.audit_create_count,

        d.audit_delete_count,

        d.net_daily_change,

        CASE
            WHEN d.date < p.audit_start_date THEN null

            ELSE
                p.opening_balance
                +
                sum(d.net_daily_change)
                    OVER (
                        ORDER BY d.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
        END AS running_workbook_total

    FROM daily_activity d

    CROSS JOIN parameters p

)

SELECT
    date,

    workbooks_created,

    audit_create_count,

    audit_delete_count,

    net_daily_change,

    running_workbook_total,

    running_workbook_total
    -
    lag(running_workbook_total, 30)
        OVER (ORDER BY date)
        AS running_30_day_change,

    round(
        (
            running_workbook_total
            -
            lag(running_workbook_total, 30)
                OVER (ORDER BY date)
        )
        /
        nullif(
            lag(running_workbook_total, 30)
                OVER (ORDER BY date),
            0
        ),
        4
    ) AS running_30_day_pct_change,

    running_workbook_total
    -
    lag(running_workbook_total, 365)
        OVER (ORDER BY date)
        AS running_365_day_change,

    round(
        (
            running_workbook_total
            -
            lag(running_workbook_total, 365)
                OVER (ORDER BY date)
        )
        /
        nullif(
            lag(running_workbook_total, 365)
                OVER (ORDER BY date),
            0
        ),
        4
    ) AS running_365_day_pct_change

FROM final

ORDER BY date