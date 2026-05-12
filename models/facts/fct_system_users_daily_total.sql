WITH calendar AS (

    SELECT date
    FROM {{ source('calendar', 'calendar') }}
    WHERE date >= DATE '2019-10-19'

),

system_users_created AS (

    SELECT
        to_date(system_user_created_at) AS created_date,
        count(DISTINCT system_user_id) AS system_users_created
    FROM {{ ref('stg_system_users_ent_prd') }}
    GROUP BY 1

),

audit_events AS (

    SELECT
        created_date,

        hist_event_action_type,

        count(DISTINCT hist_event_id) AS audit_event_count

    FROM {{ ref('int_historical_events_ent_prd') }}

    WHERE
        hist_event_name IN (
            'Create System User',
            'Delete System User'
        )

    GROUP BY 1, 2

),

audit_daily AS (

    SELECT
        created_date,

        sum(
            CASE
                WHEN hist_event_action_type = 'Create'
                    THEN audit_event_count
                ELSE 0
            END
        ) AS audit_create_count,

        sum(
            CASE
                WHEN hist_event_action_type = 'Delete'
                    THEN audit_event_count
                ELSE 0
            END
        ) AS audit_delete_count

    FROM audit_events

    GROUP BY 1

),

parameters AS (

    SELECT

        current_user_count
        - total_creates
        + total_deletes AS opening_balance,

        DATE '2024-07-31' AS audit_start_date

    FROM (

        SELECT
            (
                SELECT count(DISTINCT system_user_id)
                FROM {{ ref('stg_system_users_ent_prd') }}
            ) AS current_user_count,

            (
                SELECT sum(audit_event_count)
                FROM audit_events
                WHERE hist_event_action_type = 'Create'
            ) AS total_creates,

            (
                SELECT sum(audit_event_count)
                FROM audit_events
                WHERE hist_event_action_type = 'Delete'
            ) AS total_deletes

    ) x

),

daily_activity AS (

    SELECT
        c.date,

        coalesce(su.system_users_created, 0) AS system_users_created,

        coalesce(ad.audit_create_count, 0) AS audit_create_count,

        coalesce(ad.audit_delete_count, 0) AS audit_delete_count,

        coalesce(ad.audit_create_count, 0)
        -
        coalesce(ad.audit_delete_count, 0)
            AS net_daily_change

    FROM calendar c

    LEFT JOIN system_users_created su
        ON c.date = su.created_date

    LEFT JOIN audit_daily ad
        ON c.date = ad.created_date

),

final AS (

    SELECT
        d.date,

        d.system_users_created,

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
        END AS running_user_total

    FROM daily_activity d

    CROSS JOIN parameters p

)

SELECT
    date,

    system_users_created,

    audit_create_count,

    audit_delete_count,

    net_daily_change,

    running_user_total,

    running_user_total
    -
    lag(running_user_total, 30)
        OVER (ORDER BY date)
        AS running_30_day_change,

    (
        running_user_total
        -
        lag(running_user_total, 30)
            OVER (ORDER BY date)
    )
    /
    nullif(
        lag(running_user_total, 30)
            OVER (ORDER BY date),
        0
    ) AS running_30_day_pct_change,

    running_user_total
    -
    lag(running_user_total, 365)
        OVER (ORDER BY date)
        AS running_365_day_change,

    (
        running_user_total
        -
        lag(running_user_total, 365)
            OVER (ORDER BY date)
    )
    /
    nullif(
        lag(running_user_total, 365)
            OVER (ORDER BY date),
        0
    ) AS running_365_day_pct_change

FROM final

ORDER BY date