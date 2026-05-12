WITH workbooks AS (
    SELECT *
    FROM
        {{ ref('stg_workbooks_ent_prd') }}
),

views AS (
    SELECT *
    FROM
        {{ ref('stg_views_ent_prd') }}
),

views_stats AS (
    SELECT *
    FROM
        {{ ref('stg_views_stats_ent_prd') }}
),

viewer_info AS (
    SELECT *
    FROM
        {{ ref('int_site_users_details_ent_prd') }}
),

joined AS (
    SELECT
        wb.workbook_site_id,
        wb.workbook_id,
        wb.workbook_name,
        vs.view_stats_id,
        v.view_id,
        v.view_name,
        v.view_index,
        vs.user_id,
        vi.system_user_name,
        vi.system_user_display_name,
        vs.user_id_last_viewed_at,
        vs.total_views,
        coalesce(vs.device_type, 'blank') AS device_type
    FROM
        workbooks wb
    JOIN views v ON wb.workbook_id = v.workbook_id
    JOIN views_stats vs ON v.view_id = vs.view_id
    LEFT JOIN viewer_info vi ON vs.user_id = vi.site_user_id
    ORDER BY vs.user_id_last_viewed_at DESC
)

SELECT *
FROM
    joined