with workbooks as (
    select
        *
    from
        {{ ref('stg_workbooks_ent_prd') }}
),
views as (
    select
        *
    from
        {{ ref('stg_views_ent_prd') }}
),
views_stats as (
    select
        *
    from
        {{ ref('stg_views_stats_ent_prd') }}
),
viewer_info as (
    select
        *
    from
        {{ ref('int_site_users_details_ent_prd') }}
),
joined as (
    select
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
        coalesce(vs.device_type, 'blank') as device_type
    from
        workbooks wb
        join views v on wb.workbook_id = v.workbook_id
        join views_stats vs on v.view_id = vs.view_id
        left join viewer_info vi on vs.user_id = vi.site_user_id
        order by vs.user_id_last_viewed_at desc
)
select
    *
from
    joined