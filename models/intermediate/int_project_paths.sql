WITH RECURSIVE project_tree AS (

    --Anchor: top-level projects (no parent)
    SELECT
        project_id,
        project_name,
        project_site_id AS site_id,
        parent_project_id,
        project_name AS path,
        0 AS level
    FROM {{ ref('stg_projects_ent_prd') }}
    WHERE parent_project_id IS null

    UNION ALL

    --Recursive step: attach children
    SELECT
        c.project_id,
        c.project_name,
        c.project_site_id AS site_id,
        c.parent_project_id,
        pt.path || '->' || c.project_name AS path,
        pt.level + 1 AS level
    FROM {{ ref('stg_projects_ent_prd') }} c
    JOIN project_tree pt
        ON c.parent_project_id = pt.project_id
)

SELECT
    project_id,
    project_name,
    site_id,
    parent_project_id,
    path,
    level,
    convert_timezone('UTC', 'America/New_York', current_timestamp()) AS load_datetime
FROM project_tree
