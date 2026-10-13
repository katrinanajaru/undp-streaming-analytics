{{ config(materialized='table', engine='MergeTree', order_by='project_id') }}

select
    p.project_id   as project_id,
    p.project_name as project_name,
    p.sector       as sector,
    p.status       as status,
    p.region_id    as region_id,
    r.region_name  as region_name,
    r.country      as country
from {{ ref('stg_projects') }} p
left join {{ ref('stg_regions') }} r on p.region_id = r.region_id
