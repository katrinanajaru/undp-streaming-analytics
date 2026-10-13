{{ config(materialized='table', engine='MergeTree', order_by='project_id') }}

select
    pwr.project_id           as project_id,
    pwr.project_name         as project_name,
    pwr.sector                as sector,
    pwr.status                as status,
    pwr.region_id             as region_id,
    pwr.region_name           as region_name,
    pwr.country                as country,
    pis.total_funding_usd     as total_funding_usd,
    pis.total_beneficiaries   as total_beneficiaries,
    case
        when pis.total_funding_usd > 0
        then pis.total_beneficiaries / (pis.total_funding_usd / 1000000)
        else 0
    end as beneficiaries_per_million_usd
from {{ ref('projects_with_region') }} pwr
left join {{ ref('project_impact_summary') }} pis on pwr.project_id = pis.project_id
