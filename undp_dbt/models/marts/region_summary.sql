select
    region_id,
    region_name,
    country,
    sum(total_funding_usd)      as total_funding_usd,
    sum(total_beneficiaries)    as total_beneficiaries,
    count(distinct project_id)  as total_projects
from {{ ref('master_project_summary') }}
group by region_id, region_name, country
