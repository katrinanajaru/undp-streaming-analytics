select
    sector,
    sum(total_funding_usd)                        as total_funding_usd,
    sum(total_beneficiaries)                      as total_beneficiaries,
    count(distinct project_id)                    as total_projects,
    avg(beneficiaries_per_million_usd)            as avg_efficiency
from {{ ref('master_project_summary') }}
group by sector
