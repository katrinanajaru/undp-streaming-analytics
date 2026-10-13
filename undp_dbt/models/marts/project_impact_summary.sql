{{ config(materialized='table', engine='MergeTree', order_by='project_id') }}

with funding_agg as (
    select
        project_id,
        sum(amount_disbursed_usd) as total_funding_usd
    from {{ ref('stg_funding') }}
    group by project_id
),

beneficiaries_agg as (
    select
        project_id,
        count() as total_beneficiaries
    from {{ ref('stg_beneficiaries') }}
    group by project_id
)

select
    p.project_id                       as project_id,
    coalesce(f.total_funding_usd, 0)   as total_funding_usd,
    coalesce(b.total_beneficiaries, 0) as total_beneficiaries
from {{ ref('stg_projects') }} p
left join funding_agg f on p.project_id = f.project_id
left join beneficiaries_agg b on p.project_id = b.project_id
