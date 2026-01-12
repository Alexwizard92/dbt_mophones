with base as (
  select * from {{ ref('credit_customer_snapshot') }}
)
select
  reporting_date,
  age_band,
  income_band,
  region,
  plan_type,
  device_model,
  count(*) as accounts,
  sum(case when account_status = 'current' then 1 else 0 end) as current_accounts,
  sum(case when account_status = 'arrears' then 1 else 0 end) as arrears_accounts,
  sum(case when account_status = 'default' then 1 else 0 end) as default_accounts,
  1.0 * sum(case when account_status = 'current' then 1 else 0 end) / nullif(count(*),0) as current_rate,
  1.0 * sum(case when account_status = 'arrears' then 1 else 0 end) / nullif(count(*),0) as arrears_rate,
  1.0 * sum(case when account_status = 'default' then 1 else 0 end) / nullif(count(*),0) as default_rate
from base
group by 1,2,3,4,5,6
order by reporting_date, age_band, income_band