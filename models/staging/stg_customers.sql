with base as (
  select
    customer_id,
    first_name,
    last_name,
    cast(dob as date) as dob,
    gender,
    region,
    plan_type,
    device_model,
    cast(start_date as date) as start_date,
    cast(duration_months as integer) as duration_months,
    cast(income_salary as numeric) as income_salary,
    cast(income_business as numeric) as income_business,
    cast(income_other as numeric) as income_other
  from {{ ref('raw_customers') }}
)
select
  *,
  (coalesce(income_salary,0) + coalesce(income_business,0) + coalesce(income_other,0)) as total_income,
  {{ income_avg('income_salary','income_business','income_other','duration_months') }} as avg_income_per_month,
  {{ income_band( income_avg('income_salary','income_business','income_other','duration_months') ) }} as income_band
from base