with joined as (
  select
    s.reporting_date,
    s.customer_id,
    c.gender,
    c.region,
    c.plan_type,
    c.device_model,
    c.duration_months,
    c.avg_income_per_month,
    c.income_band,
    c.dob,
    s.account_status,
    s.balance,
    s.principal_outstanding,
    s.interest_outstanding,
    s.arrears_amount,
    s.dpd,
    s.payment_amount,
    s.default_flag,
    {{ age_at('c.dob','s.reporting_date') }} as age_years,
    {{ age_band('age_years') }} as age_band
  from {{ ref('stg_credit_snapshots') }} s
  left join {{ ref('stg_customers') }} c using (customer_id)
)
select * from joined