
-- Vintage metrics per origination month
-- Assumptions:
-- - Age is calculated from dob
-- - sale_date = reporting_date - (age * 365) days approx
-- - Status derived via DPD thresholds if account_status absent upstream

with base as (
  select
    s.reporting_date,
    s.customer_id,
    s.dpd,
    s.balance,
    s.balance as closing_balance,
    s.balance as balance_due_to_date,
    s.dpd as days_past_due,
    s.customer_id as loan_id,
    s.principal_outstanding,
    s.interest_outstanding,
    s.payment_amount,
    s.payment_amount as total_paid,
    s.arrears_amount as total_due_today,
    datediff(day, c.dob, s.reporting_date) as customer_age,
    -- derive sale_date from customer_age in days
    date_add(s.reporting_date, -cast(datediff(day, c.dob, s.reporting_date) as integer)) as sale_date,
    date_format(date_add(s.reporting_date, -cast(datediff(day, c.dob, s.reporting_date) as integer)), 'yyyy-MM') as orig_month,
    -- derive account_status if not provided
    case
      when coalesce(s.balance,0) = 0 then 'closed'
      when s.dpd >= 90 then 'default'
      when s.dpd between 1 and 89 then 'arrears'
      else 'current'
    end as account_status
  from {{ ref('stg_credit_snapshots') }} s
  join {{ ref('stg_customers') }} c on s.customer_id = c.customer_id
),
summary as (
  select
    reporting_date,
    orig_month,
    count(distinct loan_id) as loans,
    sum(case when account_status = 'current' then 1 else 0 end) as current_accounts,
    sum(case when account_status = 'arrears' then 1 else 0 end) as arrears_accounts,
    sum(case when account_status = 'default' then 1 else 0 end) as default_accounts,
    sum(case when account_status = 'closed'  then 1 else 0 end) as closed_accounts
  from base
  group by 1,2
)
select
  reporting_date,
  orig_month,
  loans,
  current_accounts,
  arrears_accounts,
  default_accounts,
  closed_accounts,
  1.0 * current_accounts / nullif(loans,0)  as current_rate,
  1.0 * arrears_accounts / nullif(loans,0)  as arrears_rate,
  1.0 * default_accounts / nullif(loans,0)  as default_rate,
  1.0 * closed_accounts  / nullif(loans,0)  as closed_rate
from summary
order by orig_month, reporting_date
