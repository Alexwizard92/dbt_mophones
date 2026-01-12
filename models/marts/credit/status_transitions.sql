
-- Status transitions per loan between consecutive snapshots
-- Produces pairs (prev_status -> curr_status) and counts

with enriched as (
  select
    s.reporting_date,
    s.customer_id as loan_id,
    s.dpd as days_past_due,
    s.balance,
    s.balance as closing_balance,
    case
      when coalesce(s.balance,0) = 0 then 'closed'
      when s.dpd >= 90 then 'default'
      when s.dpd between 1 and 89 then 'arrears'
      else 'current'
    end as account_status
  from {{ ref('stg_credit_snapshots') }} s
),
ordered as (
  select *, row_number() over (partition by loan_id order by reporting_date) as rn
  from enriched
),
paired as (
  select
    curr.loan_id,
    curr.reporting_date as curr_reporting_date,
    prev.reporting_date as prev_reporting_date,
    prev.account_status as prev_status,
    curr.account_status as curr_status
  from ordered curr
  join ordered prev
    on curr.loan_id = prev.loan_id and curr.rn = prev.rn + 1
)
select
  prev_reporting_date,
  curr_reporting_date,
  prev_status,
  curr_status,
  count(*) as transitions
from paired
group by 1,2,3,4
order by prev_reporting_date, curr_reporting_date, prev_status, curr_status
