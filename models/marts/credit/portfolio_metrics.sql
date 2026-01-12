with base as (
  select * from {{ ref('credit_customer_snapshot') }}
),
status_counts as (
  select reporting_date,
         count(*) as accounts,
         sum(case when account_status = 'current' then 1 else 0 end) as current_accounts,
         sum(case when account_status = 'arrears' then 1 else 0 end) as arrears_accounts,
         sum(case when account_status = 'default' then 1 else 0 end) as default_accounts,
         sum(case when account_status = 'closed'  then 1 else 0 end) as closed_accounts,
         sum(arrears_amount) as total_arrears,
         sum(balance) as total_balance
  from base
  group by reporting_date
),
ratios as (
  select reporting_date,
         accounts,
         current_accounts,
         arrears_accounts,
         default_accounts,
         closed_accounts,
         total_arrears,
         total_balance,
         1.0 * current_accounts / nullif(accounts,0)      as current_rate,
         1.0 * arrears_accounts / nullif(accounts,0)      as arrears_rate,
         1.0 * default_accounts / nullif(accounts,0)      as default_rate,
         1.0 * closed_accounts  / nullif(accounts,0)      as closed_rate,
         1.0 * total_arrears / nullif(total_balance,0)    as par_ratio  -- Portfolio at Risk proxy
  from status_counts
)
select * from ratios order by reporting_date