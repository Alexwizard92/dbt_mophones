with credit as (
  select * from {{ ref('credit_customer_snapshot') }}
),
-- Link each NPS response to the closest reporting_date snapshot for that customer
nps_credit as (
  select
    n.customer_id,
    n.response_date,
    n.nps_score,
    n.nps_group,
    c.reporting_date,
    c.account_status,
    c.dpd,
    c.arrears_amount,
    c.balance,
    c.age_band,
    c.income_band,
    c.region,
    c.plan_type,
    abs(datediff(day, n.response_date, c.reporting_date)) as date_diff
  from {{ ref('stg_nps') }} n
  join credit c on n.customer_id = c.customer_id
),
ranked as (
  select *,
    row_number() over (partition by customer_id, response_date order by date_diff) as rn
  from nps_credit
),
linked as (
  select
    customer_id,
    response_date,
    nps_score,
    nps_group,
    reporting_date,
    account_status,
    dpd,
    arrears_amount,
    balance,
    age_band,
    income_band,
    region,
    plan_type
  from ranked
  where rn = 1
)
select * from linked