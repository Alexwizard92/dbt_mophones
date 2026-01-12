select
  cast(reporting_date as date) as reporting_date,
  customer_id,
  lower(account_status) as account_status,
  cast(balance as numeric) as balance,
  cast(principal_outstanding as numeric) as principal_outstanding,
  cast(interest_outstanding as numeric) as interest_outstanding,
  cast(arrears_amount as numeric) as arrears_amount,
  cast(dpd as integer) as dpd,
  cast(payment_amount as numeric) as payment_amount,
  cast(default_flag as boolean) as default_flag
from {{ ref('raw_credit_snapshots') }}