with base as (
  select
    cast(response_date as date) as response_date,
    customer_id,
    cast(nps_score as integer) as nps_score,
    nps_comment
  from {{ ref('raw_nps') }}
)
select
  *,
  case
    when nps_score between 0 and 6 then 'Detractor'
    when nps_score in (7,8) then 'Passive'
    when nps_score in (9,10) then 'Promoter'
    else 'Unknown'
  end as nps_group
from base