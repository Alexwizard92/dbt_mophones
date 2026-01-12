
-- Roll-rate matrix: percent of loans in prev_status that roll to each curr_status between snapshots

with trans as (
  select * from {{ ref('status_transitions') }}
),
prev_totals as (
  select prev_reporting_date, curr_reporting_date, prev_status, sum(transitions) as prev_total
  from trans
  group by 1,2,3
)
select
  t.prev_reporting_date,
  t.curr_reporting_date,
  t.prev_status,
  t.curr_status,
  t.transitions,
  1.0 * t.transitions / nullif(p.prev_total,0) as roll_rate
from trans t
join prev_totals p using (prev_reporting_date, curr_reporting_date, prev_status)
order by t.prev_reporting_date, t.curr_reporting_date, t.prev_status, t.curr_status
