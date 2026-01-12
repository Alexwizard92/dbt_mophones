{% macro age_at(dob, as_of_date) %}
  -- dob and as_of_date expected as date types
  floor(datediff(day, {{ dob }}, {{ as_of_date }}) / 365.25)
{% endmacro %}

{% macro age_band(age_expr) %}
  case
    when {{ age_expr }} between 18 and 25 then '18–25'
    when {{ age_expr }} between 26 and 35 then '26–35'
    when {{ age_expr }} between 36 and 45 then '36–45'
    when {{ age_expr }} between 46 and 55 then '46–55'
    when {{ age_expr }} > 55 then 'Above 55'
    else 'Below 18 / Invalid'
  end
{% endmacro %}

{% macro income_avg(salary, business, other, duration_months) %}
  (coalesce({{ salary }}, 0) + coalesce({{ business }}, 0) + coalesce({{ other }}, 0)) / nullif({{ duration_months }}, 0)
{% endmacro %}

{% macro income_band(income_expr) %}
  case
    when {{ income_expr }} < 5000 then 'Below 5,000'
    when {{ income_expr }} between 5000 and 9999 then '5,000–9,999'
    when {{ income_expr }} between 10000 and 19999 then '10,000–19,999'
    when {{ income_expr }} between 20000 and 29999 then '20,000–29,999'
    when {{ income_expr }} between 30000 and 49999 then '30,000–49,999'
    when {{ income_expr }} between 50000 and 99999 then '50,000–99,999'
    when {{ income_expr }} between 100000 and 149999 then '100,000–149,999'
    when {{ income_expr }} >= 150000 then '150,000 and above'
    else 'Unknown'
  end
{% endmacro %}