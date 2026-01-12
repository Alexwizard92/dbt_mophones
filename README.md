# MoPhones Credit Analytics (dbt)

This dbt project models MoPhones' customer, credit snapshot, and NPS data to enable repeatable credit analytics and linkage to customer satisfaction.

## Structure
- **seeds/**: sample raw data structures with example data. Replace with your actual upstream tables or continue using seeds for demo.
- **models/staging/**: source-standardization and light transformations.
- **models/marts/**: business-ready models with metrics and linkage.
- **macros/**: reusable Jinja macros for age and income banding.
- **tests/**: schema tests for data quality.

## Getting started
1. Place your actual CSVs in `seeds/` (or configure sources to your warehouse).
2. Update `profiles.yml` with the `default` profile or change the profile name in `dbt_project.yml`.
3. Run:
   ```bash
   dbt seed
   dbt run
   dbt test
   dbt docs generate
   dbt docs serve
   ```

## Assumptions encoded
- Credit data is delivered as point-in-time snapshots at the start of January and end of each quarter.
- Age is calculated **as of the reporting_date** in the credit snapshot.
- Income average is `(income_salary + income_business + income_other) / duration_months`.
- Income and age are bucketed per the case instructions.

## Outputs
- `marts.credit.portfolio_metrics`: portfolio-level KPIs per reporting_date.
- `marts.credit.segment_metrics`: KPIs by segment (age_band, income_band, region, plan_type, device_model).
- `marts.nps.nps_linkage`: NPS outcomes linked to credit statuses.