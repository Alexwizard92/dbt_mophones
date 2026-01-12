
# -*- coding: utf-8 -*-
"""
MoPhones Credit Analytics - Additional Analysis

This script provides supplementary analysis on the dbt-transformed data.
Run this after executing the dbt models to generate insights from the outputs.

Assumptions:
- dbt has been run and outputs are available in ../outputs/
- Python environment has pandas, matplotlib, seaborn installed
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Define paths
OUTPUT_DIR = Path(__file__).parent.parent / "outputs"

def load_data():
    """Load the dbt output CSV files."""
    files = {
        'nps_by_status': OUTPUT_DIR / 'nps_by_status.csv',
        'nps_linkage_detail': OUTPUT_DIR / 'nps_linkage_detail.csv',
        'portfolio_kpis': OUTPUT_DIR / 'portfolio_kpis.csv',
        'roll_rates': OUTPUT_DIR / 'roll_rates.csv',
        'segment_metrics': OUTPUT_DIR / 'segment_metrics.csv',
        'status_transitions': OUTPUT_DIR / 'status_transitions.csv',
        'vintage_metrics': OUTPUT_DIR / 'vintage_metrics.csv'
    }

    data = {}
    for name, path in files.items():
        if path.exists():
            data[name] = pd.read_csv(path)
            print(f"Loaded {name}: {len(data[name])} rows")
        else:
            print(f"Warning: {path} not found")

    return data

def analyze_nps_linkage(data):
    """Analyze NPS scores by credit status."""
    if 'nps_linkage_detail' not in data:
        return

    df = data['nps_linkage_detail']

    # NPS distribution by account status
    plt.figure(figsize=(12, 6))

    plt.subplot(1, 2, 1)
    sns.boxplot(data=df, x='account_status', y='nps_score')
    plt.title('NPS Score Distribution by Account Status')
    plt.xticks(rotation=45)

    plt.subplot(1, 2, 2)
    nps_by_status = df.groupby('account_status')['nps_score'].agg(['mean', 'median', 'count'])
    nps_by_status['mean'].plot(kind='bar', rot=45)
    plt.title('Average NPS by Account Status')
    plt.ylabel('Average NPS Score')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'nps_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

    print("\nNPS Analysis Summary:")
    print(nps_by_status)

def analyze_portfolio_metrics(data):
    """Analyze portfolio-level KPIs over time."""
    if 'portfolio_kpis' not in data:
        return

    df = data['portfolio_kpis']

    # Ensure reporting_date is datetime
    df['reporting_date'] = pd.to_datetime(df['reporting_date'])

    # Plot key metrics over time
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Portfolio Metrics Over Time', fontsize=16)

    metrics = ['total_accounts', 'current_accounts', 'arrears_accounts', 'default_accounts']

    for i, metric in enumerate(metrics):
        ax = axes[i//2, i%2]
        if metric in df.columns:
            df.plot(x='reporting_date', y=metric, ax=ax, marker='o')
            ax.set_title(f'{metric.replace("_", " ").title()}')
            ax.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'portfolio_trends.png', dpi=300, bbox_inches='tight')
    plt.show()

def analyze_segment_performance(data):
    """Analyze performance by customer segments."""
    if 'segment_metrics' not in data:
        return

    df = data['segment_metrics']

    # Age band analysis
    plt.figure(figsize=(15, 6))

    plt.subplot(1, 3, 1)
    age_default = df.groupby('age_band')['default_rate'].mean()
    age_default.plot(kind='bar', rot=45)
    plt.title('Default Rate by Age Band')
    plt.ylabel('Default Rate (%)')

    plt.subplot(1, 3, 2)
    income_default = df.groupby('income_band')['default_rate'].mean()
    income_default.plot(kind='bar', rot=45)
    plt.title('Default Rate by Income Band')
    plt.ylabel('Default Rate (%)')

    plt.subplot(1, 3, 3)
    region_default = df.groupby('region')['default_rate'].mean()
    region_default.plot(kind='bar', rot=45)
    plt.title('Default Rate by Region')
    plt.ylabel('Default Rate (%)')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'segment_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def analyze_roll_rates(data):
    """Analyze roll rates between statuses."""
    if 'roll_rates' not in data:
        return

    df = data['roll_rates']

    # Create a pivot table for roll rates
    pivot = df.pivot_table(
        values='roll_rate',
        index='from_status',
        columns='to_status',
        aggfunc='mean'
    )

    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot, annot=True, fmt='.1%', cmap='YlOrRd')
    plt.title('Roll Rates Between Account Statuses')
    plt.savefig(OUTPUT_DIR / 'roll_rates_heatmap.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """Main analysis function."""
    print("MoPhones Credit Analytics - Additional Analysis")
    print("=" * 50)

    # Load data
    data = load_data()

    if not data:
        print("No output files found. Please run dbt models first.")
        return

    # Run analyses
    analyze_nps_linkage(data)
    analyze_portfolio_metrics(data)
    analyze_segment_performance(data)
    analyze_roll_rates(data)

    print("\nAnalysis complete! Check the outputs directory for generated charts.")

if __name__ == "__main__":
    main()
