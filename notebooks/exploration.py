import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from datetime import datetime
import numpy as np

# --- Configuration ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = 'DataMiningProject'
OUTPUT_DIR = 'reports/visuals'

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# --- MongoDB Connection ---
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

print("Connecting to MongoDB...")
print(f"Collections available: {db.list_collection_names()}")


def load_data():
    """Load data from MongoDB collections."""
    print("\nLoading data from MongoDB...")
    
    # Load raw dirty data
    raw_data = list(db['raw_dirty'].find())
    print(f"Loaded {len(raw_data)} raw records")
    
    # Load cleaned data
    cleaned_data = list(db['cleaned.events'].find())
    print(f"Loaded {len(cleaned_data)} cleaned records")
    
    # Load curated data
    curated_data = list(db['curated.events'].find())
    print(f"Loaded {len(curated_data)} curated records")
    
    # Load features
    features_data = list(db['curated.features'].find())
    print(f"Loaded {len(features_data)} feature records")
    
    return raw_data, cleaned_data, curated_data, features_data


def analyze_data_quality(raw_data, cleaned_data):
    """
    Analyze and visualize data quality improvements.
    """
    print("\n" + "="*60)
    print("DATA QUALITY ANALYSIS")
    print("="*60)
    
    # Count missing values in raw data
    raw_missing = {
        'timestamp': sum(1 for r in raw_data if r.get('timestamp') is None),
        'value': sum(1 for r in raw_data if r.get('value') is None),
        'category': sum(1 for r in raw_data if r.get('category') is None),
        'platform': sum(1 for r in raw_data if r.get('platform') is None),
    }
    
    # Count missing values in cleaned data
    cleaned_missing = {
        'timestamp': sum(1 for r in cleaned_data if r.get('timestamp') is None),
        'value': sum(1 for r in cleaned_data if r.get('value') is None),
        'category': sum(1 for r in cleaned_data if r.get('category') is None),
        'platform': sum(1 for r in cleaned_data if r.get('platform') is None),
    }
    
    # Create summary table
    summary_data = []
    for field in raw_missing.keys():
        summary_data.append({
            'Field': field,
            'Raw Missing': raw_missing[field],
            'Raw Missing %': f"{(raw_missing[field]/len(raw_data)*100):.1f}%",
            'Cleaned Missing': cleaned_missing[field],
            'Cleaned Missing %': f"{(cleaned_missing[field]/len(cleaned_data)*100):.1f}%",
            'Improvement': raw_missing[field] - cleaned_missing[field]
        })
    
    summary_df = pd.DataFrame(summary_data)
    print("\nMissing Values Summary:")
    print(summary_df.to_string(index=False))
    
    # Visualize missing data comparison
    fig, ax = plt.subplots(1, 2, figsize=(14, 6))
    
    fields = list(raw_missing.keys())
    raw_counts = [raw_missing[f] for f in fields]
    cleaned_counts = [cleaned_missing[f] for f in fields]
    
    x = np.arange(len(fields))
    width = 0.35
    
    ax[0].bar(x - width/2, raw_counts, width, label='Raw Data', color='#e74c3c')
    ax[0].bar(x + width/2, cleaned_counts, width, label='Cleaned Data', color='#27ae60')
    ax[0].set_xlabel('Field')
    ax[0].set_ylabel('Number of Missing Values')
    ax[0].set_title('Missing Values: Before vs After Cleaning')
    ax[0].set_xticks(x)
    ax[0].set_xticklabels(fields, rotation=45)
    ax[0].legend()
    ax[0].grid(axis='y', alpha=0.3)
    
    # Percentage comparison
    raw_pct = [(raw_missing[f]/len(raw_data)*100) for f in fields]
    cleaned_pct = [(cleaned_missing[f]/len(cleaned_data)*100) for f in fields]
    
    ax[1].bar(x - width/2, raw_pct, width, label='Raw Data', color='#e74c3c')
    ax[1].bar(x + width/2, cleaned_pct, width, label='Cleaned Data', color='#27ae60')
    ax[1].set_xlabel('Field')
    ax[1].set_ylabel('Missing Values (%)')
    ax[1].set_title('Missing Values Percentage: Before vs After Cleaning')
    ax[1].set_xticks(x)
    ax[1].set_xticklabels(fields, rotation=45)
    ax[1].legend()
    ax[1].grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/01_missing_values_comparison.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved: {OUTPUT_DIR}/01_missing_values_comparison.png")
    plt.close()
    
    return summary_df


def analyze_value_distributions(raw_data, cleaned_data, curated_data):
    """
    Compare value distributions across pipeline stages.
    """
    print("\n" + "="*60)
    print("VALUE DISTRIBUTION ANALYSIS")
    print("="*60)
    
    # Extract numeric values
    raw_values = []
    for r in raw_data:
        val = r.get('value')
        if val is not None:
            try:
                if isinstance(val, (int, float)):
                    raw_values.append(float(val))
                elif isinstance(val, str):
                    # Try to extract number from string
                    import re
                    cleaned = re.sub(r"[^0-9.-]", "", val)
                    if cleaned:
                        raw_values.append(float(cleaned))
            except:
                pass
    
    cleaned_values = [r.get('value') for r in cleaned_data if r.get('value') is not None]
    curated_values = [r.get('value') for r in curated_data if r.get('value') is not None]
    
    print(f"\nRaw values extracted: {len(raw_values)}")
    print(f"Cleaned values: {len(cleaned_values)}")
    print(f"Curated values: {len(curated_values)}")
    
    # Statistics
    stats_data = []
    for name, values in [('Raw', raw_values), ('Cleaned', cleaned_values), ('Curated', curated_values)]:
        if values:
            stats_data.append({
                'Stage': name,
                'Count': len(values),
                'Mean': f"{np.mean(values):.2f}",
                'Median': f"{np.median(values):.2f}",
                'Std': f"{np.std(values):.2f}",
                'Min': f"{np.min(values):.2f}",
                'Max': f"{np.max(values):.2f}"
            })
    
    stats_df = pd.DataFrame(stats_data)
    print("\nValue Statistics:")
    print(stats_df.to_string(index=False))
    
    # Visualizations
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # Histograms
    axes[0, 0].hist(raw_values, bins=30, alpha=0.7, color='#e74c3c', edgecolor='black')
    axes[0, 0].set_title('Raw Data - Value Distribution')
    axes[0, 0].set_xlabel('Value')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].grid(alpha=0.3)
    
    axes[0, 1].hist(cleaned_values, bins=30, alpha=0.7, color='#27ae60', edgecolor='black')
    axes[0, 1].set_title('Cleaned Data - Value Distribution')
    axes[0, 1].set_xlabel('Value')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].grid(alpha=0.3)
    
    # Box plots
    box_data = [raw_values, cleaned_values, curated_values]
    box_labels = ['Raw', 'Cleaned', 'Curated']
    bp = axes[1, 0].boxplot(box_data, labels=box_labels, patch_artist=True)
    for patch, color in zip(bp['boxes'], ['#e74c3c', '#27ae60', '#3498db']):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    axes[1, 0].set_title('Value Distribution - Box Plot Comparison')
    axes[1, 0].set_ylabel('Value')
    axes[1, 0].grid(axis='y', alpha=0.3)
    
    # Overlaid histograms
    axes[1, 1].hist(raw_values, bins=30, alpha=0.5, color='#e74c3c', label='Raw', edgecolor='black')
    axes[1, 1].hist(cleaned_values, bins=30, alpha=0.5, color='#27ae60', label='Cleaned', edgecolor='black')
    axes[1, 1].set_title('Overlaid Distribution Comparison')
    axes[1, 1].set_xlabel('Value')
    axes[1, 1].set_ylabel('Frequency')
    axes[1, 1].legend()
    axes[1, 1].grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/02_value_distributions.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {OUTPUT_DIR}/02_value_distributions.png")
    plt.close()
    
    return stats_df


def analyze_time_series(curated_data, features_data):
    """
    Analyze time series patterns and compare raw vs smoothed features.
    """
    print("\n" + "="*60)
    print("TIME SERIES ANALYSIS")
    print("="*60)
    
    # Convert to DataFrames
    curated_df = pd.DataFrame(curated_data)
    features_df = pd.DataFrame(features_data)
    
    if len(curated_df) == 0 or 'timestamp' not in curated_df.columns:
        print("Not enough time series data available")
        return
    
    # Convert timestamps
    curated_df['timestamp'] = pd.to_datetime(curated_df['timestamp'], errors='coerce')
    features_df['timestamp'] = pd.to_datetime(features_df['timestamp'], errors='coerce')
    
    # Sort by time
    curated_df = curated_df.sort_values('timestamp')
    features_df = features_df.sort_values('timestamp')
    
    # Select one entity for visualization
    if 'entity_id' in curated_df.columns:
        top_entity = curated_df['entity_id'].value_counts().index[0]
        entity_curated = curated_df[curated_df['entity_id'] == top_entity].copy()
        entity_features = features_df[features_df['entity_id'] == top_entity].copy()
        
        print(f"\nAnalyzing entity: {top_entity}")
        print(f"Events for this entity: {len(entity_curated)}")
    else:
        entity_curated = curated_df
        entity_features = features_df
    
    # Create time series plots
    fig, axes = plt.subplots(3, 1, figsize=(15, 12))
    
    # Plot 1: Raw values vs Rolling mean
    if 'value' in entity_curated.columns and len(entity_curated) > 0:
        axes[0].plot(entity_curated['timestamp'], entity_curated['value'], 
                    'o-', alpha=0.6, label='Raw Value', color='#3498db')
        
        if 'rolling_mean_15min' in entity_features.columns:
            axes[0].plot(entity_features['timestamp'], entity_features['rolling_mean_15min'],
                        '-', linewidth=2, label='Rolling Mean (15min)', color='#e74c3c')
        
        axes[0].set_title('Raw Values vs Smoothed (Rolling Mean)')
        axes[0].set_xlabel('Timestamp')
        axes[0].set_ylabel('Value')
        axes[0].legend()
        axes[0].grid(alpha=0.3)
        axes[0].tick_params(axis='x', rotation=45)
    
    # Plot 2: Z-scores over time
    if 'z_score' in entity_curated.columns:
        axes[1].plot(entity_curated['timestamp'], entity_curated['z_score'],
                    'o-', alpha=0.6, color='#9b59b6')
        axes[1].axhline(y=2, color='red', linestyle='--', alpha=0.5, label='Outlier threshold (±2)')
        axes[1].axhline(y=-2, color='red', linestyle='--', alpha=0.5)
        axes[1].axhline(y=0, color='gray', linestyle='-', alpha=0.3)
        axes[1].set_title('Z-Score Over Time (Anomaly Detection)')
        axes[1].set_xlabel('Timestamp')
        axes[1].set_ylabel('Z-Score')
        axes[1].legend()
        axes[1].grid(alpha=0.3)
        axes[1].tick_params(axis='x', rotation=45)
    
    # Plot 3: Event frequency over time
    if 'event_frequency_per_min' in entity_features.columns:
        axes[2].plot(entity_features['timestamp'], entity_features['event_frequency_per_min'],
                    'o-', alpha=0.6, color='#27ae60')
        axes[2].axhline(y=2, color='red', linestyle='--', alpha=0.5, 
                       label='High activity threshold')
        axes[2].set_title('Event Frequency Over Time')
        axes[2].set_xlabel('Timestamp')
        axes[2].set_ylabel('Events per Minute')
        axes[2].legend()
        axes[2].grid(alpha=0.3)
        axes[2].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/03_time_series_analysis.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {OUTPUT_DIR}/03_time_series_analysis.png")
    plt.close()


def analyze_pipeline_flow(raw_data, cleaned_data, curated_data, features_data):
    """
    Show record counts through the pipeline.
    """
    print("\n" + "="*60)
    print("PIPELINE FLOW ANALYSIS")
    print("="*60)
    
    # Count records at each stage
    counts = {
        'Raw Data': len(raw_data),
        'Cleaned Events': len([r for r in cleaned_data if r.get('value') is not None]),
        'Curated Events': len(curated_data),
        'Feature Records': len(features_data)
    }
    
    # Count dropped/invalid records
    cleaned_invalid = len([r for r in cleaned_data if r.get('data_quality_issue') is not None])
    
    print("\nRecord Counts Through Pipeline:")
    for stage, count in counts.items():
        print(f"  {stage}: {count}")
    print(f"  Records with quality issues: {cleaned_invalid}")
    
    # Calculate drop rates
    drop_clean = counts['Raw Data'] - counts['Cleaned Events']
    drop_curate = counts['Cleaned Events'] - counts['Curated Events']
    
    print(f"\nRecords dropped during cleaning: {drop_clean} ({drop_clean/counts['Raw Data']*100:.1f}%)")
    print(f"Records not curated: {drop_curate}")
    
    # Visualize pipeline flow
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Bar chart of counts
    stages = list(counts.keys())
    stage_counts = list(counts.values())
    colors = ['#e74c3c', '#f39c12', '#27ae60', '#3498db']
    
    bars = ax1.bar(stages, stage_counts, color=colors, alpha=0.7, edgecolor='black')
    ax1.set_ylabel('Number of Records')
    ax1.set_title('Records Through Pipeline Stages')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')
    
    # Funnel chart
    funnel_data = [
        ('Raw Data', counts['Raw Data']),
        ('Cleaned', counts['Cleaned Events']),
        ('Curated', counts['Curated Events']),
        ('Features', counts['Feature Records'])
    ]
    
    y_pos = np.arange(len(funnel_data))
    widths = [item[1] for item in funnel_data]
    labels = [f"{item[0]}\n({item[1]} records)" for item in funnel_data]
    
    ax2.barh(y_pos, widths, color=colors, alpha=0.7, edgecolor='black')
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(labels)
    ax2.set_xlabel('Number of Records')
    ax2.set_title('Pipeline Funnel View')
    ax2.invert_yaxis()
    ax2.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/04_pipeline_flow.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {OUTPUT_DIR}/04_pipeline_flow.png")
    plt.close()


def analyze_feature_correlations(features_data):
    """
    Analyze correlations between engineered features.
    """
    print("\n" + "="*60)
    print("FEATURE CORRELATION ANALYSIS")
    print("="*60)
    
    if len(features_data) == 0:
        print("No feature data available")
        return
    
    features_df = pd.DataFrame(features_data)
    
    # Select numeric features
    numeric_features = [
        'value', 'value_delta', 'z_score', 
        'rolling_mean_15min', 'rolling_std_15min',
        'inter_arrival_time_sec', 'rate_of_change',
        'event_frequency_per_min', 'deviation_from_rolling_mean'
    ]
    
    available_features = [f for f in numeric_features if f in features_df.columns]
    
    if len(available_features) < 2:
        print("Not enough numeric features for correlation analysis")
        return
    
    # Calculate correlation matrix
    corr_data = features_df[available_features].corr()
    
    # Plot heatmap
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_data, annot=True, cmap='coolwarm', center=0,
                fmt='.2f', square=True, linewidths=1, cbar_kws={"shrink": 0.8})
    plt.title('Feature Correlation Heatmap', fontsize=16, pad=20)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/05_feature_correlations.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {OUTPUT_DIR}/05_feature_correlations.png")
    plt.close()
    
    # Print top correlations
    print("\nTop Feature Correlations:")
    corr_pairs = []
    for i in range(len(corr_data.columns)):
        for j in range(i+1, len(corr_data.columns)):
            corr_pairs.append({
                'Feature 1': corr_data.columns[i],
                'Feature 2': corr_data.columns[j],
                'Correlation': corr_data.iloc[i, j]
            })
    
    corr_df = pd.DataFrame(corr_pairs)
    corr_df = corr_df.sort_values('Correlation', key=abs, ascending=False).head(10)
    print(corr_df.to_string(index=False))


def generate_summary_report(raw_data, cleaned_data, curated_data, features_data):
    """
    Generate a comprehensive summary report.
    """
    print("\n" + "="*60)
    print("SUMMARY REPORT")
    print("="*60)
    
    report = []
    report.append("# Data Quality Improvement Summary")
    report.append("")
    report.append("## Pipeline Overview")
    report.append(f"- Raw records ingested: {len(raw_data)}")
    report.append(f"- Cleaned records: {len(cleaned_data)}")
    report.append(f"- Curated events: {len(curated_data)}")
    report.append(f"- Feature records: {len(features_data)}")
    report.append("")
    
    # Data quality issues found
    issues = [r for r in cleaned_data if r.get('data_quality_issue')]
    report.append(f"## Data Quality Issues")
    report.append(f"- Records with quality flags: {len(issues)}")
    
    # Outliers
    outliers = [r for r in curated_data if r.get('is_outlier')]
    report.append(f"- Outliers detected: {len(outliers)}")
    
    # Anomalies
    if len(features_data) > 0:
        anomalies = [r for r in features_data if r.get('is_anomaly')]
        report.append(f"- Anomalies detected: {len(anomalies)}")
    
    report.append("")
    report.append("## Visualizations Generated")
    report.append("1. Missing Values Comparison (before/after cleaning)")
    report.append("2. Value Distributions (raw vs cleaned vs curated)")
    report.append("3. Time Series Analysis (raw vs smoothed features)")
    report.append("4. Pipeline Flow (records through stages)")
    report.append("5. Feature Correlations (engineered features)")
    
    # Save report
    report_text = "\n".join(report)
    with open(f'{OUTPUT_DIR}/00_summary_report.txt', 'w') as f:
        f.write(report_text)
    
    print(report_text)
    print(f"\n✓ Saved: {OUTPUT_DIR}/00_summary_report.txt")


def main():
    """
    Main exploration and analysis function.
    """
    print("="*60)
    print("DATA MINING PROJECT - EXPLORATORY ANALYSIS")
    print("="*60)
    
    # Load all data
    raw_data, cleaned_data, curated_data, features_data = load_data()
    
    # Run analyses
    analyze_data_quality(raw_data, cleaned_data)
    analyze_value_distributions(raw_data, cleaned_data, curated_data)
    analyze_time_series(curated_data, features_data)
    analyze_pipeline_flow(raw_data, cleaned_data, curated_data, features_data)
    analyze_feature_correlations(features_data)
    
    # Generate summary report
    generate_summary_report(raw_data, cleaned_data, curated_data, features_data)
    
    print("\n" + "="*60)
    print("EXPLORATION COMPLETE!")
    print(f"All visualizations saved to: {OUTPUT_DIR}/")
    print("="*60)


if __name__ == "__main__":
    main()
