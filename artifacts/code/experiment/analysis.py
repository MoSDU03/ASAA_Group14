#/usr/bin/env python3
"""
Statistical Analysis Script for MQTT vs HTTP Experiment
Performs t-tests, calculates effect sizes, generates plots
"""

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys

def load_data(filename):
    """Load experimental data from CSV"""
    try:
        df = pd.read_csv(filename)
        print(f" Loaded {len(df)} samples from {filename}")
        return df
    except FileNotFoundError:
        print(f" File not found: {filename}")
        print("  Run test_harness.py first to generate data")
        sys.exit(1)

def descriptive_statistics(df):
    """Calculate and print descriptive statistics"""
    print("\n" + "="*70)
    print(" DESCRIPTIVE STATISTICS")
    print("="*70)
    
    mqtt_data = df[df['protocol'] == 'MQTT']['latency_ms']
    http_data = df[df['protocol'] == 'HTTP']['latency_ms']
    
    print(f"\n MQTT (n={len(mqtt_data)}):")
    print(f"   Mean:     {mqtt_data.mean():.3f} ms")
    print(f"   Std Dev:  {mqtt_data.std():.3f} ms")
    print(f"   Median:   {mqtt_data.median():.3f} ms")
    print(f"   Min:      {mqtt_data.min():.3f} ms")
    print(f"   Max:      {mqtt_data.max():.3f} ms")
    print(f"   Q1/Q3:    {mqtt_data.quantile(0.25):.3f} / {mqtt_data.quantile(0.75):.3f} ms")
    
    print(f"\nHTTP (n={len(http_data)}):")
    print(f"   Mean:     {http_data.mean():.3f} ms")
    print(f"   Std Dev:  {http_data.std():.3f} ms")
    print(f"   Median:   {http_data.median():.3f} ms")
    print(f"   Min:      {http_data.min():.3f} ms")
    print(f"   Max:      {http_data.max():.3f} ms")
    print(f"   Q1/Q3:    {http_data.quantile(0.25):.3f} / {http_data.quantile(0.75):.3f} ms")
    
    return mqtt_data, http_data

def test_normality(mqtt_data, http_data):
    """Test for normality using Shapiro-Wilk"""
    print("\n" + "="*70)
    print(" NORMALITY TEST (Shapiro-Wilk)")
    print("="*70)
    
    # Sample subset if data is too large (Shapiro-Wilk limit is 5000)
    mqtt_sample = mqtt_data.sample(min(5000, len(mqtt_data)), random_state=42)
    http_sample = http_data.sample(min(5000, len(http_data)), random_state=42)
    
    mqtt_stat, mqtt_p = stats.shapiro(mqtt_sample)
    http_stat, http_p = stats.shapiro(http_sample)
    
    print(f"\n MQTT:")
    print(f"   W-statistic: {mqtt_stat:.4f}")
    print(f"   p-value:     {mqtt_p:.4f}")
    print(f"   Normal?      {' Yes (p > 0.05)' if mqtt_p > 0.05 else ' No (p < 0.05)'}")
    
    print(f"\n HTTP:")
    print(f"   W-statistic: {http_stat:.4f}")
    print(f"   p-value:     {http_p:.4f}")
    print(f"   Normal?      {' Yes (p > 0.05)' if http_p > 0.05 else ' No (p < 0.05)'}")
    
    parametric = mqtt_p > 0.05 and http_p > 0.05
    return parametric

def hypothesis_test(mqtt_data, http_data, parametric=True):
    """Perform t-test or Mann-Whitney U test"""
    print("\n" + "="*70)
    print(" HYPOTHESIS TEST")
    print("="*70)
    
    print(f"\nH: _MQTT = _HTTP")
    print(f"H: _MQTT < _HTTP (one-tailed)")
    print(f"Significance level:  = 0.05")
    
    if parametric:
        print(f"\nUsing: Independent samples t-test (parametric)")
        t_stat, p_value = stats.ttest_ind(mqtt_data, http_data, alternative='less')
        print(f"\n   t-statistic: {t_stat:.3f}")
        print(f"   p-value:     {p_value:.6f}")
    else:
        print(f"\nUsing: Mann-Whitney U test (non-parametric)")
        u_stat, p_value = stats.mannwhitneyu(mqtt_data, http_data, alternative='less')
        print(f"\n   U-statistic: {u_stat:.3f}")
        print(f"   p-value:     {p_value:.6f}")
    
    if p_value < 0.05:
        print(f"\nRESULT: REJECT H (p < 0.05)")
        print(f"   Conclusion: MQTT has significantly lower latency than HTTP")
    else:
        print(f"\n RESULT: FAIL TO REJECT H (p  0.05)")
        print(f"   Conclusion: No significant difference detected")
    
    return p_value

def effect_size(mqtt_data, http_data):
    """Calculate Cohen's d effect size"""
    print("\n" + "="*70)
    print(" EFFECT SIZE (Cohen's d)")
    print("="*70)
    
    mean_diff = http_data.mean() - mqtt_data.mean()
    pooled_std = np.sqrt((mqtt_data.std()**2 + http_data.std()**2) / 2)
    cohens_d = mean_diff / pooled_std
    
    print(f"\n   Mean difference: {mean_diff:.3f} ms")
    print(f"   Pooled std dev:  {pooled_std:.3f} ms")
    print(f"   Cohen's d:       {cohens_d:.3f}")
    
    if abs(cohens_d) < 0.2:
        magnitude = "negligible"
    elif abs(cohens_d) < 0.5:
        magnitude = "small"
    elif abs(cohens_d) < 0.8:
        magnitude = "medium"
    else:
        magnitude = "large"
    
    print(f"   Effect size:     {magnitude}")
    
    # Practical significance
    speedup = http_data.mean() / mqtt_data.mean()
    improvement = ((http_data.mean() - mqtt_data.mean()) / http_data.mean()) * 100
    
    print(f"\n Practical Significance:")
    print(f"   MQTT is {speedup:.2f}x faster")
    print(f"   Latency reduction: {improvement:.1f}%")
    
    return cohens_d

def generate_plots(mqtt_data, http_data):
    """Generate visualization plots"""
    print("\n" + "="*70)
    print(" GENERATING PLOTS")
    print("="*70)
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 8)
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('MQTT vs HTTP Performance Comparison', fontsize=16, fontweight='bold')
    
    # 1. Box plot
    ax1 = axes[0, 0]
    data_box = pd.DataFrame({
        'MQTT': mqtt_data,
        'HTTP': http_data
    })
    data_box.boxplot(ax=ax1)
    ax1.set_title('Latency Distribution (Box Plot)')
    ax1.set_ylabel('Latency (ms)')
    ax1.grid(True, alpha=0.3)
    
    # 2. Histogram
    ax2 = axes[0, 1]
    ax2.hist(mqtt_data, bins=50, alpha=0.7, label='MQTT', color='blue', density=True)
    ax2.hist(http_data, bins=50, alpha=0.7, label='HTTP', color='orange', density=True)
    ax2.set_title('Latency Distribution (Histogram)')
    ax2.set_xlabel('Latency (ms)')
    ax2.set_ylabel('Density')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Cumulative distribution
    ax3 = axes[1, 0]
    mqtt_sorted = np.sort(mqtt_data)
    http_sorted = np.sort(http_data)
    mqtt_cdf = np.arange(1, len(mqtt_sorted)+1) / len(mqtt_sorted)
    http_cdf = np.arange(1, len(http_sorted)+1) / len(http_sorted)
    
    ax3.plot(mqtt_sorted, mqtt_cdf, label='MQTT', linewidth=2)
    ax3.plot(http_sorted, http_cdf, label='HTTP', linewidth=2)
    ax3.set_title('Cumulative Distribution Function')
    ax3.set_xlabel('Latency (ms)')
    ax3.set_ylabel('Cumulative Probability')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Summary statistics
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    summary_text = f"""
     Summary Statistics
    
    MQTT:
      Mean:   {mqtt_data.mean():.2f} ms
      Median: {mqtt_data.median():.2f} ms
      Std:    {mqtt_data.std():.2f} ms
    
    HTTP:
      Mean:   {http_data.mean():.2f} ms
      Median: {http_data.median():.2f} ms
      Std:    {http_data.std():.2f} ms
    
    Comparison:
      Speedup: {http_data.mean() / mqtt_data.mean():.2f}x
      Improvement: {((http_data.mean() - mqtt_data.mean()) / http_data.mean()) * 100:.1f}%
    """
    
    ax4.text(0.1, 0.5, summary_text, fontsize=11, family='monospace',
             verticalalignment='center')
    
    plt.tight_layout()
    
    # Save plot
    filename = 'performance_comparison.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"\n Plot saved: {filename}")
    
    plt.close()

def main():
    """Main analysis workflow"""
    print("\n" + "="*70)
    print(" MQTT vs HTTP PERFORMANCE ANALYSIS")
    print("="*70)
    
    # Find most recent data file
    data_files = list(Path('.').glob('raw_data_*.csv'))
    if not data_files:
        print("\n No data files found")
        print("  Run test_harness.py first")
        return
    
    latest_file = max(data_files, key=lambda p: p.stat().st_mtime)
    print(f"\nAnalyzing: {latest_file}")
    
    # Load data
    df = load_data(latest_file)
    
    # Descriptive statistics
    mqtt_data, http_data = descriptive_statistics(df)
    
    # Test normality
    parametric = test_normality(mqtt_data, http_data)
    
    # Hypothesis test
    p_value = hypothesis_test(mqtt_data, http_data, parametric)
    
    # Effect size
    cohens_d = effect_size(mqtt_data, http_data)
    
    # Generate plots
    generate_plots(mqtt_data, http_data)
    
    # Final summary
    print("\n" + "="*70)
    print("ANALYSIS COMPLETE")
    print("="*70)
    print(f"\nResults:")
    print(f"  - Statistical significance: {'YES (p < 0.05)' if p_value < 0.05 else 'NO (p  0.05)'}")
    print(f"  - Effect size: {cohens_d:.3f} ({'large' if abs(cohens_d) >= 0.8 else 'medium' if abs(cohens_d) >= 0.5 else 'small'})")
    print(f"  - Practical significance: {http_data.mean() / mqtt_data.mean():.2f}x speedup")
    print(f"\nGenerated files:")
    print(f"  - performance_comparison.png")
    print("\n" + "="*70)

if __name__ == "__main__":
    main()