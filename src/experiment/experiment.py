import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import sys

def connect_database(db_config):
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        sys.exit(1)

def extract_experiment_data(conn, min_samples=50):
    query = """
    SELECT 
        event_id,
        timestamp,
        can_id,
        fill_level_ml,
        cycle_time_ms,
        fill_duration_ms
    FROM filling_events
    WHERE event_type = 'fill_complete'
        AND fill_level_ml BETWEEN 325.0 AND 335.0
        AND cycle_time_ms IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT %s
    """
    
    df = pd.read_sql_query(query, conn, params=(min_samples * 2,))
    
    print(f"Extracted {len(df)} successful fill cycles from database")
    return df

def analyze_performance(df, requirement_min=600, requirement_max=1500):
    cycle_times = df['cycle_time_ms'].values
    
    stats = {
        'count': len(cycle_times),
        'mean': np.mean(cycle_times),
        'std': np.std(cycle_times),
        'min': np.min(cycle_times),
        'max': np.max(cycle_times),
        'median': np.median(cycle_times),
        'q25': np.percentile(cycle_times, 25),
        'q75': np.percentile(cycle_times, 75),
    }
    
    within_spec = np.sum((cycle_times >= requirement_min) & (cycle_times <= requirement_max))
    stats['within_spec_count'] = within_spec
    stats['within_spec_percent'] = (within_spec / len(cycle_times)) * 100
    
    stats['ci_95_lower'] = stats['mean'] - 1.96 * (stats['std'] / np.sqrt(len(cycle_times)))
    stats['ci_95_upper'] = stats['mean'] + 1.96 * (stats['std'] / np.sqrt(len(cycle_times)))
    
    return stats, cycle_times

def print_results(stats, requirement_min=600, requirement_max=1500):
    print("\n" + "=" * 70)
    print("EXPERIMENT RESULTS: Performance Quality Attribute Validation")
    print("=" * 70)
    
    print(f"\nRequirement (NFR-01): Cycle time between {requirement_min}-{requirement_max}ms")
    
    print(f"\nDescriptive Statistics (n={stats['count']}):")
    print(f"  Mean:     {stats['mean']:.2f} ms")
    print(f"  Std Dev:  {stats['std']:.2f} ms")
    print(f"  Median:   {stats['median']:.2f} ms")
    print(f"  Min:      {stats['min']:.2f} ms")
    print(f"  Max:      {stats['max']:.2f} ms")
    print(f"  Q1 (25%): {stats['q25']:.2f} ms")
    print(f"  Q3 (75%): {stats['q75']:.2f} ms")
    
    print(f"\n95% Confidence Interval for Mean:")
    print(f"  [{stats['ci_95_lower']:.2f}, {stats['ci_95_upper']:.2f}] ms")
    
    print(f"\nRequirement Compliance:")
    print(f"  Samples within spec: {stats['within_spec_count']} / {stats['count']}")
    print(f"  Success rate: {stats['within_spec_percent']:.1f}%")
    
    requirement_met = (stats['ci_95_lower'] >= requirement_min and 
                      stats['ci_95_upper'] <= requirement_max)
    
    print(f"\nConclusion:")
    if requirement_met:
        print(f"NFR-01 VALIDATED: Mean cycle time with 95% confidence falls within")
        print(f"the required range [{requirement_min}, {requirement_max}]ms")
    else:
        print(f"NFR-01 NOT FULLY VALIDATED: Confidence interval extends outside")
        print(f"the required range [{requirement_min}, {requirement_max}]ms")
        
    print("\n" + "=" * 70)

def save_results(df, stats, output_file):
    df.to_csv(output_file, index=False)
    print(f"\nDetailed data saved to: {output_file}")
    
    stats_file = output_file.replace('.csv', '_stats.csv')
    stats_df = pd.DataFrame([stats])
    stats_df.to_csv(stats_file, index=False)
    print(f"Statistics saved to: {stats_file}")

def main():
    parser = argparse.ArgumentParser(description='Performance validation experiment')
    parser.add_argument('--runs', type=int, default=100, help='Minimum number of samples')
    parser.add_argument('--output', type=str, default='data/results.csv', help='Output file')
    parser.add_argument('--host', type=str, default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--db', type=str, default='filling_db', help='Database name')
    parser.add_argument('--user', type=str, default='filling_user', help='Database user')
    parser.add_argument('--password', type=str, default='filling_pass', help='Database password')
    
    args = parser.parse_args()
    
    print(f"Starting experiment: Performance Quality Attribute Validation")
    print(f"Target samples: {args.runs}")
    print(f"Connecting to database: {args.host}:{args.port}/{args.db}")
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.db,
        'user': args.user,
        'password': args.password
    } 

    conn = connect_database(db_config)
    
    df = extract_experiment_data(conn, min_samples=args.runs)
    
    if len(df) < args.runs:
        print(f"\nWarning: Only {len(df)} samples available (requested {args.runs})")
        print("Consider running the system longer to collect more data")
    
    stats, cycle_times = analyze_performance(df)
    
    print_results(stats)
    
    save_results(df, stats, args.output)
    
    conn.close()

if __name__ == "__main__":
    main()
