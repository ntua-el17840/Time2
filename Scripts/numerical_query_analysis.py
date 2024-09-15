import os
import re
import csv

# Directory where the result files are located
RESULTS_DIR = '../Results'

# Regular expression to extract data from the result files
pattern = re.compile(
    r"Run complete after (?P<num_queries>\d+) queries with (?P<workers>\d+) workers \(Overall query rate (?P<overall_rate>[\d\.]+) queries/sec\):*\n"
    r".*:\n"
    r"min:\s*(?P<min_time>[\d\.]+)ms, med:\s*(?P<med_time>[\d\.]+)ms, mean:\s*(?P<mean_time>[\d\.]+)ms, max:\s*(?P<max_time>[\d\.]+)ms, stddev:\s*(?P<stddev_time>[\d\.]+)ms, sum:\s*(?P<sum_time>[\d\.]+)sec, count:\s*(?P<query_count>\d+)\n"
    r"all queries\s*:\n"
    r"min:\s*(?P<all_min_time>[\d\.]+)ms, med:\s*(?P<all_med_time>[\d\.]+)ms, mean:\s*(?P<all_mean_time>[\d\.]+)ms, max:\s*(?P<all_max_time>[\d\.]+)ms, stddev:\s*(?P<all_stddev_time>[\d\.]+)ms, sum:\s*(?P<all_sum_time>[\d\.]+)sec, count:\s*(?P<all_query_count>\d+)\n"
    r"wall clock time: (?P<wall_clock_time>[\d\.]+)sec"
)

# Function to parse a result file and extract metrics
def parse_result_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    match = pattern.search(content)
    if match:
        metrics = {
            'num_queries': int(match.group('num_queries')),
            'workers': int(match.group('workers')),
            'overall_rate': float(match.group('overall_rate')),
            'min_time': float(match.group('min_time')),
            'med_time': float(match.group('med_time')),
            'mean_time': float(match.group('mean_time')),
            'max_time': float(match.group('max_time')),
            'stddev_time': float(match.group('stddev_time')),
            'sum_time': float(match.group('sum_time')),
            'wall_clock_time': float(match.group('wall_clock_time'))
        }
        return metrics
    else:
        print(f"Failed to parse file: {filepath}")
        return None

# Collect result files
timescale_files = {}
influxdb_files = {}
for filename in os.listdir(RESULTS_DIR):
    if filename.endswith('.txt'):
        filepath = os.path.join(RESULTS_DIR, filename)
        if 'timescaledb' in filename.lower() or 'timescale' in filename.lower():
            timescale_files[filename] = filepath
        elif 'influxdb' in filename.lower():
            influxdb_files[filename] = filepath

# Prepare to store comparison results
comparison_results = []

# Compare each pair of TimescaleDB and InfluxDB files
for ts_filename, ts_filepath in timescale_files.items():
    # Derive the corresponding InfluxDB filename
    influx_filename = ts_filename.replace('timescaledb', 'influxdb').replace('timescale', 'influxdb')
    influx_filepath = influxdb_files.get(influx_filename)
    if influx_filepath:
        # Parse the result files
        ts_metrics = parse_result_file(ts_filepath)
        influx_metrics = parse_result_file(influx_filepath)
        if ts_metrics and influx_metrics:
            # Compute differences
            diff = {
                'Test': ts_filename,
                'Num Queries': ts_metrics['num_queries'],
                'Workers': ts_metrics['workers'],
                'TimescaleDB Mean Time (ms)': ts_metrics['mean_time'],
                'InfluxDB Mean Time (ms)': influx_metrics['mean_time'],
                'Difference (ms)': influx_metrics['mean_time'] - ts_metrics['mean_time'],
                'Percentage Difference (%)': ((influx_metrics['mean_time'] - ts_metrics['mean_time']) / ts_metrics['mean_time']) * 100,
                'TimescaleDB Overall Rate (qps)': ts_metrics['overall_rate'],
                'InfluxDB Overall Rate (qps)': influx_metrics['overall_rate'],
                'Rate Difference (qps)': influx_metrics['overall_rate'] - ts_metrics['overall_rate'],
                'Rate Percentage Difference (%)': ((influx_metrics['overall_rate'] - ts_metrics['overall_rate']) / ts_metrics['overall_rate']) * 100
            }
            comparison_results.append(diff)
    else:
        print(f"Corresponding InfluxDB file not found for {ts_filename}")

# Output the comparison results
print("Comparison Results:")
for result in comparison_results:
    print(f"Test: {result['Test']}")
    print(f"  TimescaleDB Mean Time: {result['TimescaleDB Mean Time (ms)']} ms")
    print(f"  InfluxDB Mean Time: {result['InfluxDB Mean Time (ms)']} ms")
    print(f"  Difference: {result['Difference (ms)']:.2f} ms")
    print(f"  Percentage Difference: {result['Percentage Difference (%)']:.2f}%")
    print(f"  TimescaleDB Overall Rate: {result['TimescaleDB Overall Rate (qps)']} qps")
    print(f"  InfluxDB Overall Rate: {result['InfluxDB Overall Rate (qps)']} qps")
    print(f"  Rate Difference: {result['Rate Difference (qps)']:.2f} qps")
    print(f"  Rate Percentage Difference: {result['Rate Percentage Difference (%)']:.2f}%")
    print()

# Optionally, write the results to a CSV file
csv_file = os.path.join(RESULTS_DIR, 'comparison_results.csv')
with open(csv_file, 'w', newline='') as csvfile:
    fieldnames = [
        'Test',
        'Num Queries',
        'Workers',
        'TimescaleDB Mean Time (ms)',
        'InfluxDB Mean Time (ms)',
        'Difference (ms)',
        'Percentage Difference (%)',
        'TimescaleDB Overall Rate (qps)',
        'InfluxDB Overall Rate (qps)',
        'Rate Difference (qps)',
        'Rate Percentage Difference (%)'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in comparison_results:
        writer.writerow(result)

print(f"Comparison results written to {csv_file}")
