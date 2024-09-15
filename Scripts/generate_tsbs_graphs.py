import os
import re
import matplotlib.pyplot as plt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the path to your results files (adjust this as needed)
RESULTS_DIR = "../Results"  # The directory where the results files are located

# Define the databases and workers you want to process
DATABASES = ["iot_data", "iot_data_medium", "iot_data_small"]
WORKERS = [1, 4, 8, 16]
QUERY_TYPES = ["last-loc", "long-driving-sessions", "avg-load", "daily-activity", "breakdown-frequency"]

# Data structure to hold the parsed results
data = {
    'workers': [],
    'num_queries': [],
    'overall_rate': [],
    'min_time': [],
    'median_time': [],
    'mean_time': [],
    'max_time': [],
    'database': [],
    'query_type': [],
    'db_engine': []
}

# TimescaleDB regex pattern
timescale_pattern = re.compile(
    r"Run complete after (?P<num_queries>\d+) queries with (?P<workers>\d+) workers \(Overall query rate (?P<overall_rate>[\d\.]+) queries/sec\):*\n"
    r"TimescaleDB.*:\n"  # Matches any description in the second line
    r"min:\s*(?P<min_time>[\d\.]+)ms, med:\s*(?P<med_time>[\d\.]+)ms, mean:\s*(?P<mean_time>[\d\.]+)ms, max:\s*(?P<max_time>[\d\.]+)ms, stddev:\s*(?P<stddev_time>[\d\.]+)ms, sum:\s*(?P<sum_time>[\d\.]+)sec, count:\s*(?P<query_count>\d+)\n"
    r"all queries\s*:\n"
    r"min:\s*(?P<all_min_time>[\d\.]+)ms, med:\s*(?P<all_med_time>[\d\.]+)ms, mean:\s*(?P<all_mean_time>[\d\.]+)ms, max:\s*(?P<all_max_time>[\d\.]+)ms, stddev:\s*(?P<all_stddev_time>[\d\.]+)ms, sum:\s*(?P<all_sum_time>[\d\.]+)sec, count:\s*(?P<all_query_count>\d+)\n"
    r"wall clock time: (?P<wall_clock_time>[\d\.]+)sec"
)

# InfluxDB regex pattern
influxdb_pattern = re.compile(
    r"Run complete after (?P<num_queries>\d+) queries with (?P<workers>\d+) workers \(Overall query rate (?P<overall_rate>[\d\.]+) queries/sec\):*\n"
    r"InfluxDB.*:\n"  # Matches any description in the second line
    r"min:\s*(?P<min_time>[\d\.]+)ms, med:\s*(?P<med_time>[\d\.]+)ms, mean:\s*(?P<mean_time>[\d\.]+)ms, max:\s*(?P<max_time>[\d\.]+)ms, stddev:\s*(?P<stddev_time>[\d\.]+)ms, sum:\s*(?P<sum_time>[\d\.]+)sec, count:\s*(?P<query_count>\d+)\n"
    r"all queries\s*:\n"
    r"min:\s*(?P<all_min_time>[\d\.]+)ms, med:\s*(?P<all_med_time>[\d\.]+)ms, mean:\s*(?P<all_mean_time>[\d\.]+)ms, max:\s*(?P<all_max_time>[\d\.]+)ms, stddev:\s*(?P<all_stddev_time>[\d\.]+)ms, sum:\s*(?P<all_sum_time>[\d\.]+)sec, count:\s*(?P<all_query_count>\d+)\n"
    r"wall clock time: (?P<wall_clock_time>[\d\.]+)sec"
)


# Function to parse a result file and extract relevant data
def parse_result_file(file_path, db_name, query_type):
    logging.info(f"Parsing file: {file_path}")
    
    # Detect database engine type from file name or file content
    with open(file_path, 'r') as file:
        content = file.read()

    # Select the appropriate regex pattern based on the content
    if "TimescaleDB" in content:
        pattern = timescale_pattern
        logging.info("Using TimescaleDB pattern")
    elif "InfluxDB" in content:
        pattern = influxdb_pattern
        logging.info("Using InfluxDB pattern")
    else:
        logging.warning(f"Unknown DB engine in {file_path}")
        return

    # Use the selected regex pattern to find all matches in the file
    matches = list(pattern.finditer(content))
    
    if not matches:
        logging.warning(f"No matches found in {file_path}")
    else:
        logging.info(f"Found {len(matches)} matches in {file_path}")

    for match in matches:
        num_queries = match.group('num_queries')
        workers = match.group('workers')
        overall_rate = match.group('overall_rate')
        min_time = match.group('min_time')
        median_time = match.group('med_time')
        mean_time = match.group('mean_time')
        max_time = match.group('max_time')
        db_engine = "TimescaleDB" if "TimescaleDB" in content else "InfluxDB"

        logging.debug(
            f"Match details - Workers: {workers}, Num Queries: {num_queries}, "
            f"Overall Rate: {overall_rate}, Min Time: {min_time}, Median Time: {median_time}, "
            f"Mean Time: {mean_time}, Max Time: {max_time}, DB Engine: {db_engine}"
        )

        data['workers'].append(int(workers))
        data['num_queries'].append(int(num_queries))
        data['overall_rate'].append(float(overall_rate))
        data['min_time'].append(float(min_time))
        data['median_time'].append(float(median_time))
        data['mean_time'].append(float(mean_time))
        data['max_time'].append(float(max_time))
        data['database'].append(db_name)
        data['query_type'].append(query_type)
        data['db_engine'].append(db_engine)


# Function to generate individual graphs for each database engine
def generate_graphs():
    logging.info("Generating individual graphs for each database engine")
    db_engines = list(set(data['db_engine']))
    for db_name in DATABASES:
        for query_type in QUERY_TYPES:
            for db_engine in db_engines:
                # Filter data for the current database, query type, and db_engine
                filtered_data = {
                    key: [value[i] for i in range(len(data['database']))
                          if data['database'][i] == db_name and data['query_type'][i] == query_type and data['db_engine'][i] == db_engine]
                    for key, value in data.items()
                }

                if not filtered_data['workers']:
                    logging.info(f"No data available for {db_name}, {query_type}, {db_engine}")
                    continue  # Skip if there's no data for this combination

                logging.info(f"Generating graphs for {db_name}, {query_type}, {db_engine}")

                # Sort data by the number of workers
                sorted_indices = sorted(range(len(filtered_data['workers'])), key=lambda k: filtered_data['workers'][k])
                sorted_workers = [filtered_data['workers'][i] for i in sorted_indices]
                sorted_overall_rate = [filtered_data['overall_rate'][i] for i in sorted_indices]
                sorted_mean_time = [filtered_data['mean_time'][i] for i in sorted_indices]
                sorted_max_time = [filtered_data['max_time'][i] for i in sorted_indices]
                sorted_median_time = [filtered_data['median_time'][i] for i in sorted_indices]

                # Create a figure with subplots
                fig, ax = plt.subplots(2, 2, figsize=(12, 8))

                # Graph 1: Overall Query Rate vs Workers
                ax[0, 0].plot(sorted_workers, sorted_overall_rate, marker='o', label=f"{db_engine}")
                ax[0, 0].set_title(f"{db_name} - {query_type} - {db_engine} - Overall Query Rate vs Workers")
                ax[0, 0].set_xlabel("Number of Workers")
                ax[0, 0].set_ylabel("Overall Query Rate (queries/sec)")
                ax[0, 0].legend()

                # Graph 2: Mean Query Time vs Workers
                ax[0, 1].plot(sorted_workers, sorted_mean_time, marker='o', color="r", label=f"{db_engine}")
                ax[0, 1].set_title(f"{db_name} - {query_type} - {db_engine} - Mean Query Time vs Workers")
                ax[0, 1].set_xlabel("Number of Workers")
                ax[0, 1].set_ylabel("Mean Query Time (ms)")
                ax[0, 1].legend()

                # Graph 3: Max Query Time vs Workers
                ax[1, 0].plot(sorted_workers, sorted_max_time, marker='o', color="g", label=f"{db_engine}")
                ax[1, 0].set_title(f"{db_name} - {query_type} - {db_engine} - Max Query Time vs Workers")
                ax[1, 0].set_xlabel("Number of Workers")
                ax[1, 0].set_ylabel("Max Query Time (ms)")
                ax[1, 0].legend()

                # Graph 4: Median Query Time vs Workers
                ax[1, 1].plot(sorted_workers, sorted_median_time, marker='o', color="b", label=f"{db_engine}")
                ax[1, 1].set_title(f"{db_name} - {query_type} - {db_engine} - Median Query Time vs Workers")
                ax[1, 1].set_xlabel("Number of Workers")
                ax[1, 1].set_ylabel("Median Query Time (ms)")
                ax[1, 1].legend()

                # Adjust the layout and save the plot
                plt.tight_layout()
                plt.savefig(f"{RESULTS_DIR}/{db_name}_{query_type}_{db_engine}_graph.png")
                plt.close()
                logging.info(f"Saved graph to {RESULTS_DIR}/{db_name}_{query_type}_{db_engine}_graph.png")


# Function to generate comparison graphs between database engines
def generate_comparison_graphs():
    logging.info("Generating comparison graphs between database engines")
    for db_name in DATABASES:
        for query_type in QUERY_TYPES:
            # Filter data for the current database and query type
            filtered_data = {
                key: [value[i] for i in range(len(data['database']))
                      if data['database'][i] == db_name and data['query_type'][i] == query_type]
                for key, value in data.items()
            }

            if not filtered_data['workers']:
                logging.info(f"No data available for {db_name}, {query_type}")
                continue  # Skip if there's no data for this combination

            db_engines = list(set(filtered_data['db_engine']))

            logging.info(f"Generating comparison graphs for {db_name}, {query_type}")

            # Create a figure with subplots
            fig, ax = plt.subplots(2, 2, figsize=(12, 8))

            # For each db_engine, plot on the same axes
            for db_engine in db_engines:
                engine_data = {
                    key: [value[i] for i in range(len(filtered_data['db_engine']))
                          if filtered_data['db_engine'][i] == db_engine]
                    for key, value in filtered_data.items()
                }

                if not engine_data['workers']:
                    logging.info(f"No data available for engine {db_engine}")
                    continue

                # Sort the data based on workers to ensure proper plotting
                sorted_indices = sorted(range(len(engine_data['workers'])), key=lambda k: engine_data['workers'][k])
                sorted_workers = [engine_data['workers'][i] for i in sorted_indices]
                sorted_overall_rate = [engine_data['overall_rate'][i] for i in sorted_indices]
                sorted_mean_time = [engine_data['mean_time'][i] for i in sorted_indices]
                sorted_max_time = [engine_data['max_time'][i] for i in sorted_indices]
                sorted_median_time = [engine_data['median_time'][i] for i in sorted_indices]

                # Graph 1: Overall Query Rate vs Workers
                ax[0, 0].plot(sorted_workers, sorted_overall_rate, marker='o', label=f"{db_engine}")

                # Graph 2: Mean Query Time vs Workers
                ax[0, 1].plot(sorted_workers, sorted_mean_time, marker='o', label=f"{db_engine}")

                # Graph 3: Max Query Time vs Workers
                ax[1, 0].plot(sorted_workers, sorted_max_time, marker='o', label=f"{db_engine}")

                # Graph 4: Median Query Time vs Workers
                ax[1, 1].plot(sorted_workers, sorted_median_time, marker='o', label=f"{db_engine}")

            # Set titles, labels, legends
            ax[0, 0].set_title(f"{db_name} - {query_type} - Overall Query Rate vs Workers")
            ax[0, 0].set_xlabel("Number of Workers")
            ax[0, 0].set_ylabel("Overall Query Rate (queries/sec)")
            ax[0, 0].legend()

            ax[0, 1].set_title(f"{db_name} - {query_type} - Mean Query Time vs Workers")
            ax[0, 1].set_xlabel("Number of Workers")
            ax[0, 1].set_ylabel("Mean Query Time (ms)")
            ax[0, 1].legend()

            ax[1, 0].set_title(f"{db_name} - {query_type} - Max Query Time vs Workers")
            ax[1, 0].set_xlabel("Number of Workers")
            ax[1, 0].set_ylabel("Max Query Time (ms)")
            ax[1, 0].legend()

            ax[1, 1].set_title(f"{db_name} - {query_type} - Median Query Time vs Workers")
            ax[1, 1].set_xlabel("Number of Workers")
            ax[1, 1].set_ylabel("Median Query Time (ms)")
            ax[1, 1].legend()

            # Adjust the layout and save the plot
            plt.tight_layout()
            plt.savefig(f"{RESULTS_DIR}/{db_name}_{query_type}_comparison_graph.png")
            plt.close()
            logging.info(f"Saved comparison graph to {RESULTS_DIR}/{db_name}_{query_type}_comparison_graph.png")

# Function to extract the database name from the filename
def get_db_name_from_filename(file_name):
    matching_db_names = [db_name for db_name in DATABASES if db_name in file_name]
    if matching_db_names:
        # Return the longest matching db_name to avoid partial matches
        return max(matching_db_names, key=len)
    else:
        return None

# Main function to parse all result files and generate graphs
def main():
    logging.info("Starting to parse result files")
    # Iterate over all files in the result directory
    for file_name in os.listdir(RESULTS_DIR):
        if file_name.endswith(".txt"):  # Only process .txt files
            db_name = get_db_name_from_filename(file_name)
            if db_name:
                for query_type in QUERY_TYPES:
                    if query_type in file_name:
                        file_path = os.path.join(RESULTS_DIR, file_name)
                        logging.info(f"Processing {file_path}")
                        parse_result_file(file_path, db_name, query_type)
            else:
                logging.warning(f"No matching database name found in {file_name}")
    # Generate the individual graphs
    generate_graphs()
    # Generate the comparison graphs
    generate_comparison_graphs()
    logging.info("Completed processing")

if __name__ == "__main__":
    main()