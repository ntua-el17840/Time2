#!/bin/bash

# Configuration
HOST="http://localhost:8086"

# List of databases (buckets) to execute queries on
DATABASES=(
    "iot_data_small"
    "iot_data_medium"
    "iot_data"
)

# Path to the Results directory (where the result files will be stored)
RESULTS_DIR="../Results"

# Create the Results directory if it doesn't exist
mkdir -p "$RESULTS_DIR"

# List of query files to execute (you can modify this as needed)
QUERY_FILES=(
    "../Queries/influxdb-queries-last-loc"
    "../Queries/influxdb-queries-long-driving-sessions"
    "../Queries/influxdb-queries-avg-load"
    "../Queries/influxdb-queries-daily-activity"
    "../Queries/influxdb-queries-breakdown-frequency"
)

# List of worker counts to test
WORKERS=(1 4 8 16)

# Loop through each database (bucket)
for DB_NAME in "${DATABASES[@]}"; do
    echo "========================================="
    echo "Running queries on database: $DB_NAME"
    echo "========================================="

    # Loop through each query file
    for QUERY_FILE in "${QUERY_FILES[@]}"; do
        # Extract query name from the file path for logging purposes
        QUERY_NAME=$(basename "$QUERY_FILE")
        
        echo "-----------------------------------------"
        echo "Running queries from: $QUERY_FILE"
        echo "-----------------------------------------"
        
        # Loop through each worker count
        for WORKER_COUNT in "${WORKERS[@]}"; do
            echo "-----------------------------------------"
            echo "Running with $WORKER_COUNT workers on database: $DB_NAME"
            echo "-----------------------------------------"

            # The command to be run
            CMD="tsbs_run_queries_influx --urls=$HOST --db-name=\"$DB_NAME\" --file=\"$QUERY_FILE\" --workers=\"$WORKER_COUNT\""

            # Output the command before running it
            echo "Executing: $CMD"
            
            # Run the command and log the output to the Results directory
            eval "$CMD >> ${RESULTS_DIR}/results_${DB_NAME}_${QUERY_NAME}_${WORKER_COUNT}_workers.txt"
            
            echo "Finished running $QUERY_NAME on $DB_NAME with $WORKER_COUNT workers"
            echo "Results saved to: ${RESULTS_DIR}/results_${DB_NAME}_${QUERY_NAME}_${WORKER_COUNT}_workers.txt"
        done
    done
done
