#!/bin/bash

# Configuration
HOST="localhost"
PORT="5432"
USER="postgres"
PASS="postgres"

# List of databases to execute queries on
DATABASES=(
    "iot_data_small"
    "iot_data_medium"
    "iot_data"
)

# Path to the Results directory (where the result files will be stored)
RESULTS_DIR="../Results"

# List of query files to execute (you can modify this as needed)
QUERY_FILES=(
    "../Queries/timescaledb-queries-last-loc"
    "../Queries/timescaledb-queries-long-driving-sessions"
    "../Queries/timescaledb-queries-avg-load"
    "../Queries/timescaledb-queries-daily-activity"
    "../Queries/timescaledb-queries-breakdown-frequency"
)

# List of worker counts to test
WORKERS=(1 4 8 16)

# Loop through each database
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

            # Connection string for PostgreSQL
            POSTGRES_CONN="host=$HOST port=$PORT user=$USER password=$PASS sslmode=disable"

            # The command to be run
            CMD="tsbs_run_queries_timescaledb --postgres=\"$POSTGRES_CONN\" --db-name=\"$DB_NAME\" --file=\"$QUERY_FILE\" --workers=\"$WORKER_COUNT\""

            # Output the command before running it
            echo "Executing: $CMD"
            
            # Run the command and log the output to the Results directory
            eval "$CMD >> ${RESULTS_DIR}/results_${DB_NAME}_${QUERY_NAME}_${WORKER_COUNT}_workers.txt"
            
            echo "Finished running $QUERY_NAME on $DB_NAME with $WORKER_COUNT workers"
            echo "Results saved to: ${RESULTS_DIR}/results_${DB_NAME}_${QUERY_NAME}_${WORKER_COUNT}_workers.txt"
        done
    done
done