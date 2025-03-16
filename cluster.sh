#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 [--start|--stop]"
    echo "Options:"
    echo "  --start    Start the cluster using start-cluster.sh"
    echo "  --stop     Stop the cluster using stop-cluster.sh"
    exit 1
}

# Check if no arguments were provided
if [ $# -eq 0 ]; then
    usage
fi

# Process command line arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --start)
            if [ -x "./scripts/start-cluster.sh" ]; then
                ./scripts/start-cluster.sh
            else
                echo "Error: start-cluster.sh not found or not executable"
                exit 1
            fi
            ;;
        --stop)
            if [ -x "./scripts/stop-cluster.sh" ]; then
                ./scripts/stop-cluster.sh
            else
                echo "Error: stop-cluster.sh not found or not executable"
                exit 1
            fi
            ;;
        --flink)
            if [ -x "./scripts/connect-flink.sh" ]; then
                ./scripts/connect-flink.sh
            else
                echo "Error: connect-flink.sh not found or not executable"
                exit 1
            fi
            ;;
        --doris)
            if [ -x "./scripts/connect-doris.sh" ]; then
                ./scripts/connect-doris.sh
            else
                echo "Error: connect-doris.sh not found or not executable"
                exit 1
            fi
            ;;
        *)
            echo "Invalid option: $1"
            usage
            ;;
    esac
    shift
done