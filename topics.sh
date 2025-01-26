#!/bin/bash

# Array of measurement types matching the Python VitalSign enum
MEASUREMENT_TYPES=(
    "RESPIRATORY_RATE"
    "OXYGEN_SATURATION"
    "BLOOD_PRESSURE_SYSTOLIC"
    "HEART_RATE"
    "TEMPERATURE"
    "CONSCIOUSNESS"
)

# Wait for Kafka UI to be ready
echo "Waiting for Kafka UI to be up..."
until $(curl --output /dev/null --silent --fail http://localhost:10150/api/clusters); do
    printf '.'
    sleep 5
done

# Function to create a topic
create_topic() {
    local topic_name=$1
    echo "Creating topic: $topic_name"
    curl -X POST http://localhost:10150/api/clusters/local/topics \
      -H 'Content-Type: application/json' \
      -d "{
        \"name\": \"$topic_name\",
        \"partitions\": 3,
        \"replicationFactor\": 3,
        \"configs\": {
          \"retention.ms\": \"604800000\",
          \"cleanup.policy\": \"delete\"
        }
      }"
    echo -e "\n"
}

# Create base topics
create_topic "raw.measurements"
create_topic "enriched.measurements"

# Create measurement type specific topics
for type in "${MEASUREMENT_TYPES[@]}"; do
    # Convert to lowercase for topic names
    type_lower=$(echo "$type" | tr '[:upper:]' '[:lower:]')
    create_topic "measurements.${type_lower}"
    create_topic "scores.${type_lower}"
done

# Create score aggregation topics
create_topic "all_measurement_scores"
create_topic "gdnews2_scores"

# List all topics
echo "Created topics:"
curl -s http://localhost:10150/api/clusters/local/topics | jq '.topics[].name'