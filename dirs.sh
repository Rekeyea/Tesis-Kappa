#!/bin/bash

# Create base directories
mkdir -p data logs

# Create Kafka directories
for i in {1..3}; do
    mkdir -p data/kafka-$i
    chown -R 10000:10000 data/kafka-$i
    chmod -R 750 data/kafka-$i
done

# Create ZooKeeper data and log directories
for i in {1..3}; do
    mkdir -p data/zookeeper-$i
    mkdir -p logs/zookeeper-$i
    chown -R 10000:10000 data/zookeeper-$i logs/zookeeper-$i
    chmod -R 750 data/zookeeper-$i logs/zookeeper-$i
done

# Print directory structure and permissions
echo "Directory structure created:"
ls -la data/
echo -e "\nZooKeeper logs:"
ls -la logs/