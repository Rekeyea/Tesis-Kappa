#!/bin/bash

# Create user and grant privileges
docker exec -i tesis-kappa-doris-fe-1 mysql -h 127.0.0.1 -P 9030 -u root << EOF
CREATE USER 'kappa'@'%' IDENTIFIED BY 'kappa';
GRANT ALL ON *.* TO 'kappa'@'%';

SET GLOBAL TIME_ZONE = 'UTC';
SET TIME_ZONE = 'UTC';

CREATE DATABASE kappa;

USE kappa;

CREATE TABLE IF NOT EXISTS gdnews2_scores (
    patient_id VARCHAR(36),
    window_start DATETIME,
    window_end DATETIME,

    -- AVG Raw measurements
    respiratory_rate_value DOUBLE,
    oxygen_saturation_value DOUBLE,
    blood_pressure_value DOUBLE,
    heart_rate_value DOUBLE,
    temperature_value DOUBLE,
    consciousness_value DOUBLE,

    -- Raw NEWS2 scores
    respiratory_rate_score DOUBLE,
    oxygen_saturation_score DOUBLE,
    blood_pressure_score DOUBLE,
    heart_rate_score DOUBLE,
    temperature_score DOUBLE,
    consciousness_score DOUBLE,
    news2_score DOUBLE,

    -- Trust gdNEWS2 scores
    respiratory_rate_trust_score DOUBLE,
    oxygen_saturation_trust_score DOUBLE,
    blood_pressure_trust_score DOUBLE,
    heart_rate_trust_score DOUBLE,
    temperature_trust_score DOUBLE,
    consciousness_trust_score DOUBLE,

    news2_trust_score DOUBLE,

    -- Timestamps
    measurement_timestamp DATETIME,
    ingestion_timestamp DATETIME,
    enrichment_timestamp DATETIME,
    routing_timestamp DATETIME,
    scoring_timestamp DATETIME,

    union_timestamp DATETIME,
    aggregation_timestamp DATETIME,
    storage_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(patient_id, window_start, window_end)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3"
);
EOF