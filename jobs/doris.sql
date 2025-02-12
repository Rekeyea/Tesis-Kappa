CREATE DATABASE kappa;

USE kappa;

CREATE TABLE IF NOT EXISTS gdnews2_scores (
    patient_id VARCHAR(36),  -- Changed from STRING to VARCHAR
    window_start DATETIME,
    window_end DATETIME,
    -- Raw measurements
    respiratory_rate_value DOUBLE,
    oxygen_saturation_value DOUBLE,
    blood_pressure_value DOUBLE,
    heart_rate_value DOUBLE,
    temperature_value DOUBLE,
    consciousness_value STRING,
    -- Raw NEWS2 scores
    respiratory_rate_score DOUBLE,
    oxygen_saturation_score DOUBLE,
    blood_pressure_score DOUBLE,
    heart_rate_score DOUBLE,
    temperature_score DOUBLE,
    consciousness_score DOUBLE,
    raw_news2_total DOUBLE,
    -- Adjusted gdNEWS2 scores
    adjusted_respiratory_rate_score DOUBLE,
    adjusted_oxygen_saturation_score DOUBLE,
    adjusted_blood_pressure_score DOUBLE,
    adjusted_heart_rate_score DOUBLE,
    adjusted_temperature_score DOUBLE,
    adjusted_consciousness_score DOUBLE,
    gdnews2_total DOUBLE,
    -- Quality and status
    overall_confidence DOUBLE,
    valid_parameters INT,
    degraded_parameters INT,
    invalid_parameters INT,
    -- Timestamps
    measurement_timestamp DATETIME,
    ingestion_timestamp DATETIME,
    enrichment_timestamp DATETIME,
    routing_timestamp DATETIME,
    scoring_timestamp DATETIME,
    aggregation_timestamp DATETIME,
    storage_timestamp DATETIME DEFAULT "__DORIS_CREATE_TIME__",
    updated_storage_timestamp DATETIME DEFAULT "__DORIS_CREATE_TIME__" UPDATE current_timestamp(),
)
UNIQUE KEY(patient_id, window_start, window_end)
DISTRIBUTED BY HASH(patient_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3"
);