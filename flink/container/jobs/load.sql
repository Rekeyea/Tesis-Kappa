SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '60000';
SET 'execution.checkpointing.timeout' = '30000';
SET 'state.backend' = 'hashmap';
SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '1';

CREATE TABLE gdnews2_scores (
    patient_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

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
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    union_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR aggregation_timestamp AS aggregation_timestamp - INTERVAL '10' SECONDS,
    
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gdnews2_scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'key.format' = 'json',
    'value.format' = 'json'
);

CREATE TABLE doris_gdnews2_scores (
    patient_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

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
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),

    union_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = '172.20.4.2:8030',
    'table.identifier' = 'kappa.gdnews2_scores',  -- Writing to hot storage
    'username' = 'kappa',
    'password' = 'kappa',
    'sink.label-prefix' = 'doris_sink_gdnews2',
    'sink.properties.format' = 'json',
    'sink.properties.timezone' = 'UTC'
);

INSERT INTO doris_gdnews2_scores
SELECT *
FROM gdnews2_scores;