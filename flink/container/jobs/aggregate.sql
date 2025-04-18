SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '60000';
SET 'execution.checkpointing.timeout' = '30000';
SET 'state.backend' = 'hashmap';
SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '6';

CREATE TABLE scores (
    patient_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),

    respiratory_rate_value DOUBLE,
    oxygen_saturation_value DOUBLE,
    blood_pressure_value DOUBLE,
    heart_rate_value DOUBLE,
    temperature_value DOUBLE,
    consciousness_value DOUBLE,

    respiratory_rate_score DOUBLE,
    oxygen_saturation_score DOUBLE,
    blood_pressure_score DOUBLE,
    heart_rate_score DOUBLE,
    temperature_score DOUBLE,
    consciousness_score DOUBLE,

    respiratory_rate_trust_score DOUBLE,
    oxygen_saturation_trust_score DOUBLE,
    blood_pressure_trust_score DOUBLE,
    heart_rate_trust_score DOUBLE,
    temperature_trust_score DOUBLE,
    consciousness_trust_score DOUBLE,

    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    union_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR union_timestamp AS union_timestamp - INTERVAL '10' SECONDS,
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'key.format' = 'json',
    'value.format' = 'json'
);

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
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS,
    
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gdnews2_scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO gdnews2_scores
SELECT
    patient_id,
    window_start,
    window_end,

    respiratory_rate_value,
    oxygen_saturation_value,
    blood_pressure_value,
    heart_rate_value,
    temperature_value,
    consciousness_value,

    respiratory_rate_score,
    oxygen_saturation_score,
    blood_pressure_score,
    heart_rate_score,
    temperature_score,
    consciousness_score,

    -- raw NEWS2 total
    (
        respiratory_rate_score +
        oxygen_saturation_score +
        blood_pressure_score +
        heart_rate_score +
        temperature_score +
        consciousness_score
    ) AS news2_score,

    -- adjusted scores
    respiratory_rate_trust_score,
    oxygen_saturation_trust_score,
    blood_pressure_trust_score,
    heart_rate_trust_score,
    temperature_trust_score,
    consciousness_trust_score,

    (
        respiratory_rate_trust_score +
        oxygen_saturation_trust_score +
        blood_pressure_trust_score +
        heart_rate_trust_score +
        temperature_trust_score +
        consciousness_trust_score
    ) AS news2_trust_score,
    
    -- use aggregated timestamps (make sure these come after the parameters)
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    scoring_timestamp,
    union_timestamp
FROM scores;