SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.local-time-zone' = 'UTC';
SET 'execution.checkpointing.interval' = '60000';
SET 'execution.checkpointing.timeout' = '30000';
SET 'state.backend' = 'hashmap';
SET 'table.exec.state.ttl' = '300000';
SET 'parallelism.default' = '6';

-- Create tables for measurement scores
CREATE TABLE scores_respiratory_rate (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.respiratory_rate',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE scores_oxygen_saturation (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.oxygen_saturation',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE scores_blood_pressure_systolic (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.blood_pressure_systolic',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE scores_heart_rate (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.heart_rate',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE scores_temperature (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.temperature',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE scores_consciousness (
    patient_id STRING,
    `value` DOUBLE,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,

    -- Scores
    score INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),

    scoring_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.consciousness',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

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
    union_timestamp TIMESTAMP(3),
    WATERMARK FOR union_timestamp AS union_timestamp - INTERVAL '10' SECONDS,
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO scores
SELECT * FROM (
    WITH unions as (
        SELECT * FROM
        (
            SELECT 
                'RESPIRATORY_RATE' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_respiratory_rate
            UNION ALL
            SELECT 
                'OXYGEN_SATURATION' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_oxygen_saturation
            UNION ALL
            SELECT 
                'BLOOD_PRESSURE_SYSTOLIC' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_blood_pressure_systolic
            UNION ALL
            SELECT
                'HEART_RATE' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_heart_rate
            UNION ALL
            SELECT 
                'TEMPERATURE' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_temperature
            UNION ALL
            SELECT 
                'CONSCIOUSNESS' as measurement_type, patient_id, `value`, score, 0.7 * quality_weight + 0.3 * freshness_weight as trust_score, 
                measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
            FROM scores_consciousness
        ) as unions
    )
    SELECT 
        patient_id,
        window_start,
        MAX(window_end) as window_end,

        MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' THEN `value` END) as respiratory_rate_value,
        MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN `value` END) as oxygen_saturation_value,
        MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' THEN `value` END) as blood_pressure_value,
        MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN `value` END) as heart_rate_value,
        MAX(CASE WHEN measurement_type = 'TEMPERATURE' THEN `value` END) as temperature_value,
        MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' THEN `value` END) as consciousness_value,

        COALESCE(MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' THEN score END), 0) as respiratory_rate_score,
        COALESCE(MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN score END), 0) as oxygen_saturation_score,
        COALESCE(MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' THEN score END), 0) as blood_pressure_score,
        COALESCE(MAX(CASE WHEN measurement_type = 'HEART_RATE' THEN score END), 0) as heart_rate_score,
        COALESCE(MAX(CASE WHEN measurement_type = 'TEMPERATURE' THEN score END), 0) as temperature_score,
        COALESCE(MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' THEN score END), 0) as consciousness_score,

        COALESCE(AVG(CASE WHEN measurement_type = 'RESPIRATORY_RATE' THEN trust_score END), 0) as respiratory_rate_trust_score,
        COALESCE(AVG(CASE WHEN measurement_type = 'OXYGEN_SATURATION' THEN trust_score END), 0) as oxygen_saturation_trust_score,
        COALESCE(AVG(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' THEN trust_score END), 0) as blood_pressure_trust_score,
        COALESCE(AVG(CASE WHEN measurement_type = 'HEART_RATE' THEN trust_score END), 0) as heart_rate_trust_score,
        COALESCE(AVG(CASE WHEN measurement_type = 'TEMPERATURE' THEN trust_score END), 0) as temperature_trust_score,
        COALESCE(AVG(CASE WHEN measurement_type = 'CONSCIOUSNESS' THEN trust_score END), 0) as consciousness_trust_score,

        MIN(measurement_timestamp) AS measurement_timestamp,
        MIN(ingestion_timestamp) AS ingestion_timestamp,
        MIN(enrichment_timestamp) AS enrichment_timestamp,
        MIN(routing_timestamp) AS routing_timestamp,
        MIN(scoring_timestamp) AS scoring_timestamp,
        CURRENT_TIMESTAMP as union_timestamp
    FROM TABLE(
        TUMBLE(
            TABLE unions, 
            DESCRIPTOR(measurement_timestamp), 
            INTERVAL '1' MINUTES
        )
    ) AS unions 
    GROUP BY patient_id, window_start
) as t;