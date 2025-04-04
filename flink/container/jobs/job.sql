SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '120s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '4';

-- Raw measurements table with original timestamps and device metrics
CREATE TABLE raw_measurements (
    measurement_timestamp TIMESTAMP(3),
    measurement_type STRING,
    raw_value STRING,
    device_id STRING,
    battery DOUBLE,
    signal_strength DOUBLE,
    ingestion_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'topic' = 'raw.measurements',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- #################################################################################

-- Enriched measurements with quality metrics and corrected types
CREATE TABLE enriched_measurements (
    measurement_type STRING,
    `value` DOUBLE,
    device_id STRING,
    patient_id STRING,
    
    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'topic' = 'enriched.measurements',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- Insert with quality and freshness calculations
INSERT INTO enriched_measurements
SELECT
    measurement_type,
    CAST(raw_value AS DOUBLE) AS `value`,
    device_id,
    REGEXP_EXTRACT(device_id, '.*_(P\d+)$', 1) AS patient_id,

    -- Quality components
    CAST((
        CASE
            WHEN device_id LIKE 'MEDICAL%' THEN 1.0
            WHEN device_id LIKE 'PREMIUM%' THEN 0.7
            ELSE 0.4
        END * 0.7 +
        CASE
            WHEN battery >= 80 THEN 1.0
            WHEN battery >= 50 THEN 0.8
            WHEN battery >= 20 THEN 0.6
            ELSE 0.4
        END * 0.2 +
        CASE
            WHEN signal_strength >= 0.8 THEN 1.0
            WHEN signal_strength >= 0.6 THEN 0.8
            WHEN signal_strength >= 0.4 THEN 0.6
            ELSE 0.4
        END * 0.1
    ) AS DECIMAL(7,2)) AS quality_weight,

    -- Combined freshness calculation
    CASE
        WHEN TIMESTAMPDIFF(HOUR, measurement_timestamp, ingestion_timestamp) <= 1 THEN 1.0
        WHEN TIMESTAMPDIFF(HOUR, measurement_timestamp, ingestion_timestamp) <= 6 THEN 0.9
        WHEN TIMESTAMPDIFF(HOUR, measurement_timestamp, ingestion_timestamp) <= 12 THEN 0.7
        WHEN TIMESTAMPDIFF(HOUR, measurement_timestamp, ingestion_timestamp) <= 24 THEN 0.5
        WHEN TIMESTAMPDIFF(HOUR, measurement_timestamp, ingestion_timestamp) <= 48 THEN 0.3
        ELSE 0.2
    END AS freshness_weight,
    
    -- Timestamps
    measurement_timestamp,
    ingestion_timestamp
FROM raw_measurements;

-- #################################################################################

-- Create tables for each measurement type
CREATE TABLE measurements_respiratory_rate (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    
    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.respiratory_rate',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE measurements_oxygen_saturation (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),

    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.oxygen_saturation',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE measurements_blood_pressure_systolic (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),

    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.blood_pressure_systolic',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE measurements_heart_rate (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),

    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.heart_rate',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE measurements_temperature (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),

    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.temperature',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE measurements_consciousness (
    device_id STRING,
    patient_id STRING,
    `value` DOUBLE,

    -- Weights
    quality_weight DOUBLE,
    freshness_weight DOUBLE,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP(3),
    ingestion_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),

    routing_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR measurement_timestamp AS measurement_timestamp - INTERVAL '10' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.consciousness',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- Insert for respiratory rate measurements
INSERT INTO measurements_respiratory_rate
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'RESPIRATORY_RATE';

-- Insert for oxygen saturation measurements
INSERT INTO measurements_oxygen_saturation
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'OXYGEN_SATURATION';

-- Insert for blood pressure measurements
INSERT INTO measurements_blood_pressure_systolic
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'BLOOD_PRESSURE_SYSTOLIC';

-- Insert for heart rate measurements
INSERT INTO measurements_heart_rate
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'HEART_RATE';

-- Insert for temperature measurements
INSERT INTO measurements_temperature
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'TEMPERATURE';

-- Insert for consciousness measurements
INSERT INTO measurements_consciousness
SELECT
    device_id,
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp
FROM enriched_measurements
WHERE measurement_type = 'CONSCIOUSNESS';

-- #################################################################################
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

-- Respiratory Rate Scoring
INSERT INTO scores_respiratory_rate
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,

    CASE
        WHEN `value` <= 8 THEN 3
        WHEN `value` >= 25 THEN 3
        WHEN `value` BETWEEN 21 AND 24 THEN 2
        WHEN `value` BETWEEN 9 AND 11 THEN 1
        WHEN `value` BETWEEN 12 AND 20 THEN 0
    END AS respiratory_rate_score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_respiratory_rate;

-- Oxygen Saturation Scoring
INSERT INTO scores_oxygen_saturation
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    CAST(CASE
        WHEN `value` <= 91 THEN 3
        WHEN `value` BETWEEN 92 AND 93 THEN 2
        WHEN `value` BETWEEN 94 AND 95 THEN 1
        WHEN `value` >= 96 THEN 0
    END AS INT) AS score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_oxygen_saturation;

-- Blood Pressure Scoring
INSERT INTO scores_blood_pressure_systolic
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    CAST(CASE
        WHEN `value` <= 90 THEN 3
        WHEN `value` <= 100 THEN 2
        WHEN `value` <= 110 THEN 1
        WHEN `value` BETWEEN 111 AND 219 THEN 0
        WHEN `value` >= 220 THEN 3
    END AS INT) AS score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_blood_pressure_systolic;

-- Heart Rate Scoring
INSERT INTO scores_heart_rate
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    CAST(CASE
        WHEN `value` <= 40 THEN 3
        WHEN `value` >= 131 THEN 3
        WHEN `value` BETWEEN 111 AND 130 THEN 2
        WHEN `value` BETWEEN 41 AND 50 THEN 1
        WHEN `value` BETWEEN 91 AND 110 THEN 1
        WHEN `value` BETWEEN 51 AND 90 THEN 0
    END AS INT) AS score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_heart_rate;

-- Temperature Scoring
INSERT INTO scores_temperature
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    CAST(CASE
        WHEN `value` <= 35.0 THEN 3
        WHEN `value` >= 39.1 THEN 2
        WHEN `value` BETWEEN 38.1 AND 39.0 THEN 1
        WHEN `value` BETWEEN 35.1 AND 36.0 THEN 1
        WHEN `value` BETWEEN 36.1 AND 38.0 THEN 0
    END AS INT) AS score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_temperature;

-- Consciousness Scoring
INSERT INTO scores_consciousness
SELECT
    patient_id,
    `value`,
    quality_weight,
    freshness_weight,
    CAST(CASE
        WHEN `value` = 1 THEN 0
        ELSE 3
    END AS INT) AS score,
    
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp
FROM measurements_consciousness;


-- #######################################

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
    respiratory_rate_score INT,
    oxygen_saturation_score INT,
    blood_pressure_score INT,
    heart_rate_score INT,
    temperature_score INT,
    consciousness_score INT,
    news2_score INT,

    -- Measurements statuses
    respiratory_rate_status STRING,
    oxygen_saturation_status STRING,
    blood_pressure_status STRING,
    heart_rate_status STRING,
    temperature_status STRING,
    consciousness_status STRING,

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

    flink_timestamp TIMESTAMP(3),
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