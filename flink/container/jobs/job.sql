SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '1 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '900s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'table.local-time-zone' = 'UTC';

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
        END * 0.4 +
        CASE
            WHEN battery >= 80 THEN 1.0
            WHEN battery >= 50 THEN 0.8
            WHEN battery >= 20 THEN 0.6
            ELSE 0.4
        END * 0.3 +
        CASE
            WHEN signal_strength >= 0.8 THEN 1.0
            WHEN signal_strength >= 0.6 THEN 0.8
            WHEN signal_strength >= 0.4 THEN 0.6
            ELSE 0.4
        END * 0.3
    ) AS DECIMAL(7,2)) AS quality_weight,

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
FROM measurements_respiratory_rate
WINDOW w AS (
    PARTITION BY patient_id
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

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
FROM measurements_oxygen_saturation
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

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
FROM measurements_blood_pressure_systolic
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

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
FROM measurements_heart_rate
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

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
FROM measurements_temperature
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

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
FROM measurements_consciousness
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY measurement_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);


-- #######################################

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

    flink_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR aggregation_timestamp AS aggregation_timestamp - INTERVAL '10' SECONDS,
    
    PRIMARY KEY (patient_id, window_start, window_end) NOT ENFORCED
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
    respiratory_rate_score +
    oxygen_saturation_score +
    blood_pressure_score +
    heart_rate_score +
    temperature_score +
    consciousness_score AS news2_score,

    -- Statuses
    CAST(CASE 
        WHEN 0.7 * respiratory_rate_quality_weight + 0.3 *respiratory_rate_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * respiratory_rate_quality_weight + 0.3 *respiratory_rate_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END 
    AS STRING) as respiratory_rate_status,
    CAST(CASE 
        WHEN 0.7 * oxygen_saturation_quality_weight + 0.3 *oxygen_saturation_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * oxygen_saturation_quality_weight + 0.3 *oxygen_saturation_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END
    AS STRING) as oxygen_saturation_status,
    CAST(CASE 
        WHEN 0.7 * blood_pressure_quality_weight + 0.3 *blood_pressure_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * blood_pressure_quality_weight + 0.3 *blood_pressure_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END
    AS STRING) as blood_pressure_status,
    CAST(CASE 
        WHEN 0.7 * heart_rate_quality_weight + 0.3 *heart_rate_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * heart_rate_quality_weight + 0.3 *heart_rate_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END
    AS STRING) as heart_rate_status,
    CAST(CASE 
        WHEN 0.7 * temperature_quality_weight + 0.3 *temperature_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * temperature_quality_weight + 0.3 *temperature_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END
    AS STRING) as temperature_status,
    CAST(CASE 
        WHEN 0.7 * consciousness_quality_weight + 0.3 *consciousness_freshness_weight > 0.75 THEN 'VALID'
        WHEN 0.7 * consciousness_quality_weight + 0.3 *consciousness_freshness_weight > 0.5 THEN 'DEGRADED'
        ELSE 'INVALID'
    END
    AS STRING) as consciousness_status,

    -- adjusted scores
    0.7 * respiratory_rate_quality_weight + 0.3 *respiratory_rate_freshness_weight AS respiratory_rate_trust_score,
    0.7 * oxygen_saturation_quality_weight + 0.3 *oxygen_saturation_freshness_weight AS oxygen_saturation_trust_score,
    0.7 * blood_pressure_quality_weight + 0.3 *blood_pressure_freshness_weight AS blood_pressure_trust_score,
    0.7 * heart_rate_quality_weight + 0.3 *heart_rate_freshness_weight AS heart_rate_trust_score,
    0.7 * temperature_quality_weight + 0.3 *temperature_freshness_weight AS temperature_trust_score,
    0.7 * consciousness_quality_weight + 0.3 *consciousness_freshness_weight AS consciousness_trust_score,

    (
        0.7 * respiratory_rate_quality_weight + 0.3 *respiratory_rate_freshness_weight +
        0.7 * oxygen_saturation_quality_weight + 0.3 *oxygen_saturation_freshness_weight +
        0.7 * blood_pressure_quality_weight + 0.3 *blood_pressure_freshness_weight +
        0.7 * heart_rate_quality_weight + 0.3 *heart_rate_freshness_weight +
        0.7 * temperature_quality_weight + 0.3 *temperature_freshness_weight +
        0.7 * consciousness_quality_weight + 0.3 *consciousness_freshness_weight
    ) / 6 AS news2_trust_score,
    
    -- use aggregated timestamps (make sure these come after the parameters)
    measurement_timestamp,
    ingestion_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    scoring_timestamp,
    CURRENT_TIMESTAMP AS flink_timestamp
FROM (
    WITH scores AS (
        SELECT 
            `value` as rr_value, CAST(NULL AS INT) as os_value, CAST(NULL AS INT) as bp_value, CAST(NULL AS INT) as hr_value, CAST(NULL AS INT) as tp_value, CAST(NULL AS INT) as cs_value,
            score as rr_score, CAST(NULL AS INT) as os_score, CAST(NULL AS INT) as bp_score, CAST(NULL AS INT) as hr_score, CAST(NULL AS INT) as tp_score, CAST(NULL AS INT) as cs_score,
            quality_weight as rr_quality_weight, CAST(NULL AS INT) as os_quality_weight, CAST(NULL AS INT) as bp_quality_weight, CAST(NULL AS INT) as hr_quality_weight, CAST(NULL AS INT) as tp_quality_weight, CAST(NULL AS INT) as cs_quality_weight,
            freshness_weight as rr_freshness_weight, CAST(NULL AS INT) as os_freshness_weight, CAST(NULL AS INT) as bp_freshness_weight, CAST(NULL AS INT) as hr_freshness_weight, CAST(NULL AS INT) as tp_freshness_weight, CAST(NULL AS INT) as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp, scoring_timestamp
        FROM scores_respiratory_rate
        UNION ALL
        SELECT 
            CAST(NULL AS INT) as rr_value, `value` as os_value, CAST(NULL AS INT) as bp_value, CAST(NULL AS INT) as hr_value, CAST(NULL AS INT) as tp_value, CAST(NULL AS INT) as cs_value,
            CAST(NULL AS INT) as rr_score, score as os_score, CAST(NULL AS INT) as bp_score, CAST(NULL AS INT) as hr_score, CAST(NULL AS INT) as tp_score, CAST(NULL AS INT) as cs_score,
            CAST(NULL AS INT) as rr_quality_weight, quality_weight as os_quality_weight, CAST(NULL AS INT) as bp_quality_weight, CAST(NULL AS INT) as hr_quality_weight, CAST(NULL AS INT) as tp_quality_weight, CAST(NULL AS INT) as cs_quality_weight,
            CAST(NULL AS INT) as rr_freshness_weight, freshness_weight as os_freshness_weight, CAST(NULL AS INT) as bp_freshness_weight, CAST(NULL AS INT) as hr_freshness_weight, CAST(NULL AS INT) as tp_freshness_weight, CAST(NULL AS INT) as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp , scoring_timestamp
        FROM scores_oxygen_saturation
        UNION ALL
        SELECT 
            CAST(NULL AS INT) as rr_value, CAST(NULL AS INT) as os_value, `value` as bp_value, CAST(NULL AS INT) as hr_value, CAST(NULL AS INT) as tp_value, CAST(NULL AS INT) as cs_value,
            CAST(NULL AS INT) as rr_score, CAST(NULL AS INT) as os_score, score as bp_score, CAST(NULL AS INT) as hr_score, CAST(NULL AS INT) as tp_score, CAST(NULL AS INT) as cs_score,
            CAST(NULL AS INT) as rr_quality_weight, CAST(NULL AS INT) as os_quality_weight, quality_weight as bp_quality_weight, CAST(NULL AS INT) as hr_quality_weight, CAST(NULL AS INT) as tp_quality_weight, CAST(NULL AS INT) as cs_quality_weight,
            CAST(NULL AS INT) as rr_freshness_weight, CAST(NULL AS INT) as os_freshness_weight, freshness_weight as bp_freshness_weight, CAST(NULL AS INT) as hr_freshness_weight, CAST(NULL AS INT) as tp_freshness_weight, CAST(NULL AS INT) as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp , scoring_timestamp
        FROM scores_blood_pressure_systolic
        UNION ALL
        SELECT
            CAST(NULL AS INT) as rr_value, CAST(NULL AS INT) as os_value, CAST(NULL AS INT) as bp_value, `value` as hr_value, CAST(NULL AS INT) as tp_value, CAST(NULL AS INT) as cs_value,
            CAST(NULL AS INT) as rr_score, CAST(NULL AS INT) as os_score, CAST(NULL AS INT) as bp_score, score as hr_score, CAST(NULL AS INT) as tp_score, CAST(NULL AS INT) as cs_score,
            CAST(NULL AS INT) as rr_quality_weight, CAST(NULL AS INT) as os_quality_weight, CAST(NULL AS INT) as bp_quality_weight, quality_weight as hr_quality_weight, CAST(NULL AS INT) as tp_quality_weight, CAST(NULL AS INT) as cs_quality_weight,
            CAST(NULL AS INT) as rr_freshness_weight, CAST(NULL AS INT) as os_freshness_weight, CAST(NULL AS INT) as bp_freshness_weight, freshness_weight as hr_freshness_weight, CAST(NULL AS INT) as tp_freshness_weight, CAST(NULL AS INT) as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp , scoring_timestamp
        FROM scores_heart_rate
        UNION ALL
        SELECT 
            CAST(NULL AS INT) as rr_value, CAST(NULL AS INT) as os_value, CAST(NULL AS INT) as bp_value, CAST(NULL AS INT) as hr_value, `value` as tp_value, CAST(NULL AS INT) as cs_value,
            CAST(NULL AS INT) as rr_score, CAST(NULL AS INT) as os_score, CAST(NULL AS INT) as bp_score, CAST(NULL AS INT) as hr_score, score as tp_score, CAST(NULL AS INT) as cs_score,
            CAST(NULL AS INT) as rr_quality_weight, CAST(NULL AS INT) as os_quality_weight, CAST(NULL AS INT) as bp_quality_weight, CAST(NULL AS INT) as hr_quality_weight, quality_weight as tp_quality_weight, CAST(NULL AS INT) as cs_quality_weight,
            CAST(NULL AS INT) as rr_freshness_weight, CAST(NULL AS INT) as os_freshness_weight, CAST(NULL AS INT) as bp_freshness_weight, CAST(NULL AS INT) as hr_freshness_weight, freshness_weight as tp_freshness_weight, CAST(NULL AS INT) as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp , scoring_timestamp
        FROM scores_temperature
        UNION ALL
        SELECT 
            CAST(NULL AS INT) as rr_value, CAST(NULL AS INT) as os_value, CAST(NULL AS INT) as bp_value, CAST(NULL AS INT) as hr_value, CAST(NULL AS INT) as tp_value, `value` as cs_value,
            CAST(NULL AS INT) as rr_score, CAST(NULL AS INT) as os_score, CAST(NULL AS INT) as bp_score, CAST(NULL AS INT) as hr_score, CAST(NULL AS INT) as tp_score, score as cs_score,
            CAST(NULL AS INT) as rr_quality_weight, CAST(NULL AS INT) as os_quality_weight, CAST(NULL AS INT) as bp_quality_weight, CAST(NULL AS INT) as hr_quality_weight, CAST(NULL AS INT) as tp_quality_weight, quality_weight as cs_quality_weight,
            CAST(NULL AS INT) as rr_freshness_weight, CAST(NULL AS INT) as os_freshness_weight, CAST(NULL AS INT) as bp_freshness_weight, CAST(NULL AS INT) as hr_freshness_weight, CAST(NULL AS INT) as tp_freshness_weight, freshness_weight as cs_freshness_weight,
            patient_id, measurement_timestamp, ingestion_timestamp, enrichment_timestamp, routing_timestamp , scoring_timestamp
        FROM scores_consciousness
    )
    SELECT
        patient_id,
        window_start,
        MAX(window_end) as window_end,
        COALESCE(AVG(rr_value), 0) AS respiratory_rate_value,
        COALESCE(AVG(os_value), 0) AS oxygen_saturation_value,
        COALESCE(AVG(bp_value), 0) AS blood_pressure_value,
        COALESCE(AVG(hr_value), 0) AS heart_rate_value,
        COALESCE(AVG(tp_value), 0) AS temperature_value,
        COALESCE(AVG(cs_value), 0) AS consciousness_value,

        COALESCE(AVG(rr_score), 0) AS respiratory_rate_score,
        COALESCE(AVG(os_score), 0) AS oxygen_saturation_score,
        COALESCE(AVG(bp_score), 0) AS blood_pressure_score,
        COALESCE(AVG(hr_score), 0) AS heart_rate_score,
        COALESCE(AVG(tp_score), 0) AS temperature_score,
        COALESCE(AVG(cs_score), 0) AS consciousness_score, 

        COALESCE(AVG(rr_quality_weight), 0.2) AS respiratory_rate_quality_weight,
        COALESCE(AVG(os_quality_weight), 0.2) AS oxygen_saturation_quality_weight,
        COALESCE(AVG(bp_quality_weight), 0.2) AS blood_pressure_quality_weight,
        COALESCE(AVG(hr_quality_weight), 0.2) AS heart_rate_quality_weight,
        COALESCE(AVG(tp_quality_weight), 0.2) AS temperature_quality_weight,
        COALESCE(AVG(cs_quality_weight), 0.2) AS consciousness_quality_weight,


        COALESCE(AVG(rr_freshness_weight), 0.2) AS respiratory_rate_freshness_weight,
        COALESCE(AVG(os_freshness_weight), 0.2) AS oxygen_saturation_freshness_weight,
        COALESCE(AVG(bp_freshness_weight), 0.2) AS blood_pressure_freshness_weight,
        COALESCE(AVG(hr_freshness_weight), 0.2) AS heart_rate_freshness_weight,
        COALESCE(AVG(tp_freshness_weight), 0.2) AS temperature_freshness_weight,
        COALESCE(AVG(cs_freshness_weight), 0.2) AS consciousness_freshness_weight,

        MIN(measurement_timestamp) AS measurement_timestamp,
        MIN(ingestion_timestamp) AS ingestion_timestamp,
        MIN(enrichment_timestamp) AS enrichment_timestamp,
        MIN(routing_timestamp) AS routing_timestamp,
        MIN(scoring_timestamp) AS scoring_timestamp
    FROM TABLE(
        TUMBLE(
            TABLE scores, 
            DESCRIPTOR(measurement_timestamp), 
            INTERVAL '1' MINUTES
        )
    ) AS gdnews2
    GROUP BY patient_id, window_start
) AS gdnews2;


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

    flink_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    PRIMARY KEY (patient_id, window_start, window_end) NOT ENFORCED
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