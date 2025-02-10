SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '900s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';

-- Create ticker with proper timestamp and watermark
CREATE TABLE minute_ticker (
    ts TIMESTAMP(3),  -- Timestamp column
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- Define watermark
) WITH (
    'connector' = 'datagen',  -- Use datagen connector
    'rows-per-second' = '1'   -- Generate 1 row per second
);

-- Create sequence table
CREATE TABLE number_sequence (
    val INT
) WITH (
    'connector' = 'datagen',  -- Use datagen connector
    'fields.val.kind' = 'sequence',  -- Generate a sequence of numbers
    'fields.val.start' = '1',        -- Start from 1
    'fields.val.end' = '10'           -- End at 5
);

-- Raw measurements table with original timestamps and device metrics
CREATE TABLE raw_measurements (
    measurement_timestamp TIMESTAMP(9),
    measurement_type STRING,
    raw_value STRING,
    device_id STRING,
    battery DOUBLE,
    signal_strength DOUBLE,
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_timestamp TIMESTAMP(9),
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    device_id STRING,
    patient_id STRING,
    
    -- Quality components
    battery DOUBLE,
    signal_strength DOUBLE,
    device_quality DECIMAL(2, 1),
    measurement_conditions DECIMAL(2, 1),
    signal_quality DECIMAL(2, 1),
    quality_weight DECIMAL(7, 2),
    
    -- Freshness tracking
    measurement_age_minutes INT,
    freshness_weight DECIMAL(7, 2),
    
    -- Status tracking
    measurement_status VARCHAR(8),
    
    -- Processing timestamps
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_timestamp,
    measurement_type,
    raw_value,
    CASE
        WHEN measurement_type = 'CONSCIOUSNESS' THEN NULL
        ELSE CAST(raw_value AS DOUBLE)
    END AS numeric_value,
    device_id,
    REGEXP_EXTRACT(device_id, '.*_(P\d+)$', 1) AS patient_id,
    battery,
    signal_strength,
    -- Device quality
    CAST(CASE
        WHEN device_id LIKE 'MEDICAL%' THEN 0.9
        WHEN device_id LIKE 'CONSUMER%' THEN 0.7
        ELSE 0.8
    END AS DECIMAL(2,1)) AS device_quality,
    -- Measurement conditions
    CAST(CASE
        WHEN battery >= 80 THEN 1.0
        WHEN battery >= 50 THEN 0.8
        WHEN battery >= 20 THEN 0.6
        ELSE 0.4
    END AS DECIMAL(2,1)) AS measurement_conditions,
    -- Signal quality
    CAST(CASE
        WHEN signal_strength >= 0.8 THEN 1.0
        WHEN signal_strength >= 0.6 THEN 0.8
        WHEN signal_strength >= 0.4 THEN 0.6
        ELSE 0.4
    END AS DECIMAL(2,1)) AS signal_quality,
    -- Calculate combined quality weight
    CAST((
        CASE
            WHEN device_id LIKE 'MEDICAL%' THEN 0.9
            WHEN device_id LIKE 'CONSUMER%' THEN 0.7
            ELSE 0.8
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
    -- Calculate measurement age
    CAST(
        (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
         EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 60 
    AS INT) AS measurement_age_minutes,
    -- Calculate freshness weight
    CAST(CASE measurement_type
        WHEN 'CONSCIOUSNESS' THEN 
            GREATEST(0, 1 - ((EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                             EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / (24.0 * 3600)))
        WHEN 'RESPIRATORY_RATE' THEN 
            GREATEST(0, 1 - ((EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                             EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / (8.0 * 3600)))
        WHEN 'OXYGEN_SATURATION' THEN 
            GREATEST(0, 1 - ((EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                             EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / (8.0 * 3600)))
        WHEN 'HEART_RATE' THEN 
            GREATEST(0, 1 - ((EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                             EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / (8.0 * 3600)))
        ELSE 
            GREATEST(0, 1 - ((EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                             EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / (12.0 * 3600)))
    END AS DECIMAL(7,2)) AS freshness_weight,
    -- Calculate measurement status
    CASE
        WHEN (CASE
                WHEN device_id LIKE 'MEDICAL%' THEN 0.9
                WHEN device_id LIKE 'CONSUMER%' THEN 0.7
                ELSE 0.8
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
             END * 0.3) >= 0.8
             AND
             CASE measurement_type
                WHEN 'CONSCIOUSNESS' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 24
                WHEN 'RESPIRATORY_RATE' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                WHEN 'OXYGEN_SATURATION' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                WHEN 'HEART_RATE' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                ELSE 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 12
             END
        THEN 'VALID'
        WHEN (CASE
                WHEN device_id LIKE 'MEDICAL%' THEN 0.9
                WHEN device_id LIKE 'CONSUMER%' THEN 0.7
                ELSE 0.8
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
             END * 0.3) >= 0.5
             AND
             CASE measurement_type
                WHEN 'CONSCIOUSNESS' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 24
                WHEN 'RESPIRATORY_RATE' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                WHEN 'OXYGEN_SATURATION' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                WHEN 'HEART_RATE' THEN 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 8
                ELSE 
                    (EXTRACT(EPOCH FROM LOCALTIMESTAMP) - 
                     EXTRACT(EPOCH FROM CAST(measurement_timestamp AS TIMESTAMP))) / 3600 <= 12
             END
        THEN 'DEGRADED'
        ELSE 'INVALID'
    END AS measurement_status,
    kafka_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS enrichment_timestamp
FROM raw_measurements;

-- #################################################################################

-- Create tables for each measurement type
CREATE TABLE measurements_respiratory_rate (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    quality_weight DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'RESPIRATORY_RATE';

-- Insert for oxygen saturation measurements
INSERT INTO measurements_oxygen_saturation
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'OXYGEN_SATURATION';

-- Insert for blood pressure measurements
INSERT INTO measurements_blood_pressure_systolic
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'BLOOD_PRESSURE_SYSTOLIC';

-- Insert for heart rate measurements
INSERT INTO measurements_heart_rate
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'HEART_RATE';

-- Insert for temperature measurements
INSERT INTO measurements_temperature
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'TEMPERATURE';

-- Insert for consciousness measurements
INSERT INTO measurements_consciousness
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    quality_weight,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'CONSCIOUSNESS';

-- #################################################################################
-- Create tables for measurement scores
CREATE TABLE scores_respiratory_rate (
    patient_id STRING,
    measurement_type STRING,
    measured_value DOUBLE,
    measurement_avg DOUBLE,
    measurement_min DOUBLE,
    measurement_max DOUBLE,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    measured_value DOUBLE,
    measurement_avg DOUBLE,
    measurement_min DOUBLE,
    measurement_max DOUBLE,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    measured_value DOUBLE,
    measurement_avg DOUBLE,
    measurement_min DOUBLE,
    measurement_max DOUBLE,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    measured_value DOUBLE,
    measurement_avg DOUBLE,
    measurement_min DOUBLE,
    measurement_max DOUBLE,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    measured_value DOUBLE,
    measurement_avg DOUBLE,
    measurement_min DOUBLE,
    measurement_max DOUBLE,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type STRING,
    measured_value STRING,
    measurement_count INT,
    quality_weight DOUBLE,
    raw_news2_score DOUBLE,
    adjusted_score DOUBLE,
    confidence DOUBLE,
    freshness_weight DECIMAL(7,2),
    measurement_status STRING,
    measurement_timestamp TIMESTAMP(9),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
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
    measurement_type,
    numeric_value AS measured_value,
    AVG(numeric_value) OVER w AS measurement_avg,
    MIN(numeric_value) OVER w AS measurement_min,
    MAX(numeric_value) OVER w AS measurement_max,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN numeric_value <= 8 THEN 3
        WHEN numeric_value <= 11 THEN 1
        WHEN numeric_value <= 20 THEN 0
        WHEN numeric_value <= 24 THEN 2
        ELSE 3
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN numeric_value <= 8 THEN 3
            WHEN numeric_value <= 11 THEN 1
            WHEN numeric_value <= 20 THEN 0
            WHEN numeric_value <= 24 THEN 2
            ELSE 3
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_respiratory_rate
WINDOW w AS (
    PARTITION BY patient_id
    ORDER BY kafka_timestamp           -- Changed from measurement_timestamp to kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

-- Oxygen Saturation Scoring
INSERT INTO scores_oxygen_saturation
SELECT
    patient_id,
    measurement_type,
    numeric_value AS measured_value,
    AVG(numeric_value) OVER w AS measurement_avg,
    MIN(numeric_value) OVER w AS measurement_min,
    MAX(numeric_value) OVER w AS measurement_max,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN numeric_value <= 91 THEN 3
        WHEN numeric_value <= 93 THEN 2
        WHEN numeric_value <= 95 THEN 1
        ELSE 0
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN numeric_value <= 91 THEN 3
            WHEN numeric_value <= 93 THEN 2
            WHEN numeric_value <= 95 THEN 1
            ELSE 0
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_oxygen_saturation
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

-- Blood Pressure Scoring
INSERT INTO scores_blood_pressure_systolic
SELECT
    patient_id,
    measurement_type,
    numeric_value AS measured_value,
    AVG(numeric_value) OVER w AS measurement_avg,
    MIN(numeric_value) OVER w AS measurement_min,
    MAX(numeric_value) OVER w AS measurement_max,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN numeric_value <= 90 THEN 3
        WHEN numeric_value <= 100 THEN 2
        WHEN numeric_value <= 110 THEN 1
        WHEN numeric_value <= 219 THEN 0
        ELSE 3
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN numeric_value <= 90 THEN 3
            WHEN numeric_value <= 100 THEN 2
            WHEN numeric_value <= 110 THEN 1
            WHEN numeric_value <= 219 THEN 0
            ELSE 3
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_blood_pressure_systolic
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

-- Heart Rate Scoring
INSERT INTO scores_heart_rate
SELECT
    patient_id,
    measurement_type,
    numeric_value AS measured_value,
    AVG(numeric_value) OVER w AS measurement_avg,
    MIN(numeric_value) OVER w AS measurement_min,
    MAX(numeric_value) OVER w AS measurement_max,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN numeric_value <= 40 THEN 3
        WHEN numeric_value <= 50 THEN 1
        WHEN numeric_value <= 90 THEN 0
        WHEN numeric_value <= 110 THEN 1
        WHEN numeric_value <= 130 THEN 2
        ELSE 3
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN numeric_value <= 40 THEN 3
            WHEN numeric_value <= 50 THEN 1
            WHEN numeric_value <= 90 THEN 0
            WHEN numeric_value <= 110 THEN 1
            WHEN numeric_value <= 130 THEN 2
            ELSE 3
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_heart_rate
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

-- Temperature Scoring
INSERT INTO scores_temperature
SELECT
    patient_id,
    measurement_type,
    numeric_value AS measured_value,
    AVG(numeric_value) OVER w AS measurement_avg,
    MIN(numeric_value) OVER w AS measurement_min,
    MAX(numeric_value) OVER w AS measurement_max,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN numeric_value <= 35.0 THEN 3
        WHEN numeric_value <= 36.0 THEN 1
        WHEN numeric_value <= 38.0 THEN 0
        WHEN numeric_value <= 39.0 THEN 1
        ELSE 2
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN numeric_value <= 35.0 THEN 3
            WHEN numeric_value <= 36.0 THEN 1
            WHEN numeric_value <= 38.0 THEN 0
            WHEN numeric_value <= 39.0 THEN 1
            ELSE 2
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_temperature
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);

-- Consciousness Scoring
INSERT INTO scores_consciousness
SELECT
    patient_id,
    measurement_type,
    raw_value AS measured_value,
    CAST(COUNT(*) OVER w AS INT) AS measurement_count,
    quality_weight,
    CAST(CASE
        WHEN raw_value = 'A' THEN 0
        ELSE 3
    END AS DOUBLE) AS raw_news2_score,
    CASE
        WHEN measurement_status = 'INVALID' THEN 0
        ELSE CASE
            WHEN raw_value = 'A' THEN 0
            ELSE 3
        END * quality_weight * freshness_weight
    END AS adjusted_score,
    quality_weight AS confidence,
    freshness_weight,
    measurement_status,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS scoring_timestamp
FROM measurements_consciousness
WINDOW w AS (
    PARTITION BY patient_id 
    ORDER BY kafka_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);


-- #######################################

CREATE TABLE gdnews2_scores (
    patient_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
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
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS,
    PRIMARY KEY (patient_id, window_start, window_end) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gdnews2_scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'key.format' = 'json',
    'value.format' = 'json'
);


INSERT INTO gdnews2_scores
SELECT * 
FROM (
    WITH base_patients AS (
        -- Generate windowed patient IDs from datagen source (P1 to P10)
        SELECT 
            CONCAT('P', LPAD(CAST(n.val AS STRING), 4, '0')) AS patient_id,
            TUMBLE_START(t.ts, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(t.ts, INTERVAL '1' MINUTE) AS window_end
        FROM minute_ticker AS t
        CROSS JOIN number_sequence AS n
        GROUP BY
            n.val,
            TUMBLE(t.ts, INTERVAL '1' MINUTE)
    ),
    respiratory_rate_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            AVG(measured_value) as respiratory_rate_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_respiratory_rate, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    ),
    oxygen_saturation_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            AVG(measured_value) as oxygen_saturation_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_oxygen_saturation, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    ),
    blood_pressure_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            AVG(measured_value) as blood_pressure_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_blood_pressure_systolic, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    ),
    heart_rate_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            AVG(measured_value) as heart_rate_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_heart_rate, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    ),
    temperature_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            AVG(measured_value) as temperature_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_temperature, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    ),
    consciousness_window AS (
        SELECT 
            patient_id,
            window_start,
            window_end,
            MAX(measured_value) as consciousness_value,
            COUNT(*) as measurement_count,
            MAX(kafka_timestamp) as kafka_timestamp,
            MAX(measurement_status) as measurement_status,
            AVG(raw_news2_score) as raw_news2_score,
            AVG(adjusted_score) as adjusted_score,
            AVG(confidence) as confidence,
            MAX(measurement_timestamp) as measurement_timestamp,
            MAX(enrichment_timestamp) as enrichment_timestamp,
            MAX(routing_timestamp) as routing_timestamp,
            MAX(scoring_timestamp) as scoring_timestamp
        FROM TABLE(
            TUMBLE(TABLE scores_consciousness, DESCRIPTOR(kafka_timestamp), INTERVAL '1' MINUTE)
        )
        GROUP BY patient_id, window_start, window_end
    )
    
    SELECT 
        bp.patient_id,
        bp.window_start,
        bp.window_end,
        -- Raw measurements
        MAX(rr.respiratory_rate_value) as respiratory_rate_value,
        MAX(os.oxygen_saturation_value) as oxygen_saturation_value,
        MAX(bp_val.blood_pressure_value) as blood_pressure_value,
        MAX(hr.heart_rate_value) as heart_rate_value,
        MAX(temp.temperature_value) as temperature_value,
        MAX(cons.consciousness_value) as consciousness_value,
        
        -- Raw NEWS2 scores
        MAX(rr.raw_news2_score) as respiratory_rate_score,
        MAX(os.raw_news2_score) as oxygen_saturation_score,
        MAX(bp_val.raw_news2_score) as blood_pressure_score,
        MAX(hr.raw_news2_score) as heart_rate_score,
        MAX(temp.raw_news2_score) as temperature_score,
        MAX(cons.raw_news2_score) as consciousness_score,
        
        -- Calculate raw NEWS2 total
        (COALESCE(MAX(rr.raw_news2_score), 0) + 
        COALESCE(MAX(os.raw_news2_score), 0) + 
        COALESCE(MAX(bp_val.raw_news2_score), 0) + 
        COALESCE(MAX(hr.raw_news2_score), 0) + 
        COALESCE(MAX(temp.raw_news2_score), 0) + 
        COALESCE(MAX(cons.raw_news2_score), 0)) as raw_news2_total,
        
        -- Adjusted scores
        MAX(rr.adjusted_score) as adjusted_respiratory_rate_score,
        MAX(os.adjusted_score) as adjusted_oxygen_saturation_score,
        MAX(bp_val.adjusted_score) as adjusted_blood_pressure_score,
        MAX(hr.adjusted_score) as adjusted_heart_rate_score,
        MAX(temp.adjusted_score) as adjusted_temperature_score,
        MAX(cons.adjusted_score) as adjusted_consciousness_score,
        
        -- Calculate gdNEWS2 total
        (COALESCE(MAX(rr.adjusted_score), 0) + 
        COALESCE(MAX(os.adjusted_score), 0) + 
        COALESCE(MAX(bp_val.adjusted_score), 0) + 
        COALESCE(MAX(hr.adjusted_score), 0) + 
        COALESCE(MAX(temp.adjusted_score), 0) + 
        COALESCE(MAX(cons.adjusted_score), 0)) as gdnews2_total,
        
        -- Quality and confidence
        (COALESCE(MAX(rr.confidence), 0) + 
        COALESCE(MAX(os.confidence), 0) + 
        COALESCE(MAX(bp_val.confidence), 0) + 
        COALESCE(MAX(hr.confidence), 0) + 
        COALESCE(MAX(temp.confidence), 0) + 
        COALESCE(MAX(cons.confidence), 0)) / 6 as overall_confidence,
        
        -- Status counts
        (CASE WHEN MAX(rr.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'VALID' THEN 1 ELSE 0 END) as valid_parameters,
        
        (CASE WHEN MAX(rr.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'DEGRADED' THEN 1 ELSE 0 END) as degraded_parameters,
        
        (CASE WHEN MAX(rr.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(os.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(bp_val.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(hr.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(temp.measurement_status) = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN MAX(cons.measurement_status) = 'INVALID' THEN 1 ELSE 0 END) as invalid_parameters,
        
        -- Timestamps
        MAX(COALESCE(
            rr.measurement_timestamp,
            os.measurement_timestamp,
            bp_val.measurement_timestamp,
            hr.measurement_timestamp,
            temp.measurement_timestamp,
            cons.measurement_timestamp
        )) as measurement_timestamp,
        
        MAX(COALESCE(
            cons.kafka_timestamp,
            temp.kafka_timestamp,
            hr.kafka_timestamp,
            bp_val.kafka_timestamp,
            os.kafka_timestamp,
            rr.kafka_timestamp
        )) as kafka_timestamp,
        
        MAX(COALESCE(
            cons.enrichment_timestamp,
            temp.enrichment_timestamp,
            hr.enrichment_timestamp,
            bp_val.enrichment_timestamp,
            os.enrichment_timestamp,
            rr.enrichment_timestamp
        )) as enrichment_timestamp,
        
        MAX(COALESCE(
            cons.routing_timestamp,
            temp.routing_timestamp,
            hr.routing_timestamp,
            bp_val.routing_timestamp,
            os.routing_timestamp,
            rr.routing_timestamp
        )) as routing_timestamp,
        
        MAX(COALESCE(
            cons.scoring_timestamp,
            temp.scoring_timestamp,
            hr.scoring_timestamp,
            bp_val.scoring_timestamp,
            os.scoring_timestamp,
            rr.scoring_timestamp
        )) as scoring_timestamp,
        
        MAX(CAST(LOCALTIMESTAMP AS TIMESTAMP(3))) as aggregation_timestamp
    FROM base_patients bp
    LEFT JOIN respiratory_rate_window rr
        ON bp.patient_id = rr.patient_id
        AND bp.window_start = rr.window_start
        AND bp.window_end = rr.window_end
    LEFT JOIN oxygen_saturation_window os
        ON bp.patient_id = os.patient_id
        AND bp.window_start = os.window_start
        AND bp.window_end = os.window_end
    LEFT JOIN blood_pressure_window bp_val
        ON bp.patient_id = bp_val.patient_id
        AND bp.window_start = bp_val.window_start
        AND bp.window_end = bp_val.window_end
    LEFT JOIN heart_rate_window hr
        ON bp.patient_id = hr.patient_id
        AND bp.window_start = hr.window_start
        AND bp.window_end = hr.window_end
    LEFT JOIN temperature_window temp
        ON bp.patient_id = temp.patient_id
        AND bp.window_start = temp.window_start
        AND bp.window_end = temp.window_end
    LEFT JOIN consciousness_window cons
        ON bp.patient_id = cons.patient_id
        AND bp.window_start = cons.window_start
        AND bp.window_end = cons.window_end
    GROUP BY
        bp.patient_id, bp.window_start, bp.window_end
) v;


CREATE TABLE doris_gdnews2_scores (
    patient_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
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
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    PRIMARY KEY (patient_id, window_start, window_end) NOT ENFORCED
) WITH (
    'connector' = 'doris',
    'fenodes' = '172.20.4.2:8030',
    'table.identifier' = 'kappa.gdnews2_scores',  -- Writing to hot storage
    'username' = 'kappa',
    'password' = 'kappa',
    'sink.label-prefix' = 'doris_sink_gdnews2',
    'sink.properties.format' = 'json'
);

INSERT INTO doris_gdnews2_scores
SELECT *
FROM gdnews2_scores;