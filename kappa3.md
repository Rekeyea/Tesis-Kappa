```sql

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

-- #################################################################################


-- ******** WORKING QUERY *************
-- Define a common table expression (CTE) for each scores table with a tumbling window
WITH respiratory_rate_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_respiratory_rate
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
oxygen_saturation_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_oxygen_saturation
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
blood_pressure_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_blood_pressure_systolic
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
heart_rate_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_heart_rate
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
temperature_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_temperature
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_avg,
        measurement_min,
        measurement_max,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
consciousness_window AS (
    SELECT
        patient_id,
        measurement_type,
        measured_value,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_consciousness
    GROUP BY
        patient_id,
        measurement_type,
        measured_value,
        measurement_count,
        quality_weight,
        raw_news2_score,
        adjusted_score,
        confidence,
        freshness_weight,
        measurement_status,
        measurement_timestamp,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
)

-- Join all tables within the same window
SELECT
    rr.patient_id,
    rr.window_start,
    rr.window_end,
    rr.measurement_type AS respiratory_rate_type,
    rr.measured_value AS respiratory_rate_value,
    rr.adjusted_score AS respiratory_rate_score,
    os.measurement_type AS oxygen_saturation_type,
    os.measured_value AS oxygen_saturation_value,
    os.adjusted_score AS oxygen_saturation_score,
    bp.measurement_type AS blood_pressure_type,
    bp.measured_value AS blood_pressure_value,
    bp.adjusted_score AS blood_pressure_score,
    hr.measurement_type AS heart_rate_type,
    hr.measured_value AS heart_rate_value,
    hr.adjusted_score AS heart_rate_score,
    temp.measurement_type AS temperature_type,
    temp.measured_value AS temperature_value,
    temp.adjusted_score AS temperature_score,
    cons.measurement_type AS consciousness_type,
    cons.measured_value AS consciousness_value,
    cons.adjusted_score AS consciousness_score
FROM respiratory_rate_window rr
JOIN oxygen_saturation_window os
    ON rr.patient_id = os.patient_id
    AND rr.window_start = os.window_start
    AND rr.window_end = os.window_end
JOIN blood_pressure_window bp
    ON rr.patient_id = bp.patient_id
    AND rr.window_start = bp.window_start
    AND rr.window_end = bp.window_end
JOIN heart_rate_window hr
    ON rr.patient_id = hr.patient_id
    AND rr.window_start = hr.window_start
    AND rr.window_end = hr.window_end
JOIN temperature_window temp
    ON rr.patient_id = temp.patient_id
    AND rr.window_start = temp.window_start
    AND rr.window_end = temp.window_end
JOIN consciousness_window cons
    ON rr.patient_id = cons.patient_id
    AND rr.window_start = cons.window_start
    AND rr.window_end = cons.window_end;

-- ************************************

-- Define a common table expression (CTE) for each scores table with a tumbling window
WITH respiratory_rate_window AS (
    SELECT
        patient_id,
        measured_value AS respiratory_rate_value,
        adjusted_score AS adjusted_respiratory_rate_score,
        raw_news2_score AS respiratory_rate_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_respiratory_rate
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
oxygen_saturation_window AS (
    SELECT
        patient_id,
        measured_value AS oxygen_saturation_value,
        adjusted_score AS adjusted_oxygen_saturation_score,
        raw_news2_score AS oxygen_saturation_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_oxygen_saturation
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
blood_pressure_window AS (
    SELECT
        patient_id,
        measured_value AS blood_pressure_value,
        adjusted_score AS adjusted_blood_pressure_score,
        raw_news2_score AS blood_pressure_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_blood_pressure_systolic
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
heart_rate_window AS (
    SELECT
        patient_id,
        measured_value AS heart_rate_value,
        adjusted_score AS adjusted_heart_rate_score,
        raw_news2_score AS heart_rate_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_heart_rate
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
temperature_window AS (
    SELECT
        patient_id,
        measured_value AS temperature_value,
        adjusted_score AS adjusted_temperature_score,
        raw_news2_score AS temperature_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_temperature
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
),
consciousness_window AS (
    SELECT
        patient_id,
        measured_value AS consciousness_value,
        adjusted_score AS adjusted_consciousness_score,
        raw_news2_score AS consciousness_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '5' MINUTE) AS window_end
    FROM scores_consciousness
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '5' MINUTE)
)

-- Main SELECT query to join all tables within the same window and calculate totals
SELECT
    rr.patient_id,
    -- Raw measurements
    rr.respiratory_rate_value,
    os.oxygen_saturation_value,
    bp.blood_pressure_value,
    hr.heart_rate_value,
    temp.temperature_value,
    cons.consciousness_value,
    -- Raw NEWS2 scores
    rr.respiratory_rate_score,
    os.oxygen_saturation_score,
    bp.blood_pressure_score,
    hr.heart_rate_score,
    temp.temperature_score,
    cons.consciousness_score,
    -- Raw NEWS2 total
    COALESCE(rr.respiratory_rate_score, 0) +
    COALESCE(os.oxygen_saturation_score, 0) +
    COALESCE(bp.blood_pressure_score, 0) +
    COALESCE(hr.heart_rate_score, 0) +
    COALESCE(temp.temperature_score, 0) +
    COALESCE(cons.consciousness_score, 0) AS raw_news2_total,
    -- Adjusted gdNEWS2 scores
    rr.adjusted_respiratory_rate_score,
    os.adjusted_oxygen_saturation_score,
    bp.adjusted_blood_pressure_score,
    hr.adjusted_heart_rate_score,
    temp.adjusted_temperature_score,
    cons.adjusted_consciousness_score,
    -- gdNEWS2 total
    COALESCE(rr.adjusted_respiratory_rate_score, 0) +
    COALESCE(os.adjusted_oxygen_saturation_score, 0) +
    COALESCE(bp.adjusted_blood_pressure_score, 0) +
    COALESCE(hr.adjusted_heart_rate_score, 0) +
    COALESCE(temp.adjusted_temperature_score, 0) +
    COALESCE(cons.adjusted_consciousness_score, 0) AS gdnews2_total,
    -- Quality and status
    (COALESCE(rr.confidence, 0) +
     COALESCE(os.confidence, 0) +
     COALESCE(bp.confidence, 0) +
     COALESCE(hr.confidence, 0) +
     COALESCE(temp.confidence, 0) +
     COALESCE(cons.confidence, 0)) / 6.0 AS overall_confidence,
    CAST(
        CASE WHEN rr.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'VALID' THEN 1 ELSE 0 END
    AS INT) AS valid_parameters,
    CAST(
        CASE WHEN rr.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END
    AS INT) AS degraded_parameters,
    CAST(
        CASE WHEN rr.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'INVALID' THEN 1 ELSE 0 END
    AS INT) AS invalid_parameters,
    -- Timestamps
    rr.kafka_timestamp AS measurement_timestamp,
    rr.kafka_timestamp,
    rr.enrichment_timestamp,
    rr.routing_timestamp,
    rr.scoring_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS aggregation_timestamp
FROM respiratory_rate_window rr
JOIN oxygen_saturation_window os
    ON rr.patient_id = os.patient_id
    AND rr.window_start = os.window_start
    AND rr.window_end = os.window_end
JOIN blood_pressure_window bp
    ON rr.patient_id = bp.patient_id
    AND rr.window_start = bp.window_start
    AND rr.window_end = bp.window_end
JOIN heart_rate_window hr
    ON rr.patient_id = hr.patient_id
    AND rr.window_start = hr.window_start
    AND rr.window_end = hr.window_end
JOIN temperature_window temp
    ON rr.patient_id = temp.patient_id
    AND rr.window_start = temp.window_start
    AND rr.window_end = temp.window_end
JOIN consciousness_window cons
    ON rr.patient_id = cons.patient_id
    AND rr.window_start = cons.window_start
    AND rr.window_end = cons.window_end;


CREATE TABLE gdnews2_scores (
    patient_id STRING,
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
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'gdnews2_scores',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- Insert the results into the gdnews2_scores table
INSERT INTO gdnews2_scores
SELECT * FROM (
    WITH respiratory_rate_window AS (
    SELECT
        patient_id,
        measured_value AS respiratory_rate_value,
        adjusted_score AS adjusted_respiratory_rate_score,
        raw_news2_score AS respiratory_rate_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_respiratory_rate
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
),
oxygen_saturation_window AS (
    SELECT
        patient_id,
        measured_value AS oxygen_saturation_value,
        adjusted_score AS adjusted_oxygen_saturation_score,
        raw_news2_score AS oxygen_saturation_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_oxygen_saturation
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
),
blood_pressure_window AS (
    SELECT
        patient_id,
        measured_value AS blood_pressure_value,
        adjusted_score AS adjusted_blood_pressure_score,
        raw_news2_score AS blood_pressure_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_blood_pressure_systolic
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
),
heart_rate_window AS (
    SELECT
        patient_id,
        measured_value AS heart_rate_value,
        adjusted_score AS adjusted_heart_rate_score,
        raw_news2_score AS heart_rate_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_heart_rate
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
),
temperature_window AS (
    SELECT
        patient_id,
        measured_value AS temperature_value,
        adjusted_score AS adjusted_temperature_score,
        raw_news2_score AS temperature_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_temperature
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
),
consciousness_window AS (
    SELECT
        patient_id,
        measured_value AS consciousness_value,
        adjusted_score AS adjusted_consciousness_score,
        raw_news2_score AS consciousness_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
        TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end
    FROM scores_consciousness
    GROUP BY
        patient_id,
        measured_value,
        adjusted_score,
        raw_news2_score,
        confidence,
        measurement_status,
        kafka_timestamp,
        enrichment_timestamp,
        routing_timestamp,
        scoring_timestamp,
        TUMBLE(kafka_timestamp, INTERVAL '1' MINUTE)
)

-- Main SELECT query to join all tables within the same window and calculate totals
SELECT
    rr.patient_id,
    -- Raw measurements
    rr.respiratory_rate_value,
    os.oxygen_saturation_value,
    bp.blood_pressure_value,
    hr.heart_rate_value,
    temp.temperature_value,
    cons.consciousness_value,
    -- Raw NEWS2 scores
    rr.respiratory_rate_score,
    os.oxygen_saturation_score,
    bp.blood_pressure_score,
    hr.heart_rate_score,
    temp.temperature_score,
    cons.consciousness_score,
    -- Raw NEWS2 total
    COALESCE(rr.respiratory_rate_score, 0) +
    COALESCE(os.oxygen_saturation_score, 0) +
    COALESCE(bp.blood_pressure_score, 0) +
    COALESCE(hr.heart_rate_score, 0) +
    COALESCE(temp.temperature_score, 0) +
    COALESCE(cons.consciousness_score, 0) AS raw_news2_total,
    -- Adjusted gdNEWS2 scores
    rr.adjusted_respiratory_rate_score,
    os.adjusted_oxygen_saturation_score,
    bp.adjusted_blood_pressure_score,
    hr.adjusted_heart_rate_score,
    temp.adjusted_temperature_score,
    cons.adjusted_consciousness_score,
    -- gdNEWS2 total
    COALESCE(rr.adjusted_respiratory_rate_score, 0) +
    COALESCE(os.adjusted_oxygen_saturation_score, 0) +
    COALESCE(bp.adjusted_blood_pressure_score, 0) +
    COALESCE(hr.adjusted_heart_rate_score, 0) +
    COALESCE(temp.adjusted_temperature_score, 0) +
    COALESCE(cons.adjusted_consciousness_score, 0) AS gdnews2_total,
    -- Quality and status
    (COALESCE(rr.confidence, 0) +
     COALESCE(os.confidence, 0) +
     COALESCE(bp.confidence, 0) +
     COALESCE(hr.confidence, 0) +
     COALESCE(temp.confidence, 0) +
     COALESCE(cons.confidence, 0)) / 6.0 AS overall_confidence,
    CAST(
        CASE WHEN rr.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'VALID' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'VALID' THEN 1 ELSE 0 END
    AS INT) AS valid_parameters,
    CAST(
        CASE WHEN rr.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'DEGRADED' THEN 1 ELSE 0 END
    AS INT) AS degraded_parameters,
    CAST(
        CASE WHEN rr.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN os.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN bp.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN hr.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN temp.measurement_status = 'INVALID' THEN 1 ELSE 0 END +
        CASE WHEN cons.measurement_status = 'INVALID' THEN 1 ELSE 0 END
    AS INT) AS invalid_parameters,
    -- Timestamps
    rr.kafka_timestamp AS measurement_timestamp,
    rr.kafka_timestamp,
    rr.enrichment_timestamp,
    rr.routing_timestamp,
    rr.scoring_timestamp,
    CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) AS aggregation_timestamp
FROM respiratory_rate_window rr
JOIN oxygen_saturation_window os
    ON rr.patient_id = os.patient_id
    AND rr.window_start = os.window_start
    AND rr.window_end = os.window_end
JOIN blood_pressure_window bp
    ON rr.patient_id = bp.patient_id
    AND rr.window_start = bp.window_start
    AND rr.window_end = bp.window_end
JOIN heart_rate_window hr
    ON rr.patient_id = hr.patient_id
    AND rr.window_start = hr.window_start
    AND rr.window_end = hr.window_end
JOIN temperature_window temp
    ON rr.patient_id = temp.patient_id
    AND rr.window_start = temp.window_start
    AND rr.window_end = temp.window_end
JOIN consciousness_window cons
    ON rr.patient_id = cons.patient_id
    AND rr.window_start = cons.window_start
    AND rr.window_end = cons.window_end
);

select patient_id, gdnews2_total, overall_confidence from gdnews2_scores;