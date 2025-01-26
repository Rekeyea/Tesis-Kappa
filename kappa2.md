Se necesitan crear los topics de Kafka


```sql

-- Raw measurements table with original timestamps
CREATE TABLE raw_measurements (
    measurement_timestamp TIMESTAMP(9),
    measurement_type STRING,
    raw_value STRING,
    device_id STRING,
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

-- Enriched measurements with processing timestamp
CREATE TABLE enriched_measurements (
    measurement_timestamp TIMESTAMP(3),
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    device_id STRING,
    patient_id STRING,  -- Added patient_id field
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
    REGEXP_EXTRACT(device_id, '.*_(P\d+)$', 1) AS patient_id,  -- Extract patient_id using regex
    kafka_timestamp,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS enrichment_timestamp
FROM raw_measurements;

-- ##########################################################
-- Heart Rate
CREATE TABLE measurements_heart_rate (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.heart_rate',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- Insert for heart rate measurements
INSERT INTO measurements_heart_rate
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'HEART_RATE';

-- Respiratory Rate
CREATE TABLE measurements_respiratory_rate (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.respiratory_rate',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO measurements_respiratory_rate
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'RESPIRATORY_RATE';

-- Oxygen Saturation
CREATE TABLE measurements_oxygen_saturation (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.oxygen_saturation',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO measurements_oxygen_saturation
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'OXYGEN_SATURATION';

-- Blood Pressure Systolic
CREATE TABLE measurements_blood_pressure_systolic (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.blood_pressure_systolic',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO measurements_blood_pressure_systolic
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'BLOOD_PRESSURE_SYSTOLIC';


-- Temperature
CREATE TABLE measurements_temperature (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.temperature',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO measurements_temperature
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'TEMPERATURE';

-- Consciousness
CREATE TABLE measurements_consciousness (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    raw_value STRING,
    numeric_value DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'measurements.consciousness',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO measurements_consciousness
SELECT
    device_id,
    patient_id,
    measurement_type,
    raw_value,
    numeric_value,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    CURRENT_TIMESTAMP AS routing_timestamp
FROM enriched_measurements
WHERE measurement_type = 'CONSCIOUSNESS';
-- ########################################################################################

-- ########################################################################################

-- Respiratory Rate Scores
CREATE TABLE scores_respiratory_rate (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.respiratory_rate',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_respiratory_rate
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN numeric_value <= 8 THEN 3
        WHEN numeric_value <= 11 THEN 1
        WHEN numeric_value <= 20 THEN 0
        WHEN numeric_value <= 24 THEN 2
        ELSE 3
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (8 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_respiratory_rate;

-- Oxygen Saturation Scores
CREATE TABLE scores_oxygen_saturation (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.oxygen_saturation',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_oxygen_saturation
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN numeric_value <= 91 THEN 3
        WHEN numeric_value <= 93 THEN 2
        WHEN numeric_value <= 95 THEN 1
        ELSE 0
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (8 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_oxygen_saturation;

-- Heart Rate Scores
CREATE TABLE scores_heart_rate (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.heart_rate',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_heart_rate
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN numeric_value <= 40 THEN 3
        WHEN numeric_value <= 50 THEN 1
        WHEN numeric_value <= 90 THEN 0
        WHEN numeric_value <= 110 THEN 1
        WHEN numeric_value <= 130 THEN 2
        ELSE 3
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (8 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_heart_rate;

-- Systolic Blood Pressure Scores
CREATE TABLE scores_blood_pressure_systolic (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.blood_pressure_systolic',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_blood_pressure_systolic
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN numeric_value <= 90 THEN 3
        WHEN numeric_value <= 100 THEN 2
        WHEN numeric_value <= 110 THEN 1
        WHEN numeric_value <= 219 THEN 0
        ELSE 3
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (12 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_blood_pressure_systolic;

-- Temperature Scores
CREATE TABLE scores_temperature (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.temperature',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_temperature
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN numeric_value <= 35.0 THEN 3
        WHEN numeric_value <= 36.0 THEN 1
        WHEN numeric_value <= 38.0 THEN 0
        WHEN numeric_value <= 39.0 THEN 1
        ELSE 2
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (12 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_temperature;

-- Consciousness Scores
CREATE TABLE scores_consciousness (
    device_id STRING,
    patient_id STRING,
    measurement_type STRING,
    score DOUBLE,
    confidence DOUBLE,
    freshness_weight DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'scores.consciousness',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO scores_consciousness
SELECT
    device_id,
    patient_id,
    measurement_type,
    CASE
        WHEN raw_value = 'A' THEN 0
        ELSE 3
    END AS score,
    1.0 AS confidence,
    GREATEST(0, 1 - (TIMESTAMPDIFF(SECOND, measurement_timestamp, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))) / (24 * 3600))) AS freshness_weight,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    CURRENT_TIMESTAMP AS scoring_timestamp
FROM measurements_consciousness;
-- ########################################################################################


-- ########################################################################################
-- Create table for final aggregated scores
CREATE TABLE all_measurement_scores (
    patient_id STRING,
    respiratory_rate_score DOUBLE,
    oxygen_saturation_score DOUBLE,
    blood_pressure_score DOUBLE,
    heart_rate_score DOUBLE,
    temperature_score DOUBLE,
    consciousness_score DOUBLE,
    total_confidence DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    WATERMARK FOR kafka_timestamp AS kafka_timestamp - INTERVAL '5' SECONDS
) WITH (
    'topic' = 'all_measurement_scores',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- Create view for windowed aggregation
CREATE VIEW windowed_measurements AS
SELECT
    patient_id,
    TUMBLE_START(kafka_timestamp, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(kafka_timestamp, INTERVAL '1' MINUTE) AS window_end,
    -- Respiratory rate
    MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' 
        THEN score * freshness_weight END) AS respiratory_rate_score,
    MAX(CASE WHEN measurement_type = 'RESPIRATORY_RATE' 
        THEN confidence END) AS respiratory_confidence,
    -- Oxygen saturation
    MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' 
        THEN score * freshness_weight END) AS oxygen_saturation_score,
    MAX(CASE WHEN measurement_type = 'OXYGEN_SATURATION' 
        THEN confidence END) AS oxygen_confidence,
    -- Blood pressure
    MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' 
        THEN score * freshness_weight END) AS blood_pressure_score,
    MAX(CASE WHEN measurement_type = 'BLOOD_PRESSURE_SYSTOLIC' 
        THEN confidence END) AS blood_pressure_confidence,
    -- Heart rate
    MAX(CASE WHEN measurement_type = 'HEART_RATE' 
        THEN score * freshness_weight END) AS heart_rate_score,
    MAX(CASE WHEN measurement_type = 'HEART_RATE' 
        THEN confidence END) AS heart_rate_confidence,
    -- Temperature
    MAX(CASE WHEN measurement_type = 'TEMPERATURE' 
        THEN score * freshness_weight END) AS temperature_score,
    MAX(CASE WHEN measurement_type = 'TEMPERATURE' 
        THEN confidence END) AS temperature_confidence,
    -- Consciousness
    MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' 
        THEN score * freshness_weight END) AS consciousness_score,
    MAX(CASE WHEN measurement_type = 'CONSCIOUSNESS' 
        THEN confidence END) AS consciousness_confidence,
    -- Timestamps
    MAX(measurement_timestamp) AS measurement_timestamp,
    MAX(kafka_timestamp) AS kafka_timestamp,
    MAX(enrichment_timestamp) AS enrichment_timestamp,
    MAX(routing_timestamp) AS routing_timestamp,
    MAX(scoring_timestamp) AS scoring_timestamp
FROM (
    SELECT * FROM scores_respiratory_rate
    UNION ALL
    SELECT * FROM scores_oxygen_saturation
    UNION ALL
    SELECT * FROM scores_blood_pressure_systolic
    UNION ALL
    SELECT * FROM scores_heart_rate
    UNION ALL
    SELECT * FROM scores_temperature
    UNION ALL
    SELECT * FROM scores_consciousness
)
GROUP BY 
    patient_id,
    TUMBLE(kafka_timestamp, INTERVAL '10' SECOND);

-- Insert aggregated scores
INSERT INTO all_measurement_scores
SELECT
    patient_id,
    respiratory_rate_score,
    oxygen_saturation_score,
    blood_pressure_score,
    heart_rate_score,
    temperature_score,
    consciousness_score,
    (COALESCE(respiratory_confidence, 0) +
     COALESCE(oxygen_confidence, 0) +
     COALESCE(blood_pressure_confidence, 0) +
     COALESCE(heart_rate_confidence, 0) +
     COALESCE(temperature_confidence, 0) +
     COALESCE(consciousness_confidence, 0)) /
    NULLIF(
        (CASE WHEN respiratory_confidence IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN oxygen_confidence IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN blood_pressure_confidence IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN heart_rate_confidence IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN temperature_confidence IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN consciousness_confidence IS NOT NULL THEN 1 ELSE 0 END),
        0
    ) AS total_confidence,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    scoring_timestamp,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS aggregation_timestamp,
    window_start,
    window_end
FROM windowed_measurements;

-- ########################################################################################

-- Final gdNEWS2 scores with all processing timestamps
CREATE TABLE gdnews2_scores (
    patient_id STRING,
    total_score DOUBLE,
    confidence DOUBLE,
    measurement_timestamp TIMESTAMP(3),
    kafka_timestamp TIMESTAMP(3),
    enrichment_timestamp TIMESTAMP(3),
    routing_timestamp TIMESTAMP(3),
    scoring_timestamp TIMESTAMP(3),
    aggregation_timestamp TIMESTAMP(3),
    final_timestamp TIMESTAMP(3)
) WITH (
    'topic' = 'gdnews2_scores',
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

INSERT INTO gdnews2_scores
SELECT 
    patient_id,
    COALESCE(respiratory_rate_score, 0) +
    COALESCE(oxygen_saturation_score, 0) +
    COALESCE(blood_pressure_score, 0) +
    COALESCE(heart_rate_score, 0) +
    COALESCE(temperature_score, 0) +
    COALESCE(consciousness_score, 0) AS total_score,
    total_confidence AS confidence,
    measurement_timestamp,
    kafka_timestamp,
    enrichment_timestamp,
    routing_timestamp,
    scoring_timestamp,
    aggregation_timestamp,
    CURRENT_TIMESTAMP AS final_timestamp
FROM all_measurement_scores;