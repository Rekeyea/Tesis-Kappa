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
    CASE
        WHEN measurement_type = 'CONSCIOUSNESS' THEN
            CASE raw_value
                WHEN 'A' THEN 1
                WHEN 'V' THEN 2
                WHEN 'P' THEN 3
                WHEN 'U' THEN 4
                ELSE NULL
            END
        ELSE CAST(raw_value AS DOUBLE)
    END AS `value`,
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
SELECT * 
FROM (
    WITH respiratory_rate_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as respiratory_rate_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_respiratory_rate
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    oxygen_saturation_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as oxygen_saturation_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_oxygen_saturation
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    blood_pressure_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as blood_pressure_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_blood_pressure_systolic
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    heart_rate_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as heart_rate_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_heart_rate
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    temperature_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as temperature_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_temperature
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    ),
    consciousness_window AS (
        SELECT 
            patient_id,
            TUMBLE_START(measurement_timestamp, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(measurement_timestamp, INTERVAL '1' MINUTE) AS window_end,
            AVG(`value`) as consciousness_value,
            AVG(score) as score,
            AVG(quality_weight) as quality_weight,
            AVG(freshness_weight) as freshness_weight,

            MIN(measurement_timestamp) as measurement_timestamp,
            MIN(ingestion_timestamp) as ingestion_timestamp,
            MIN(enrichment_timestamp) as enrichment_timestamp,
            MIN(routing_timestamp) as routing_timestamp,
            MIN(scoring_timestamp) as scoring_timestamp
        FROM scores_consciousness
        GROUP BY patient_id, TUMBLE(measurement_timestamp, INTERVAL '1' MINUTE)
    )
    
    SELECT 
        COALESCE(rr.patient_id, os.patient_id, bp_val.patient_id, hr.patient_id, temp.patient_id, cons.patient_id) AS patient_id,
        COALESCE(rr.window_start, os.window_start, bp_val.window_start, hr.window_start, temp.window_start, cons.window_start) AS window_start,
        COALESCE(rr.window_end, os.window_end, bp_val.window_end, hr.window_end, temp.window_end, cons.window_end) AS window_end,
        -- Raw measurements
        COALESCE(AVG(rr.respiratory_rate_value), 0) as respiratory_rate_value,
        COALESCE(AVG(os.oxygen_saturation_value), 0) as oxygen_saturation_value,
        COALESCE(AVG(bp_val.blood_pressure_value), 0) as blood_pressure_value,
        COALESCE(AVG(hr.heart_rate_value), 0) as heart_rate_value,
        COALESCE(AVG(temp.temperature_value), 0) as temperature_value,
        COALESCE(AVG(cons.consciousness_value), 0) as consciousness_value,
        
        -- Raw NEWS2 scores
        COALESCE(AVG(rr.score), 0) as respiratory_rate_score,
        COALESCE(AVG(os.score), 0) as oxygen_saturation_score,
        COALESCE(AVG(bp_val.score), 0) as blood_pressure_score,
        COALESCE(AVG(hr.score), 0) as heart_rate_score,
        COALESCE(AVG(temp.score), 0) as temperature_score,
        COALESCE(AVG(cons.score), 0) as consciousness_score,
        
        -- Calculate raw NEWS2 total
        (
            COALESCE(AVG(rr.score), 0) +
            COALESCE(AVG(os.score), 0) +
            COALESCE(AVG(bp_val.score), 0) +
            COALESCE(AVG(hr.score), 0) +
            COALESCE(AVG(temp.score), 0) +
            COALESCE(AVG(cons.score), 0)) as news2_score,
        
        -- Measurements statuses
        CAST(CASE 
            WHEN 0.7 * AVG(rr.quality_weight) + 0.3 * AVG(rr.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(rr.quality_weight) + 0.3 * AVG(rr.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END 
        AS STRING) as respiratory_rate_status,
        CAST(CASE 
            WHEN 0.7 * AVG(os.quality_weight) + 0.3 * AVG(os.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(os.quality_weight) + 0.3 * AVG(os.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END
        AS STRING) as oxygen_saturation_status,
        CAST(CASE 
            WHEN 0.7 * AVG(bp_val.quality_weight) + 0.3 * AVG(bp_val.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(bp_val.quality_weight) + 0.3 * AVG(bp_val.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END
        AS STRING) as blood_pressure_status,
        CAST(CASE 
            WHEN 0.7 * AVG(hr.quality_weight) + 0.3 * AVG(hr.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(hr.quality_weight) + 0.3 * AVG(hr.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END
        AS STRING) as heart_rate_status,
        CAST(CASE 
            WHEN 0.7 * AVG(temp.quality_weight) + 0.3 * AVG(temp.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(temp.quality_weight) + 0.3 * AVG(temp.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END
        AS STRING) as temperature_status,
        CAST(CASE 
            WHEN 0.7 * AVG(cons.quality_weight) + 0.3 * AVG(cons.freshness_weight) > 0.75 THEN 'VALID'
            WHEN 0.7 * AVG(cons.quality_weight) + 0.3 * AVG(cons.freshness_weight) > 0.5 THEN 'DEGRADED'
            ELSE 'INVALID'
        END
        AS STRING) as consciousness_status,

        -- Trust scores
        0.7 * AVG(rr.quality_weight) + 0.3 * AVG(rr.freshness_weight) AS respiratory_rate_trust_score,
        0.7 * AVG(os.quality_weight) + 0.3 * AVG(os.freshness_weight) AS oxygen_saturation_trust_score,
        0.7 * AVG(bp_val.quality_weight) + 0.3 * AVG(bp_val.freshness_weight) AS blood_pressure_trust_score,
        0.7 * AVG(hr.quality_weight) + 0.3 * AVG(hr.freshness_weight) AS heart_rate_trust_score,
        0.7 * AVG(temp.quality_weight) + 0.3 * AVG(temp.freshness_weight) AS temperature_trust_score,
        0.7 * AVG(cons.quality_weight) + 0.3 * AVG(cons.freshness_weight) AS consciousness_trust_score,

        (
            0.7 * AVG(rr.quality_weight) + 0.3 * AVG(rr.freshness_weight) +            
            0.7 * AVG(os.quality_weight) + 0.3 * AVG(os.freshness_weight) +
            0.7 * AVG(bp_val.quality_weight) + 0.3 * AVG(bp_val.freshness_weight) +
            0.7 * AVG(hr.quality_weight) + 0.3 * AVG(hr.freshness_weight) +
            0.7 * AVG(temp.quality_weight) + 0.3 * AVG(temp.freshness_weight) +
            0.7 * AVG(cons.quality_weight) + 0.3 * AVG(cons.freshness_weight)
        ) / 6 AS news2_trust_score,

        
        -- Timestamps
        MIN(COALESCE(
            rr.measurement_timestamp,
            os.measurement_timestamp,
            bp_val.measurement_timestamp,
            hr.measurement_timestamp,
            temp.measurement_timestamp,
            cons.measurement_timestamp
        )) as measurement_timestamp,
        
        MIN(COALESCE(
            cons.ingestion_timestamp,
            temp.ingestion_timestamp,
            hr.ingestion_timestamp,
            bp_val.ingestion_timestamp,
            os.ingestion_timestamp,
            rr.ingestion_timestamp
        )) as ingestion_timestamp,
        
        MIN(COALESCE(
            cons.enrichment_timestamp,
            temp.enrichment_timestamp,
            hr.enrichment_timestamp,
            bp_val.enrichment_timestamp,
            os.enrichment_timestamp,
            rr.enrichment_timestamp
        )) as enrichment_timestamp,
        
        MIN(COALESCE(
            cons.routing_timestamp,
            temp.routing_timestamp,
            hr.routing_timestamp,
            bp_val.routing_timestamp,
            os.routing_timestamp,
            rr.routing_timestamp
        )) as routing_timestamp,
        
        MIN(COALESCE(
            cons.scoring_timestamp,
            temp.scoring_timestamp,
            hr.scoring_timestamp,
            bp_val.scoring_timestamp,
            os.scoring_timestamp,
            rr.scoring_timestamp
        )) as scoring_timestamp,

        CURRENT_TIMESTAMP as flink_timestamp

    FROM respiratory_rate_window rr
    FULL JOIN oxygen_saturation_window os
        ON rr.patient_id = os.patient_id
        AND rr.window_start = os.window_start
        AND rr.window_end = os.window_end
    FULL JOIN blood_pressure_window bp_val
        ON rr.patient_id = bp_val.patient_id
        AND rr.window_start = bp_val.window_start
        AND rr.window_end = bp_val.window_end
    FULL JOIN heart_rate_window hr
        ON rr.patient_id = hr.patient_id
        AND rr.window_start = hr.window_start
        AND rr.window_end = hr.window_end
    FULL JOIN temperature_window temp
        ON rr.patient_id = temp.patient_id
        AND rr.window_start = temp.window_start
        AND rr.window_end = temp.window_end
    FULL JOIN consciousness_window cons
        ON rr.patient_id = cons.patient_id
        AND rr.window_start = cons.window_start
        AND rr.window_end = cons.window_end
    GROUP BY
        COALESCE(rr.patient_id, os.patient_id, bp_val.patient_id, hr.patient_id, temp.patient_id, cons.patient_id),
        COALESCE(rr.window_start, os.window_start, bp_val.window_start, hr.window_start, temp.window_start, cons.window_start),
        COALESCE(rr.window_end, os.window_end, bp_val.window_end, hr.window_end, temp.window_end, cons.window_end)
) v;


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