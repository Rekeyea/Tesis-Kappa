# Monitoring

# Kafka Flink SQL


## Example
```sql
CREATE TABLE heartrate_source (
    `user_id` STRING,
    `heart_rate` INT,
    `timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'heartrate_data',
    'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
    'properties.group.id' = 'heartrate-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

```sql
SELECT 
    `user_id`,
    `heart_rate`,
    `timestamp`
FROM heartrate_source;
```

```sql
CREATE TABLE heartrate_generator (
    `user_id` STRING,
    `heart_rate` INT,
    `timestamp` TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.user_id.length' = '10',
    'fields.heart_rate.min' = '60',
    'fields.heart_rate.max' = '180'
);
```

```sql
INSERT INTO heartrate_source
SELECT 
    `user_id`,
    `heart_rate`,
    `timestamp`
FROM heartrate_generator;
```

## More Complex Example

```sql
-- Create tables for synthetic data generation
CREATE TABLE device1_readings (
  patient_id STRING,
  device_id STRING,
  heart_rate INT,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.patient_id.length' = '8',
  'fields.device_id.length' = '10',
  'fields.heart_rate.min' = '40',
  'fields.heart_rate.max' = '180'
);
```

```sql
CREATE TABLE device2_readings (
  patient_id STRING,
  device_id STRING,
  heart_rate DOUBLE,  -- Different data type to simulate device variation
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '3',
  'fields.patient_id.length' = '8',
  'fields.device_id.length' = '10',
  'fields.heart_rate.min' = '40.0',
  'fields.heart_rate.max' = '180.0'
);
```

```sql
CREATE TABLE device3_readings (
  patient_id STRING,
  device_id STRING,
  heart_rate INT,  -- Similar to device1 but different sampling rate
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '2',
  'fields.patient_id.length' = '8',
  'fields.device_id.length' = '10',
  'fields.heart_rate.min' = '40',
  'fields.heart_rate.max' = '180'
);
```

```sql
-- Create Kafka sink for raw readings
CREATE TABLE raw_readings_topic (
  patient_id STRING,
  device_id STRING,
  heart_rate DOUBLE,
  measurement_time TIMESTAMP(3),
  device_type STRING,
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_raw_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
-- Create Kafka sink for standardized readings
CREATE TABLE standardized_readings_topic (
  patient_id STRING,
  device_id STRING,
  heart_rate DOUBLE,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'standardized_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_standardized_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
-- Create Kafka sink for aggregated readings
CREATE TABLE aggregated_readings_topic (
  patient_id STRING,
  avg_heart_rate DOUBLE,
  min_heart_rate DOUBLE,
  max_heart_rate DOUBLE,
  device_count BIGINT,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'aggregated_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_aggregated_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
-- Step 1: Combine all device readings into raw_readings_topic
INSERT INTO raw_readings_topic
SELECT 
  patient_id,
  device_id,
  CAST(heart_rate AS DOUBLE) as heart_rate,
  measurement_time,
  'device1' as device_type
FROM device1_readings
UNION ALL
SELECT 
  patient_id,
  device_id,
  heart_rate,
  measurement_time,
  'device2' as device_type
FROM device2_readings
UNION ALL
SELECT 
  patient_id,
  device_id,
  CAST(heart_rate AS DOUBLE) as heart_rate,
  measurement_time,
  'device3' as device_type
FROM device3_readings;
```

```sql
-- Step 2: Standardize readings
-- Here we're applying basic standardization, but you could add more complex logic
INSERT INTO standardized_readings_topic
SELECT
  patient_id,
  device_id,
  CASE
    WHEN device_type = 'device2' THEN heart_rate * 1.0  -- device2 is our reference
    ELSE heart_rate * 1.0  -- Add conversion factors if needed
  END as heart_rate,
  measurement_time
FROM raw_readings_topic;
```

```sql
-- Step 3: Aggregate readings per patient with 1-minute tumbling window
INSERT INTO aggregated_readings_topic
SELECT
  patient_id,
  AVG(heart_rate) as avg_heart_rate,
  MIN(heart_rate) as min_heart_rate,
  MAX(heart_rate) as max_heart_rate,
  COUNT(DISTINCT device_id) as device_count,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE standardized_readings_topic, DESCRIPTOR(measurement_time), INTERVAL '1' MINUTE)
)
GROUP BY patient_id, window_start, window_end;
```

## Complete Example for Heart Rate

```sql
CREATE TABLE device1_readings (
  patient_id INT,
  device_id STRING,
  heart_rate INT,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.patient_id.min' = '1',
  'fields.patient_id.max' = '10',
  'fields.device_id.length' = '3',
  'fields.heart_rate.min' = '40',
  'fields.heart_rate.max' = '180'
);
```

```sql
CREATE TABLE device2_readings (
  patient_id INT,
  device_id STRING,
  heart_rate DOUBLE,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '3',
  'fields.patient_id.min' = '1',
  'fields.patient_id.max' = '10',
  'fields.device_id.length' = '3',
  'fields.heart_rate.min' = '40.0',
  'fields.heart_rate.max' = '180.0'
);
```

```sql
CREATE TABLE device3_readings (
  patient_id INT,
  device_id STRING,
  heart_rate INT,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '2',
  'fields.patient_id.min' = '1',
  'fields.patient_id.max' = '10',
  'fields.device_id.length' = '3',
  'fields.heart_rate.min' = '40',
  'fields.heart_rate.max' = '180'
);
```

```sql
CREATE TABLE raw_readings_topic (
  patient_id INT,
  device_id STRING,
  heart_rate DOUBLE,
  measurement_time TIMESTAMP(3),
  device_type STRING,
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_raw_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```
```sql
CREATE TABLE standardized_readings_topic (
  patient_id INT,
  device_id STRING,
  heart_rate DOUBLE,
  measurement_time TIMESTAMP(3),
  WATERMARK FOR measurement_time AS measurement_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'standardized_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_standardized_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```
```sql
CREATE TABLE aggregated_readings_topic (
  patient_id INT,
  avg_heart_rate DOUBLE,
  min_heart_rate DOUBLE,
  max_heart_rate DOUBLE,
  device_count BIGINT,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'aggregated_readings',
  'properties.bootstrap.servers' = 'kafka-1:19091,kafka-2:19092,kafka-3:19093',
  'properties.group.id' = 'patient_monitoring_aggregated_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

```sql
-- Step 1: Combine all device readings into raw_readings_topic
INSERT INTO raw_readings_topic
SELECT 
  patient_id,
  device_id,
  CAST(heart_rate AS DOUBLE) as heart_rate,
  measurement_time,
  'device1' as device_type
FROM device1_readings
UNION ALL
SELECT 
  patient_id,
  device_id,
  heart_rate,
  measurement_time,
  'device2' as device_type
FROM device2_readings
UNION ALL
SELECT 
  patient_id,
  device_id,
  CAST(heart_rate AS DOUBLE) as heart_rate,
  measurement_time,
  'device3' as device_type
FROM device3_readings;
```

```sql
-- Step 2: Standardize readings
-- Here we're applying basic standardization, but you could add more complex logic
INSERT INTO standardized_readings_topic
SELECT
  patient_id,
  device_id,
  CASE
    WHEN device_type = 'device2' THEN heart_rate * 1.0  -- device2 is our reference
    ELSE heart_rate * 1.0  -- Add conversion factors if needed
  END as heart_rate,
  measurement_time
FROM raw_readings_topic;
```

```sql
-- Step 3: Aggregate readings per patient with 1-minute tumbling window
INSERT INTO aggregated_readings_topic
SELECT
  patient_id,
  AVG(heart_rate) as avg_heart_rate,
  MIN(heart_rate) as min_heart_rate,
  MAX(heart_rate) as max_heart_rate,
  COUNT(DISTINCT device_id) as device_count,
  window_start,
  window_end
FROM TABLE(
  TUMBLE(TABLE standardized_readings_topic, DESCRIPTOR(measurement_time), INTERVAL '1' MINUTE)
)
GROUP BY patient_id, window_start, window_end;
```

## How to Work with a Score that can have missing values?

```sql
-- Combine measurements with handling for missing values
INSERT INTO combined_measurements_topic
SELECT 
  COALESCE(h.patient_id, o.patient_id, t.patient_id) as patient_id,
  COALESCE(h.measurement_time, o.measurement_time, t.measurement_time) as measurement_time,
  h.heart_rate,
  o.spo2_level,
  t.temperature,
  -- Calculate health score with NULL handling
  CASE
    -- Full score when all measurements are present
    WHEN h.heart_rate IS NOT NULL AND o.spo2_level IS NOT NULL AND t.temperature IS NOT NULL THEN
      (
        -- Normalize and weight each measurement
        (CASE 
          WHEN h.heart_rate BETWEEN 60 AND 100 THEN 1.0
          WHEN h.heart_rate BETWEEN 50 AND 60 OR h.heart_rate BETWEEN 100 AND 110 THEN 0.7
          ELSE 0.3
        END * 0.4) +  -- 40% weight for heart rate
        (CASE
          WHEN o.spo2_level >= 95 THEN 1.0
          WHEN o.spo2_level >= 90 THEN 0.7
          ELSE 0.3
        END * 0.35) +  -- 35% weight for SpO2
        (CASE
          WHEN t.temperature BETWEEN 36.5 AND 37.5 THEN 1.0
          WHEN t.temperature BETWEEN 35.5 AND 36.5 OR t.temperature BETWEEN 37.5 AND 38.5 THEN 0.7
          ELSE 0.3
        END * 0.25)    -- 25% weight for temperature
      )
    -- Partial score when some measurements are missing
    WHEN h.heart_rate IS NOT NULL OR o.spo2_level IS NOT NULL OR t.temperature IS NOT NULL THEN
      (
        COALESCE(
          (CASE 
            WHEN h.heart_rate BETWEEN 60 AND 100 THEN 1.0
            WHEN h.heart_rate BETWEEN 50 AND 60 OR h.heart_rate BETWEEN 100 AND 110 THEN 0.7
            WHEN h.heart_rate IS NOT NULL THEN 0.3
          END * 0.4), 0
        ) +
        COALESCE(
          (CASE
            WHEN o.spo2_level >= 95 THEN 1.0
            WHEN o.spo2_level >= 90 THEN 0.7
            WHEN o.spo2_level IS NOT NULL THEN 0.3
          END * 0.35), 0
        ) +
        COALESCE(
          (CASE
            WHEN t.temperature BETWEEN 36.5 AND 37.5 THEN 1.0
            WHEN t.temperature BETWEEN 35.5 AND 36.5 OR t.temperature BETWEEN 37.5 AND 38.5 THEN 0.7
            WHEN t.temperature IS NOT NULL THEN 0.3
          END * 0.25), 0
        )
      ) / NULLIF(
        -- Normalize by available measurements
        (CASE WHEN h.heart_rate IS NOT NULL THEN 0.4 ELSE 0 END +
         CASE WHEN o.spo2_level IS NOT NULL THEN 0.35 ELSE 0 END +
         CASE WHEN t.temperature IS NOT NULL THEN 0.25 ELSE 0 END),
        0
      )
    ELSE NULL
  END as health_score,
  -- Quality indicator based on available measurements
  CASE
    WHEN h.heart_rate IS NOT NULL AND o.spo2_level IS NOT NULL AND t.temperature IS NOT NULL THEN 'COMPLETE'
    WHEN h.heart_rate IS NULL AND o.spo2_level IS NULL AND t.temperature IS NULL THEN 'NO_DATA'
    ELSE 'PARTIAL'
  END as measurement_quality
FROM 
  standardized_readings_topic h
  FULL OUTER JOIN oximeter_readings o 
    ON h.patient_id = o.patient_id 
    AND h.measurement_time BETWEEN o.measurement_time - INTERVAL '30' SECONDS 
    AND o.measurement_time + INTERVAL '30' SECONDS
  FULL OUTER JOIN temperature_readings t
    ON COALESCE(h.patient_id, o.patient_id) = t.patient_id
    AND COALESCE(h.measurement_time, o.measurement_time) BETWEEN t.measurement_time - INTERVAL '30' SECONDS 
    AND t.measurement_time + INTERVAL '30' SECONDS
WHERE 
  COALESCE(h.measurement_time, o.measurement_time, t.measurement_time) IS NOT NULL;
```

Key features of this solution:

Uses FULL OUTER JOIN to catch all measurements, even when some are missing
Implements a 30-second window to match readings that are close in time
Calculates a weighted health score that:

- Gives full score when all measurements are present
- Provides partial score normalized by available measurements
- Returns NULL when no measurements are available


Includes a measurement_quality field to track completeness
Handles missing values through:

COALESCE for basic fields
CASE statements for score calculation
Normalization based on available measurements


Uses verification queries to monitor data quality


# Doris

```sql
CREATE DATABASE measurements;
```

```sql
USE measurements;
```

```sql
CREATE TABLE IF NOT EXISTS patient_heart_rate_stats (
    patient_id INT,
    window_start DATETIME,
    avg_heart_rate DOUBLE,
    min_heart_rate DOUBLE,
    max_heart_rate DOUBLE,
    device_count BIGINT,
    window_end DATETIME
)
DUPLICATE KEY(patient_id, window_start)
DISTRIBUTED BY HASH(patient_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "3"
);
```

Now in Flink:

```sql
CREATE TABLE doris_heart_rate_stats (
    patient_id INT,
    avg_heart_rate DOUBLE,
    min_heart_rate DOUBLE,
    max_heart_rate DOUBLE,
    device_count BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (patient_id, window_start) NOT ENFORCED,
    WATERMARK FOR window_start AS window_start - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'doris',
    'fenodes' = '172.20.4.2:8030',
    'table.identifier' = 'measurements.patient_heart_rate_stats',
    'username' = 'root',
    'password' = '',
    'sink.properties.format' = 'json',
    'sink.properties.read_timeout' = '3600',
    'sink.buffer-flush.max-rows' = '10000'
);
```

```sql
SET 'execution.runtime-mode' = 'streaming';
-- Set checkpoint interval (e.g., every 10 seconds)
SET 'execution.checkpointing.interval' = '10s';
-- Enable exactly-once semantic
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Set minimum pause between checkpoints
SET 'execution.checkpointing.min-pause' = '500';
-- Set checkpoint timeout
SET 'execution.checkpointing.timeout' = '600000';
INSERT INTO doris_heart_rate_stats
SELECT 
    patient_id,
    avg_heart_rate,
    min_heart_rate,
    max_heart_rate,
    device_count,
    window_start,
    window_end
FROM aggregated_readings_topic;
```