# End-to-End Heart Rate measurement Kappa Architecture

## Apache Doris
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

## Flink

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