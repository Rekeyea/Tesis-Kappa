Initial Topic: raw.measurements

```sql
-- Source table for all raw measurements
CREATE TABLE raw_measurements (
    id STRING,
    measurement_type STRING,
    device_id STRING,
    raw_value STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.measurements',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Dynamic routing to measurement-specific topics
CREATE TABLE measurement_sink (
    id STRING,
    device_id STRING,
    numeric_value DOUBLE,
    string_value STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.${lowercase(measurement_type)}',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Route measurements to their specific topics
INSERT INTO measurement_sink
SELECT 
    id,
    patient_id,
    device_id,
    readings
FROM raw_measurements
WHERE measurement_type IN (
    'RESPIRATORY_RATE',
    'OXYGEN_SATURATION',
    'BLOOD_PRESSURE',
    'HEART_RATE',
    'TEMPERATURE',
    'CONSCIOUSNESS'
);
```

```sql
-- Create individual score output tables using dynamic routing
CREATE TABLE measurement_scores (
    id STRING,
    patient_id STRING,
    measurement_type STRING,
    timestamp TIMESTAMP(3),
    value DOUBLE,
    consciousness_value STRING,
    raw_news2_score INTEGER,
    freshness_weight DOUBLE,
    quality_weight DOUBLE,
    adjusted_score DOUBLE,
    status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.${lowercase(measurement_type)}',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Calculate scores for each measurement type
INSERT INTO measurement_scores
SELECT
    m.id,
    m.patient_id,
    m.measurement_type,
    m.readings.timestamp,
    CASE m.measurement_type
        WHEN 'CONSCIOUSNESS' THEN NULL
        WHEN 'BLOOD_PRESSURE' THEN m.readings.systolic
        ELSE m.readings.raw_value
    END as value,
    m.readings.acvpu_value as consciousness_value,
    -- NEWS2 Score calculation
    CASE m.measurement_type
        WHEN 'RESPIRATORY_RATE' THEN
            CASE
                WHEN m.readings.raw_value <= 8 THEN 3
                WHEN m.readings.raw_value <= 11 THEN 1
                WHEN m.readings.raw_value <= 20 THEN 0
                WHEN m.readings.raw_value <= 24 THEN 2
                ELSE 3
            END
        WHEN 'OXYGEN_SATURATION' THEN
            CASE
                WHEN m.readings.raw_value <= 91 THEN 3
                WHEN m.readings.raw_value <= 93 THEN 2
                WHEN m.readings.raw_value <= 95 THEN 1
                ELSE 0
            END
        WHEN 'BLOOD_PRESSURE' THEN
            CASE
                WHEN m.readings.systolic <= 90 THEN 3
                WHEN m.readings.systolic <= 100 THEN 2
                WHEN m.readings.systolic <= 110 THEN 1
                WHEN m.readings.systolic <= 219 THEN 0
                ELSE 3
            END
        WHEN 'HEART_RATE' THEN
            CASE
                WHEN m.readings.raw_value <= 40 THEN 3
                WHEN m.readings.raw_value <= 50 THEN 1
                WHEN m.readings.raw_value <= 90 THEN 0
                WHEN m.readings.raw_value <= 110 THEN 1
                WHEN m.readings.raw_value <= 130 THEN 2
                ELSE 3
            END
        WHEN 'TEMPERATURE' THEN
            CASE
                WHEN m.readings.raw_value <= 35.0 THEN 3
                WHEN m.readings.raw_value <= 36.0 THEN 1
                WHEN m.readings.raw_value <= 38.0 THEN 0
                WHEN m.readings.raw_value <= 39.0 THEN 1
                ELSE 2
            END
        WHEN 'CONSCIOUSNESS' THEN
            CASE m.readings.acvpu_value
                WHEN 'A' THEN 0
                ELSE 3
            END
    END as raw_news2_score,
    -- Freshness weight
    CASE m.measurement_type
        WHEN 'CONSCIOUSNESS' THEN 
            GREATEST(0, 1 - (TIMESTAMPDIFF(HOUR, m.readings.timestamp, CURRENT_TIMESTAMP) / 24.0))
        WHEN 'RESPIRATORY_RATE' THEN 
            GREATEST(0, 1 - (TIMESTAMPDIFF(HOUR, m.readings.timestamp, CURRENT_TIMESTAMP) / 8.0))
        WHEN 'OXYGEN_SATURATION' THEN 
            GREATEST(0, 1 - (TIMESTAMPDIFF(HOUR, m.readings.timestamp, CURRENT_TIMESTAMP) / 8.0))
        WHEN 'HEART_RATE' THEN 
            GREATEST(0, 1 - (TIMESTAMPDIFF(HOUR, m.readings.timestamp, CURRENT_TIMESTAMP) / 8.0))
        ELSE 
            GREATEST(0, 1 - (TIMESTAMPDIFF(HOUR, m.readings.timestamp, CURRENT_TIMESTAMP) / 12.0))
    END as freshness_weight,
    -- Quality weight
    CAST(0.4 * 0.8 +  -- device quality (assuming medical-grade)
        0.3 * (CASE WHEN m.readings.motion_detected THEN 0.6 ELSE 1.0 END) +
        0.3 * COALESCE(
            CASE m.measurement_type
                WHEN 'RESPIRATORY_RATE' THEN m.readings.signal_quality
                WHEN 'HEART_RATE' THEN m.readings.signal_quality
                WHEN 'OXYGEN_SATURATION' THEN 
                    CASE 
                        WHEN m.readings.perfusion_index > 1.0 THEN 1.0
                        WHEN m.readings.perfusion_index > 0.5 THEN 0.8
                        ELSE 0.6 
                    END
                ELSE m.readings.measurement_quality
            END,
            0.8
        ) AS DOUBLE) as quality_weight,
    -- Calculate adjusted score
    CAST(
        CASE m.measurement_type
            WHEN 'CONSCIOUSNESS' THEN 
                CASE m.readings.acvpu_value WHEN 'A' THEN 0 ELSE 3 END
            ELSE raw_news2_score
        END * freshness_weight * quality_weight AS DOUBLE
    ) as adjusted_score,
    -- Determine status
    CASE 
        WHEN freshness_weight * quality_weight >= 0.5 THEN 'VALID'
        WHEN freshness_weight * quality_weight >= 0.3 THEN 'DEGRADED'
        ELSE 'INVALID' 
    END as status
FROM raw_measurements m;
```

```sql
-- Create source table for all measurement scores
CREATE TABLE all_measurement_scores (
    id STRING,
    patient_id STRING,
    measurement_type STRING,
    timestamp TIMESTAMP(3),
    value DOUBLE,
    consciousness_value STRING,
    raw_news2_score INTEGER,
    freshness_weight DOUBLE,
    quality_weight DOUBLE,
    adjusted_score DOUBLE,
    status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.${lowercase(measurement_type)}',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Create gdNEWS2 output table
CREATE TABLE gdnews2_scores (
    patient_id STRING,
    timestamp TIMESTAMP(3),
    score DOUBLE,
    confidence DOUBLE,
    parameters MAP<STRING, ROW<
        value DOUBLE,
        consciousness_value STRING,
        raw_news2_score INTEGER,
        freshness_weight DOUBLE,
        quality_weight DOUBLE,
        adjusted_score DOUBLE,
        status STRING
    >>,
    summary_stats ROW(
        valid_parameters INTEGER,
        degraded_parameters INTEGER,
        invalid_parameters INTEGER,
        total_adjusted_score DOUBLE,
        average_quality DOUBLE,
        average_freshness DOUBLE,
        oldest_measurement_age INTEGER
    )
) WITH (
    'connector' = 'kafka',
    'topic' = 'gdnews2.scores',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- Calculate final gdNEWS2 scores
INSERT INTO gdnews2_scores
SELECT
    patient_id,
    CURRENT_TIMESTAMP as timestamp,
    SUM(adjusted_score) as score,
    (SUM(freshness_weight * quality_weight) / COUNT(*)) * 100 as confidence,
    -- Create parameters map
    MAP_AGG(
        measurement_type,
        ROW(
            value,
            consciousness_value,
            raw_news2_score,
            freshness_weight,
            quality_weight,
            adjusted_score,
            status
        )
    ) as parameters,
    -- Summary statistics
    ROW(
        COUNT(CASE WHEN status = 'VALID' THEN 1 END),
        COUNT(CASE WHEN status = 'DEGRADED' THEN 1 END),
        COUNT(CASE WHEN status = 'INVALID' THEN 1 END),
        SUM(adjusted_score),
        AVG(quality_weight),
        AVG(freshness_weight),
        MAX(TIMESTAMPDIFF(MINUTE, timestamp, CURRENT_TIMESTAMP))
    ) as summary_stats
FROM all_measurement_scores
GROUP BY 
    patient_id,
    TUMBLE(timestamp, INTERVAL '1' MINUTE);
```



```sql
-- High Throughtput Configuration



-- Raw measurements table with high parallelism
CREATE TABLE raw_measurements (
    id STRING,
    patient_id STRING,
    measurement_type STRING,
    device_id STRING,
    readings ROW(
        timestamp TIMESTAMP(3),
        raw_value DOUBLE,
        systolic DOUBLE,
        diastolic DOUBLE,
        signal_quality DOUBLE,
        perfusion_index DOUBLE,
        motion_detected BOOLEAN,
        measurement_quality DOUBLE,
        stabilization_time DOUBLE,
        acvpu_value STRING,
        assessor_id STRING
    ),
    WATERMARK FOR readings.timestamp AS readings.timestamp - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw.measurements',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-medical-router',
    -- High parallelism for input processing
    'source.parallelism' = '32',
    -- Configure Kafka partitions based on patient_id
    'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.RangeAssignor',
    -- Enable Kafka transactions for exactly-once processing
    'properties.isolation.level' = 'read_committed',
    'properties.transaction.timeout.ms' = '900000',
    -- Performance tuning
    'properties.fetch.min.bytes' = '1048576',  -- 1MB minimum fetch
    'properties.fetch.max.wait.ms' = '500',
    -- Use JSON format for flexibility
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Measurement sink with dynamic routing and high parallelism
CREATE TABLE measurement_sink (
    id STRING,
    patient_id STRING,
    device_id STRING,
    readings ROW(
        timestamp TIMESTAMP(3),
        raw_value DOUBLE,
        systolic DOUBLE,
        diastolic DOUBLE,
        signal_quality DOUBLE,
        perfusion_index DOUBLE,
        motion_detected BOOLEAN,
        measurement_quality DOUBLE,
        stabilization_time DOUBLE,
        acvpu_value STRING,
        assessor_id STRING
    )
) WITH (
    'connector' = 'kafka',
    'topic' = 'measurements.${lowercase(measurement_type)}',
    'properties.bootstrap.servers' = 'kafka:9092',
    -- High parallelism for output
    'sink.parallelism' = '24',
    -- Performance configurations
    'properties.batch.size' = '1048576',
    'properties.linger.ms' = '50',
    'properties.compression.type' = 'lz4',
    -- Partitioning strategy
    'properties.partitioner.class' = 'org.apache.kafka.clients.producer.RoundRobinPartitioner',
    'format' = 'json'
);

-- High-throughput score calculation
CREATE TABLE measurement_scores (
    id STRING,
    patient_id STRING,
    measurement_type STRING,
    timestamp TIMESTAMP(3),
    value DOUBLE,
    consciousness_value STRING,
    raw_news2_score INTEGER,
    freshness_weight DOUBLE,
    quality_weight DOUBLE,
    adjusted_score DOUBLE,
    status STRING,
    -- Partition by patient_id for better distribution
    PRIMARY KEY (patient_id, measurement_type) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'scores.${lowercase(measurement_type)}',
    'properties.bootstrap.servers' = 'kafka:9092',
    -- High parallelism for score processing
    'source.parallelism' = '24',
    'sink.parallelism' = '24',
    -- Performance tuning
    'properties.batch.size' = '1048576',
    'properties.compression.type' = 'lz4',
    'properties.linger.ms' = '50',
    -- Partition assignment
    'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
    'format' = 'json'
);

-- Final gdNEWS2 calculation with high throughput configuration
CREATE TABLE gdnews2_scores (
    patient_id STRING,
    timestamp TIMESTAMP(3),
    score DOUBLE,
    confidence DOUBLE,
    parameters MAP<STRING, ROW<
        value DOUBLE,
        consciousness_value STRING,
        raw_news2_score INTEGER,
        freshness_weight DOUBLE,
        quality_weight DOUBLE,
        adjusted_score DOUBLE,
        status STRING
    >>,
    summary_stats ROW(
        valid_parameters INTEGER,
        degraded_parameters INTEGER,
        invalid_parameters INTEGER,
        total_adjusted_score DOUBLE,
        average_quality DOUBLE,
        average_freshness DOUBLE,
        oldest_measurement_age INTEGER
    ),
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'gdnews2.scores',
    'properties.bootstrap.servers' = 'kafka:9092',
    -- High parallelism for final aggregation
    'source.parallelism' = '32',
    'sink.parallelism' = '32',
    -- Performance tuning
    'properties.batch.size' = '1048576',
    'properties.compression.type' = 'lz4',
    'properties.linger.ms' = '50',
    -- Partition by patient_id
    'properties.partition.assignment.strategy' = 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
    'format' = 'json'
);

-- Set global parallelism and checkpoint configuration
SET 'parallelism.default' = '32';
SET 'execution.checkpointing.interval' = '10000'; -- 10 seconds
SET 'execution.checkpointing.min-pause' = '5000'; -- 5 seconds
SET 'execution.checkpointing.timeout' = '900000'; -- 15 minutes
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'state.checkpoints.num-retained' = '3';
```

For 10,000 concurrent patients, here's the recommended configuration:

Task Node Distribution (Total: 88-96 task slots):

Raw Router: 32 parallel tasks
Individual Scores: 24 parallel tasks
gdNEWS2 Calculator: 32 parallel tasks
Plus JobManager nodes and some buffer for scaling


Resource Requirements:

Minimum 12-16 physical machines (8 cores each)
Or 6-8 larger machines (16 cores each)
Memory: 256-512GB total cluster memory


Kafka Configuration:

Topics should have 32-64 partitions
Replication factor: 3
Min.insync.replicas: 2