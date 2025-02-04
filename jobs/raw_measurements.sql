USE kappa_catalog;
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