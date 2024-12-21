# Monitoring

# Kafka Flink SQL

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
    user_id,
    heart_rate,
    `timestamp`
FROM heartrate_source;
```