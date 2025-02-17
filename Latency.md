# Latencia 

Como Flink usa window alignment y watermarks sumado a que Doris hace un buffering the las entradas que flink manda no se puede calcular exactamente la latencia.
Sin embargo, podemos calcular un upper bound que nos dara una buena idea del o que esta pasando: 

```sql
select
    max(timestampdiff(second, ingestion_timestamp, aggregation_timestamp)) as i2s_max_diff,
    max(timestampdiff(second, aggregation_timestamp, storage_timestamp)) as max_storage_diff
from gdnews2_scores;
```

Con esta query podemos saber cual es la maxima latencia dentro de flink y cual es el maximo tiempo que le lleva a flink enviar un registro hacia doris.


# Performance

## Current message rate per topic
sum by (topic) (kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate)

## Total messages across all topics
sum(kafka_server_brokertopicmetrics_messagesinpersec)

## Bytes in per second per topic
sum by (topic) (kafka_server_brokertopicmetrics_bytesinpersec)

## Input records per task
sum by (job_name, task_name) (rate(flink_taskmanager_job_task_operator_numRecordsIn[1m]))

## Output records per task
sum by (job_name, task_name) (rate(flink_taskmanager_job_task_operator_numRecordsOut[1m]))

## Processing backlog (difference between in and out)
sum by (job_name) (rate(flink_taskmanager_job_task_operator_numRecordsIn[1m])) 
sum by (job_name) (rate(flink_taskmanager_job_task_operator_numRecordsOut[1m]))

## Kafka Producer performance
avg by (job_name) (flink_taskmanager_job_task_operator_KafkaProducer_io_ratio)
avg by (job_name) (flink_taskmanager_job_task_operator_KafkaProducer_response_rate)

## Job runtime
flink_jobmanager_job_runningTime

## Task latency
avg by (job_name) (flink_taskmanager_job_task_mailboxLatencyMs)

## Bytes output rate per task
rate(flink_taskmanager_job_task_operator_numBytesOut[1m])

## Kafka CPU seconds
sum by (instance) (rate(process_cpu_seconds_total{job="kafka"}[1m]) * 100)
rate(flink_jobmanager_Status_JVM_CPU_Time[1m])

## Flink heap memory usage
sum by (job_name) (flink_taskmanager_Status_JVM_Memory_Heap_Used)