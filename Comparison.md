# CPU

(rate(flink_jobmanager_Status_JVM_CPU_Time[1m]) / 1e9) + on() (sum by (job_name) (rate(flink_taskmanager_Status_JVM_CPU_Time[1m])) / 1e9)

sum(rate(process_cpu_seconds_total{job="kafka"}[1m])) 

sum(rate(doris_be_cpu{mode="system"}[1m])) / 100

# Memory

(sum(rate(flink_jobmanager_Status_JVM_Memory_Heap_Used[1m])) / 1024 / 1024 / 1024) + on() (sum by (job_name) (rate(flink_taskmanager_Status_JVM_Memory_Heap_Used[1m])) / 1024 / 1024 / 1024)

sum(rate(process_resident_memory_bytes{job="kafka"}[1m])) / 1024 / 1024 / 1024

sum(rate(doris_be_memory_pool_bytes_total)[1m]) / 1024 / 1024 / 1024

# Storage

sum (kafka_log_log_size)

sum(doris_be_disks_local_used_capacity) / 1024 / 1024 / 1024

minio_cluster_usage_total_bytes / 1024 / 1024 / 1024

# Throughtput

sum(rate(kafka_server_brokertopicmetrics_messagesinpersec{job="kafka"}[1m]))
sum(rate(flink_taskmanager_job_task_operator_numRecordsInPerSecond[1m]))

# Latency

max(timestampdiff(second, ingestion_timestamp, aggregation_timestamp)) as i2s_max_diff,
max(timestampdiff(second, aggregation_timestamp, storage_timestamp)) as max_storage_diff
from gdnews2_scores;


doris_fe_query_latency_ms