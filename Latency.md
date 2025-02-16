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