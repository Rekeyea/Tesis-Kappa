docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/enrichment.sql
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/routing.sql
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/scoring.sql
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/union.sql
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/aggregate.sql
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/bin/jobs/load.sql