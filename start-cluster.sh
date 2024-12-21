sudo rm -rf ./data
sudo mkdir -p ./data/kafka-{1,2,3} /data/zookeeper-{1,2,3} ./logs/zookeeper-{1,2,3}
sudo chown -R 10000 data
sudo chown -R 10000 logs
docker compose up -d