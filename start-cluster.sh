sudo rm -rf ./data
sudo rm -rf ./logs
sudo rm -rf ./tiered/kafka-1
sudo rm -rf ./tiered/kafka-2
sudo rm -rf ./tiered/kafka-3

sudo mkdir -p ./data/kafka-{1,2,3} /data/zookeeper-{1,2,3} ./logs/zookeeper-{1,2,3}
sudo chown -R 10000 data
sudo chown -R 10000 logs
sudo chown -R 10000 tiered
sudo sysctl -w vm.max_map_count=2000000
docker compose up -d