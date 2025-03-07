# Metrics Streaming Service

This is a metrics streaming service that allows us to stream and aggregate metrics effectively!

## Developer's Guide

1. On a terminal window, install and spin up kafka + zookeeper:

```bash
# 1. Install Kafka and Zookeeper
brew install kafka
brew install zookeeper

# 2. Start services
brew services restart zookeeper
brew services start kafka

# 3. Verify services are running
brew services list | grep -E 'kafka|zookeeper'
```

2. Install relevant libraries (Activate custom-environment if needed)

```bash
# 1. 
pip install -r requirements.txt


```

3. Initialize kafka topic and run the application.

```bash
# 1. Delete stale kafka topics
kafka-topics --delete --bootstrap-server localhost:9092 --topic latency-metrics

# 2. Restart Kafka and Zookeeper if needed
brew services restart zookeeper
brew services restart kafka

# 3. Create topic with replication-factor 1 for local testing
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic latency-metrics

# 4. Verify topic creation
kafka-topics --list --bootstrap-server localhost:9092

# 5. Run application
python ./src/main.py --csv ./data/HDFS_v1/preprocessed/Event_traces.csv

```

4. Monitor & Observe

```bash
# Terminal 2: Monitor consumer group
watch -n 1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group latency-monitor-group

# Terminal 3: Monitor topic metrics (iffy)
watch -n 1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic latency-metrics
```
