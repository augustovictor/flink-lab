# flink-lab

1. Build image: `make build-flink-image`
1. Start flink services (JobManager and TaskManager): `make start-flink-container`
1. Submit job: `make submit-job`
1. View job execution at http://localhost:8085

# Flink examples
- (PulsarConnector)[https://github.com/apache/flink/blob/master/flink-python/pyflink/datastream/connectors/pulsar.py]
- (DataStream processing)[https://github.com/apache/flink/blob/9a725746b0abdb00d3ad7135ae39bf02e8fd3abe/flink-python/pyflink/examples/datastream/process_json_data.py]

# Pulsar

# Schema
- (Schema evolution and compatibility)[https://pulsar.apache.org/docs/schema-evolution-compatibility]
