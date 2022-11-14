# flink-lab

1. Build image: `make build-flink-image`
1. Start flink services (JobManager and TaskManager): `make start-flink-container`
1. Submit job: `make submit-job`
1. View job execution at http://localhost:8085

# Flink examples
- (PulsarConnector)[https://github.com/apache/flink/blob/master/flink-python/pyflink/datastream/connectors/pulsar.py]
- (DataStream processing)[https://github.com/apache/flink/blob/9a725746b0abdb00d3ad7135ae39bf02e8fd3abe/flink-python/pyflink/examples/datastream/process_json_data.py]
- [Distribute python code](https://www.youtube.com/watch?v=00JgwB5vJps)

# Pulsar

# Schema
- (Schema evolution and compatibility)[https://pulsar.apache.org/docs/schema-evolution-compatibility]
- (User Defined DataTypes not supported for python)[https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/#user-defined-data-types]

https://search.maven.org/search?q=a:flink-connector-pulsar
