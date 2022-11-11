from pyflink.common import SimpleStringSchema, WatermarkStrategy

# from pyflink.common.serialization import AvroRowDeserializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.pulsar import (
    PulsarSource,
    PulsarSink,
    PulsarSerializationSchema,
    StartCursor,
    StopCursor,
    SubscriptionType,
    PulsarDeserializationSchema,
    DeliveryGuarantee,
    TopicRoutingMode,
)

from pyflink.datastream.formats.avro import AvroSchema, AvroRowDeserializationSchema

# SimpleStringSchema()


# class User(DeserializationSchema):
#     def __init__(self, id, name, last_name):
#         self.id = id
#         self.name = name
#         self.last_name = last_name


if __name__ == "__main__":
    topic = "my-topic"
    pulsar_service_url = "pulsar://pulsar:6650"
    pulsar_admin_url = "http://pulsar:8080"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(
        "file:///home/flink-jars/flink-connector-pulsar-1.16.0.jar",
        "file:///home/flink-jars/flink-sql-connector-pulsar-1.15.1.1.jar",
        # "file:///home/flink-jars/flink-avro-1.16.0.jar",
        "file:///home/flink-jars/flink-sql-avro-1.16.0.jar",
    )

    USER_SCHEMA = """
    {
        "type" : "record",
        "name" : "User",
        "fields" : [
            {"name" : "id", "type" : ["null", "int" ]},
            {"name" : "name", "type" : ["null", "string"]},
            {"name" : "last_name", "type" : ["null", "string"]}
        ]
    }
    """

    # schema = AvroSchema(User)
    pulsar_source = (
        PulsarSource.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_topics(topic)
        .set_start_cursor(StartCursor.earliest())
        .set_unbounded_stop_cursor(StopCursor.never())
        .set_subscription_name("pyflink_subscription")
        .set_subscription_type(SubscriptionType.Exclusive)
        # https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/pulsar/#deserializer
        .set_deserialization_schema(
            PulsarDeserializationSchema.flink_schema(
                AvroRowDeserializationSchema(avro_schema_string=USER_SCHEMA)
            )
        )
        # .set_deserialization_schema(PulsarDeserializationSchema.flink_schema(SimpleStringSchema()))
        .set_config("pulsar.source.enableAutoAcknowledgeMessage", True)
        # .set_config("rest.flamegraph.enabled", True)
        .build()
    )

    # def append_str(data):
    #     data += " [MODIFIED BY FLINK]"
    #     return data

    ds = env.from_source(
        source=pulsar_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="pulsar source",
    )
    # ds.map(append_str).print().name("print")
    ds.print().name("print")
    # Should be created before usage. it was created in Makefile
    # destination_topic = "results"
    # pulsar_sink = (
    #     PulsarSink.builder()
    #     .set_service_url(pulsar_service_url)
    #     .set_admin_url(pulsar_admin_url)
    #     .set_producer_name("pyflink_producer")
    #     .set_topics(destination_topic)
    #     .set_serialization_schema(
    #         PulsarSerializationSchema.flink_schema(SimpleStringSchema())
    #     )
    #     .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    #     .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN)
    #     # .set_config("pulsar.producer.maxPendingMessages", 1000)
    #     # .set_properties({"pulsar.producer.batchingMaxMessages": "100"})
    #     .set_config("pulsar.source.enableAutoAcknowledgeMessage", True)
    #     .build()
    # )

    # ds.sink_to(pulsar_sink).name("pulsar sink")
    env.execute("Sample messages processing")
