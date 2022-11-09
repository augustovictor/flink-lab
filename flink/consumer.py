from pyflink.common import SimpleStringSchema, WatermarkStrategy
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


if __name__ == "__main__":
    topic = "my-topic"
    pulsar_service_url = "pulsar://pulsar:6650"
    pulsar_admin_url = "http://pulsar:8080"
    print("INITIAL...")

    env = StreamExecutionEnvironment.get_execution_environment()
    print(env)
    env.set_parallelism(1)
    env.add_jars("file:///home/pyflink/flink-connector-pulsar-1.16.0.jar")
    env.add_jars("file:///home/pyflink/flink-sql-connector-pulsar-1.15.1.1.jar")

    pulsar_source = (
        PulsarSource.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_topics(topic)
        .set_start_cursor(StartCursor.earliest())
        .set_unbounded_stop_cursor(StopCursor.never())
        .set_subscription_name("pyflink_subscription")
        .set_subscription_type(SubscriptionType.Exclusive)
        .set_deserialization_schema(
            PulsarDeserializationSchema.flink_schema(SimpleStringSchema())
        )
        .set_config("pulsar.source.enableAutoAcknowledgeMessage", True)
        .set_config("rest.flamegraph.enabled", True)
        .build()
    )

    ds = env.from_source(
        source=pulsar_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="pulsar source",
    )
    # Should be created before usage. it was created in Makefile
    destination_topic = "results"
    pulsar_sink = (
        PulsarSink.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_producer_name("pyflink_producer")
        .set_topics(destination_topic)
        .set_serialization_schema(
            PulsarSerializationSchema.flink_schema(SimpleStringSchema())
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN)
        .set_config("pulsar.producer.maxPendingMessages", 1000)
        .set_properties({"pulsar.producer.batchingMaxMessages": "100"})
        .build()
    )

    ds.sink_to(pulsar_sink).name("pulsar sink")
    env.execute()
