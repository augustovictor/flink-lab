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

from pyflink.datastream.formats.avro import AvroRowDeserializationSchema, AvroRowSerializationSchema, AvroInputFormat

from pyflink.datastream import MapFunction


# SimpleStringSchema()


class User:
    def __init__(self, id, name, last_name):
        self.id = id
        self.name = name
        self.last_name = last_name

class UserEnriched:
    def __init__(self, id, name, last_name):
        self.id = id
        self.full_name = f"{name} {last_name}"


class PassThroughFunction(MapFunction):
    def map(self, value):
        return value

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

    ENRICHED_USER_SCHEMA = """
    {
        "type" : "record",
        "name" : "UserEnriched",
        "fields" : [
            {"name" : "id", "type" : ["null", "int" ]},
            {"name" : "full_name", "type" : ["null", "string"]}
        ]
    }
    """

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
        # .set_config("pulsar.source.enableAutoAcknowledgeMessage", True)
        .build()
    )
    # TODO consume and process two streams
    # TODO create new avro message and publish to results topic

    # def append_str(data):
    #     user = data
    #     return data

    # def enrich_user(data):
    #     schema = AvroSchema.parse_string(ENRICHED_USER_SCHEMA)
    #     object = UserEnriched(data["id"], data["name"], data["last_name"])
    #     return schema, object

    def enrich_user(data):
        return data

    # def process_users_and_configs(data):
    #     user = data
    #     return data

    ds = env.from_source(
        source=pulsar_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="pulsarUserSource",
    ).uid("pulsarUserSource")

    # ds.key_by(lambda x: x[0]).process(process_users_and_configs).uid("configsLookup").name("configsLookup")

    # ds.map(append_str).print().name("print").uid("print")
    # ds.print().name("print").uid("print")
    # Should be created before usage. it was created in Makefile
    destination_topic = "results"
    pulsar_sink = (
        PulsarSink.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_producer_name("pyflink_producer")
        .set_topics(destination_topic)
        .set_serialization_schema(
            # PulsarSerializationSchema.flink_schema(SimpleStringSchema())
            # PulsarSerializationSchema.flink_schema(AvroRowSerializationSchema(avro_schema_string=ENRICHED_USER_SCHEMA))
            PulsarSerializationSchema.flink_schema(AvroRowSerializationSchema(avro_schema_string=USER_SCHEMA))
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN)
        .set_config("pulsar.producer.maxPendingMessages", 1000)
        .set_properties({"pulsar.producer.batchingMaxMessages": "100"})
        .build()
    )
    # env.create_input(AvroInputFormat())
    ds.sink_to(pulsar_sink).name("pulsar sink")
    ds.print().name("print")
    env.execute("Sample messages processing")
