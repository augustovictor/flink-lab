import os
from pyflink.common import SimpleStringSchema, WatermarkStrategy
import tempfile

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

from pyflink.table.types import Row

from pyflink.datastream.formats.avro import (
    AvroRowDeserializationSchema,
    AvroRowSerializationSchema,
    # AvroInputFormat,
    AvroSchema,
    GenericRecordAvroTypeInfo,
)


from pyflink.datastream import MapFunction


class UserEnriched:
    def __init__(self, id, name, last_name):
        self.id = id
        self.full_name = f"{name} {last_name}"


from pyflink.common.typeinfo import Types


if __name__ == "__main__":
    topic = "my-topic"
    pulsar_service_url = "pulsar://pulsar:6650"
    pulsar_admin_url = "http://pulsar:8080"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable("/usr/local/bin/python")
    # env.set_python_requirements(
    #         requirements_file_path="file:///home/pyflink/requirements.txt",
    #     requirements_cache_dir="cache_dir",
    # )
    env.add_jars(
        "file:///home/flink-jars/flink-connector-pulsar-1.16.0.jar",
        "file:///home/flink-jars/flink-sql-connector-pulsar-1.15.1.1.jar",
        # "file:///home/flink-jars/flink-avro-1.16.0.jar", # Having this makes the job not getting submitted. Just hanging
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

    # avro_file_name = tempfile.mktemp(suffix=".avro", dir="")
    # _create_avro_file()
    # file_path = "file://{os.path.join(os.path.abspath(os.path.dirname(__file__)), 'EnrichedUser.avs')}"
    # env.create_input(AvroInputFormat(file_path, AvroSchema.parse_string(ENRICHED_USER_SCHEMA)))

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
        .build()
    )

    # TODO transform a received avro message into a new avro message and publish to results topic
    # TODO consume and process two streams

    ds = env.from_source(
        source=pulsar_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="pulsarUserSource",
    ).uid("pulsarUserSource")

    def enrich_user(data):
        enriched_user = UserEnriched(
            id=data["id"], name=data["name"], last_name=data["last_name"]
        )
        result = Row(id=enriched_user.id, full_name=enriched_user.full_name)
        return result

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
    destination_topic = "results"
    pulsar_sink = (
        PulsarSink.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_producer_name("pyflink_producer")
        .set_topics(destination_topic)
        .set_serialization_schema(
            PulsarSerializationSchema.flink_schema(
                # Topic with schema should be created beforehand
                AvroRowSerializationSchema(avro_schema_string=ENRICHED_USER_SCHEMA)
            )
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN)
        .set_config("pulsar.producer.maxPendingMessages", 1000)
        .set_properties({"pulsar.producer.batchingMaxMessages": "100"})
        .build()
    )

    # schema = AvroSchema.parse_string(current_schema)
    # avro_type_info = GenericRecordAvroTypeInfo(schema)
    ds.map(
        enrich_user,
        output_type=Types.ROW_NAMED(
            field_names=["id", "full_name"], field_types=[Types.INT(), Types.STRING()]
        ),
        # Error on invalid schema: Exception: Field names ['last_name', 'name'] not exist in ['id', 'full_name'].
        # Does it validate schema evolution? Shouldn't GenericRecordAvroTypeInfo or similar be used instead?
        # output_type=avro_type_info,
    ).sink_to(pulsar_sink).name("pulsar sink")

    env.execute("Sample messages processing")
