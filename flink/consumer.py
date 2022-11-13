import os
from pyflink.common import SimpleStringSchema, WatermarkStrategy
import tempfile

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

from pyflink.datastream.formats.avro import AvroRowDeserializationSchema, AvroRowSerializationSchema, AvroInputFormat, AvroSchema, GenericRecordAvroTypeInfo
from pyflink.table.types import DataTypes

from pyflink.datastream import MapFunction
from pyflink.java_gateway import get_gateway


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

from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.cnt_state.value()
        if cnt is None or cnt < 2:
            self.cnt_state.update(1 if cnt is None else cnt + 1)
            return value[0], value[1] + 1
        else:
            return value[0], value[1]

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
    avro_file_name = tempfile.mktemp(suffix='.avro', dir="")
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
        # .set_config("pulsar.source.enableAutoAcknowledgeMessage", True)
        .build()
    )

    # TODO transform a received avro message into a new avro message and publish to results topic
    # TODO consume and process two streams

    # def append_str(data):
    #     user = data
    #     return data

    # from io import BytesIO
    # from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder
    def enrich_user(data):
        # wb = BytesIO()
        # encoder = BinaryEncoder(wb)
        # writer = DatumWriter(schema)
        # writer.write({"id":data["id"],"name": data["name"],"last_name":data["last_name"]}, encoder)
        # return writer
        # result = User(id=data["id"], name=data["name"], last_name=data["last_name"])
        # result = f"id: {result.id}, name: {result.name}, last_name: {result.last_name}"
        # env.create_input()
        # return DataTypes.ROW()
        return data

    # def enrich_user(data):
    #     result = UserEnriched(id=data["id"], name=data["name"], last_name=data["last_name"])
    #     Row
    #     return result.__dict__

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
    current_schema = USER_SCHEMA
    # current_schema = ENRICHED_USER_SCHEMA

    destination_topic = "results"
    pulsar_sink = (
        PulsarSink.builder()
        .set_service_url(pulsar_service_url)
        .set_admin_url(pulsar_admin_url)
        .set_producer_name("pyflink_producer")
        .set_topics(destination_topic)
        .set_serialization_schema(
            # PulsarSerializationSchema.flink_schema(SimpleStringSchema())
            PulsarSerializationSchema.flink_schema(AvroRowSerializationSchema(avro_schema_string=current_schema))
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN)
        .set_config("pulsar.producer.maxPendingMessages", 1000)
        .set_properties({"pulsar.producer.batchingMaxMessages": "100"})
        .build()
    )

    # schema = AvroSchema.parse_string(ENRICHED_USER_SCHEMA)
    schema = AvroSchema.parse_string(current_schema)
    avro_type_info = GenericRecordAvroTypeInfo(schema)
    # ds.map(lambda e: e, output_type=avro_type_info).sink_to(pulsar_sink).name("pulsar sink")
    ds.map(enrich_user, output_type=avro_type_info).sink_to(pulsar_sink).name("pulsar sink")
    # ds.map(enrich_user).sink_to(pulsar_sink).name("pulsar sink")
    # ds.print().name("print")
    env.execute("Sample messages processing")
