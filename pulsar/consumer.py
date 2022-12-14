import pulsar


client = pulsar.Client("pulsar://localhost:6650")
topic_to_listen_to = "results" # Results from flink
# topic_to_listen_to = "my-topic" # From pulsar

# https://pulsar.apache.org/docs/client-libraries-python/
class User(pulsar.schema.Record):
    id = pulsar.schema.Integer()
    name = pulsar.schema.String()
    last_name = pulsar.schema.String()

class UserEnriched(pulsar.schema.Record):
    id = pulsar.schema.Integer()
    full_name = pulsar.schema.String()


consumer = client.subscribe(
    topic=topic_to_listen_to,
    schema=pulsar.schema.schema_avro.AvroSchema(UserEnriched),
    subscription_name="my-pulsar-sub",
)
# Avro consumer
try:
    while True:
        msg = consumer.receive()
        print(msg.value())
        consumer.acknowledge(msg)
except KeyboardInterrupt:
    print("interrupted...")

client.close()

# Plain text consumer
# consumer = client.subscribe(topic_to_listen_to,
#                             subscription_name='my-sub')

# try:
#     while True:
#         msg = consumer.receive()
#         print(f"Received message from topic '{topic_to_listen_to}': '{msg.data()}'" )
#         consumer.acknowledge(msg)
# except KeyboardInterrupt:
#     print("interrupted...")
#     client.close()
