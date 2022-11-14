from time import sleep
import pulsar
# https://pulsar.apache.org/docs/client-libraries-python/
class User(pulsar.schema.Record):
    id = pulsar.schema.Integer()
    name = pulsar.schema.String()
    last_name = pulsar.schema.String()

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic', schema=pulsar.schema.schema_avro.AvroSchema(User))

for i in range(10):
    user = User(id=i, name=f"FirstName Flinker Changed{i}", last_name=f"LastName{i}")
    producer.send(content=user)
    print(user)
    sleep(2)

client.close()
