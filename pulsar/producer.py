import pulsar

class User(pulsar.schema.Record):
    id = pulsar.schema.Integer()
    name = pulsar.schema.String()
    last_name = pulsar.schema.String()

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic', schema=pulsar.schema.schema_avro.AvroSchema(User))

for i in range(10):
    user = User(id=i, name=f"FirstName{i}", last_name=f"LastName{i}")
    producer.send(user)
    print(user)

client.close()