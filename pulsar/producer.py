import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

for i in range(1):
    message = f"For flink hello-pulsar-{i}".encode('utf-8')
    producer.send(message)
    print(f"msg #{i} {message}")

client.close()
