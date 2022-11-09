import pulsar

client = pulsar.Client('pulsar://localhost:6650')
topic_to_listen_to = "results" #"my-topic"
consumer = client.subscribe(topic_to_listen_to,
                            subscription_name='my-sub')

try:
    while True:
        msg = consumer.receive()
        print(f"Received message from topic '{topic_to_listen_to}': '{msg.data()}'" )
        consumer.acknowledge(msg)
except KeyboardInterrupt:
    print("interrupted...")
    client.close()
    

