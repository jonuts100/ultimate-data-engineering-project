from kafka import KafkaProducer,  KafkaConsumer

# TEST CONNECTION
# from kafka import KafkaAdminClient
# client = KafkaAdminClient(bootstrap_servers='localhost:29092')
# print(client.list_topics())

servers = ['localhost:29092']
topic = 'test'
producer = KafkaProducer(bootstrap_servers=servers)
consumer = KafkaConsumer(topic, auto_offset_reset='earliest', bootstrap_servers=servers)


producer.send(topic, b'Hello world')
print("Message sent")

for msg in consumer:
    print(f"topic: {msg.topic} | message: {msg.value}")