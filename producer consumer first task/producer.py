from confluent_kafka import Producer
import uuid
me = "AbdoMostafa6"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094', 
        'client.id':me}

producer = Producer(conf)
topic = me
while True:
    producer.produce(topic, key = uuid.uuid4().bytes , value = input("please enter a value") )
    producer.flush()
    print("producer one message")
