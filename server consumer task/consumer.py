from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import random
import requests

me = "AbdoMostafa15"

conf = {
    'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
    'group.id': me,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)

def detect_object(id):
    return random.choice(['car', 'house', 'person'])

def msg_process(msg):
    # Assuming the message value is the ID of the object to detect
    object_id = msg.value().decode('utf-8')
    detected_object = detect_object(object_id)
    print(f"Received a new message with ID: {object_id}, detected object: {detected_object}")


    requests.put('http://127.0.0.1:5000/object/' + msg.value().decode(),
                 json={"object": detect_object(msg.value().decode())})
    print("message processed successfully")


running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        print("Subscribed to topics:", topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, [me])