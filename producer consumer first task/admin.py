from confluent_kafka import admin

# Configuration for the AdminClient
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094'}

# Create an instance of AdminClient
ac = admin.AdminClient(conf)

# Define topic name
me = 'AbdoMostafa6'
topic = me

# Create a NewTopic object
new_topic = admin.NewTopic(topic, num_partitions=4, replication_factor=3)

# Call create_topics to create the topic
res = ac.create_topics([new_topic])
res[topic].result()
# Wait for the operation to finish and check results
for topic, future in res.items():
    try:
        future.result()  # The result itself is None
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")