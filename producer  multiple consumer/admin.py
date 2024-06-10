from confluent_kafka import admin
from confluent_kafka.admin import AdminClient, NewTopic

conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
}

ac=admin.AdminClient(conf)
me="AbdoMostafa14"
num_partitions = 3
replication_factor =3

new_topic = NewTopic(me, num_partitions=num_partitions, replication_factor=replication_factor)

res=ac.create_topics([new_topic])
res[me].result()
# metadata = ac.list_topics(topic=me, timeout=10)
# topic_metadata = metadata.topics.get(me)
# print("Number of partins created ",len(topic_metadata.partitions))
# Wait for the operation to finish and check results
for topic, future in res.items():
    try:
        future.result()  # The result itself is None
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")