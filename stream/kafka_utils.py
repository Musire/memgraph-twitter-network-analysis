from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep
import json

# instantiate admin client from KafkaAdminClient
# if error is NoBrokersAvailable deduct a retry then sleep for 1 sec
# if no more retries, raise error
def get_admin_client(ip, port):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=ip + ':' + port,
                client_id="test")
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            sleep(1)

# instantiate consumer from KafkaConsumer and run poll of consumer
# if no messages in the poll sleep 1 second and try again
# for every message within messages within the message pack items deserialize json object into dictionary
# if error if KeyboardInterrupt, then pass
def consumer(ip, port, topic, platform):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=ip + ':' + port,
                             auto_offset_reset='earliest',
                             group_id=None)
    try:
        while True:
            msg_pack = consumer.poll()
            if not msg_pack:
                sleep(1)
                continue
            for _, messages in msg_pack.items():
                for message in messages:
                    message = json.loads(message.value.decode('utf8'))
                    print(platform, " :", str(message))

    except KeyboardInterrupt:
        pass

# instantiate the previous method get_admin_client
# instantiate a new topic with 1 partition and 1 replica in the cluster
# have the admin client create the new topic object created
# if error equals TopicAlreadyExistsError , then pass
# print all topics from the admin client list of topics
def create_topic(ip, port, topic):
    admin_client = get_admin_client(ip, port)
    my_topic = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=my_topic, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    print("All topics:")
    print(admin_client.list_topics())


# instantiate a producer from KafkaProducer
# if error equals NoBrokersAvailable, try 30 times
# if by all retries still error equals NoBrokersAvailable raise error
# print message 'failed to connect to kafka' and sleep 1 sec
def create_kafka_producer(ip, port):
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=ip + ':' + port)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)


# instantiate the previously method create_kafka_producer
# producer sends a topic paried with json string from messages endonding in utf8
# producer will flush and sleep for the duration of stream_delay
# if error, print the error
def producer(ip, port, topic, generate, stream_delay):
    producer = create_kafka_producer(ip, port)
    message = generate()
    while True:
        try:
            producer.send(topic, json.dumps(next(message)).encode('utf8'))
            producer.flush()
            sleep(stream_delay)
        except Exception as e:
            print(f"Error: {e}")