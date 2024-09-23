from confluent_kafka.admin import NewTopic, AdminClient, KafkaException
import logging

url='172.25.0.12:9092'
# Configuration for Kafka AdminClient
conf = {
    'bootstrap.servers': url,
    'client.id': 'admin-client',
    'log.connection.close': False
}

# Initialize the AdminClient
admin_client = AdminClient(conf)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic(topic):
    try:
        fs = admin_client.create_topics([topic])
        for topic, future in fs.items():
            try:
                future.result()
                logger.info(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                logger.error(f"Failed to create topic '{topic}': {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

# Define the topic configuration
me = 'customer_Churn'
#me_error = me + 'error-topic'
num_partitions = 2
replication_factor = 1

# Create NewTopic objects with the desired configuration
new_topic = NewTopic(me, num_partitions=num_partitions, replication_factor=replication_factor)
# error_topic = NewTopic(me_error, num_partitions=num_partitions, replication_factor=replication_factor)

# Create topics
create_topic(new_topic)
# create_topic(error_topic)

# List existing topics
try:
    current_topics = admin_client.list_topics(timeout=10).topics
    existing_topics = set(current_topics.keys())
    logger.info(f"Existing topics: {existing_topics}")
except KafkaException as e:
    logger.error(f"Failed to list topics: {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred: {e}")


import csv
from confluent_kafka import Producer
import json
from datetime import datetime
import time



# Kafka Producer configuration
url='172.25.0.12:9092'
conf = {
    'bootstrap.servers': url,
    'client.id': 'admin-client',
    'log.connection.close': False, 
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',  # Change to 'earliest' to process all messages from the beginning
    'max.poll.interval.ms': 10000 
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_csv_to_kafka(file_path, topic):
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
           # row['timestamp'] = datetime.utcnow().isoformat()
            message = json.dumps(row)
            producer.produce(topic, key="1", value=message, callback=delivery_report)
            producer.poll(0)
            
            # Sleep for 1 seconds
            time.sleep(1)
    producer.flush()

csv_file_path = '/workspaces/finalproj/customer_churn_dataset.csv'
# create topic before from admin file
topic='customer_Churn'

produce_csv_to_kafka(csv_file_path, topic)
