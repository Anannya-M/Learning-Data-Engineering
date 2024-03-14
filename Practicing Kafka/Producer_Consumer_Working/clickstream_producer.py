import json
import time

from confluent_kafka import SerializingProducer
from faker import Faker
from kafka import KafkaProducer
from kafka_config import KafkaConfig

# Initialize Faker for generating fake data
fake = Faker()

# Listing a set of random urls
urls = []
for _ in range(20):
    url = fake.url()
    urls.append(url)

# Initializing the Kafka Producer and Kafka Topic
producer = SerializingProducer({'bootstrap.servers': KafkaConfig.BOOTSTRAP_SERVERS, })
kafka_topic = KafkaConfig.CLICKSTREAM_TOPIC

# Craeting a function for producing error messages while producing data to kafka server
def delivery_report(err, message):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {message.topic()} [{message.partition()}]')


def generate_clickstream_events():
    # Generate fake clickstream event
    for i in range(5):
        event = {
            'user_id': fake.uuid4(),  # Unique user identifier
            'page_url': fake.random_element(elements=urls),  # URL of the page visited
            'referrer': fake.random_element(elements=urls),  # Referrer URL
            'timestamp': fake.iso8601(),  # Timestamp of the event
            'action': fake.random_element(elements=('click', 'scroll', 'hover')),  # Type of action
        }

        # Producing the clickstream event to kafka server
        producer.produce(
            kafka_topic,
            key=event['user_id'],
            value=json.dumps(event),
            on_delivery=delivery_report
        )

        print(f'Produced {i}/5 data: {event}')
        producer.flush()
        time.sleep(1)

        # return event_json


# Running the clickstream event generation loop
if __name__ == "__main__":
    generate_clickstream_events()
