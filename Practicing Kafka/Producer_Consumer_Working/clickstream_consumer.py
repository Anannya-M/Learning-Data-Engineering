from kafka import KafkaConsumer
import json
from kafka_config import KafkaConfig
import pandas as pd


def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda res: json.loads(res.decode('utf-8')))
    return consumer


def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data


def main():
    # Fetch data from Kafka on aggregated turnout by location
    location_consumer = create_kafka_consumer(KafkaConfig.CLICKSTREAM_TOPIC)
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)

    return location_result


if __name__ == '__main__':
    data = main()
    print(data)
