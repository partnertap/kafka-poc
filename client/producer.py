#!/usr/bin/env python3

# Usage:
# pip install kafka-python
# ./producer.py <my-topic> <my-key> <my-message>

# See:
# - https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/master/zk-multiple-kafka-multiple.yml
# - https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05

from confluent_kafka import Producer

import json


def main(args):
    try:
        topic = args[0]
        key = args[1]
        message = read_json(filepath="../json/data_sample.json")

        print(message)

        producer = get_kafka_producer()
        publish(producer, topic, key, message)
    except Exception as ex:
        print("Failed to set topic, key, or message.")


def read_json(filepath: str) -> str:
    with open(filepath) as data_file:
        data = json.load(data_file)

        return json.dumps(data)


def publish(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.produce(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print(f"Publish Successful ({key}, {value}) -> {topic_name}")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def get_kafka_producer(servers=['localhost:9092']):
    _producer = None

    conf = {
        "bootstrap.servers": ",".join(servers),
        "security.protocol": "PLAINTEXT"
    }

    try:
        _producer = Producer(**conf)
    except Exception as ex:
        print('Exception while connecting to Kafka:')
        print(str(ex))
    finally:
        return _producer


if __name__ == "__main__":
    main(["read", "s3", "test_message"])
