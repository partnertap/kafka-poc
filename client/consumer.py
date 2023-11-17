#!/usr/bin/env python3

# Usage:
# pip install kafka-python
# ./consumer.py <my-topic>

# NOTE: only runs for 10 seconds as per consumer_timeout_ms

# See:
# - https://raw.githubusercontent.com/simplesteph/kafka-stack-docker-compose/master/zk-multiple-kafka-multiple.yml
# - https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05

import sys
import time

from confluent_kafka import Consumer

def main(args):
    try:
        topic = args[0]
    except Exception as ex:
        print("Failed to set topic")

    consumer = get_kafka_consumer(topic)
    subscribe(consumer)


def subscribe(consumer_instance):
    try:
        while True:
            msg = consumer_instance.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Encountered a consumer error: {msg.error()}")
                continue

            print(f"Topic: {msg.topic()} and partition: {msg.partition()}")
            print(f"Received message: {msg.value().decode('utf-8')} with offset: {msg.offset()}")

            time.sleep(2.5)
            #consumer_instance.close()
    except Exception as ex:
        print('Exception in subscribing.')
        print(str(ex))

def get_kafka_consumer(topic_name, servers=['localhost:9092']):
    _consumer = None

    conf = {
        'bootstrap.servers': ','.join(servers),
        'group.id': 'KfConsumer1',
        'security.protocol': 'PLAINTEXT',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        #'max.poll.records': 5,
        'heartbeat.interval.ms': 25000,
        'max.poll.interval.ms': 90000,
        'session.timeout.ms': 45000,
        #'ssl.keystore.password' : mysecret,
        #'ssl.keystore.location' : './certkey.p12'
    }

    try:
        _consumer = Consumer(conf)
        _consumer.subscribe([topic_name])
    except Exception as ex:
        print('Exception while connecting to Kafka.')
        print(str(ex))
    finally:
        return _consumer

if __name__ == "__main__":
    main(["test"])