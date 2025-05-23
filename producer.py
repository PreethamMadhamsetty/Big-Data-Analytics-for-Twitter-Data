from confluent_kafka import Producer, Consumer
import json
import time

def read_config():
    config = {
        'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'DQ3AX66NYROTNC44',
        'sasl.password': '8ZB7CLoh3pyhv61bYOxMfhQ+DvYLIOR/TbjlfOsaGbdxJJ4dGZ+NnsAXlvUADFfu',
        'session.timeout.ms': '45000'
    }
    return config


topic = "raw_tweets"


def main():
    config = read_config()

    # creates a new producer instance
    producer = Producer(config)

    with open('data/khub.json', 'r', encoding='utf-8') as file:
        tweets = json.load(file)

    # Iterate through tweets and produce them to Kafka
    for tweet in tweets:
        key = str(tweet['id'])
        value = json.dumps(tweet)
        producer.produce(topic, key=key, value=value)
        print(f"Produced message to topic {topic}: key = {key} value = {value}")
        time.sleep(1)

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()


# producer and consumer code here


main()
