from confluent_kafka import consumer, KafkaException
import os
import logging
import json
import time
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "capteur_data"
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'agrotrace-ingestion-consumer',
    'auto.offset.reset': 'earliest'
}
