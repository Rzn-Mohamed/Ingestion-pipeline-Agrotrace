from confluent_kafka import Producer, KafkaException
import os
import logging
import time

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'agrotrace-ingestion-producer',
    'acks': 'all',  # Attendre la confirmation de tous les réplicas
    'retries': 3,   # Nombre de tentatives en cas d'échec
    'enable.idempotence': True  # Éviter les doublons
}

producer = None


def connect(max_retries: int = 5, retry_delay: int = 5) -> None:
    """
    Établit la connexion au producer Kafka avec retry logic
    
    Args:
        max_retries: Nombre maximum de tentatives de connexion
        retry_delay: Délai en secondes entre chaque tentative
    """
    global producer
    
    for attempt in range(max_retries):
        try:
            producer = Producer(producer_config)
            logger.info(f"Connecté à Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
            return
        except KafkaException as e:
            logger.error(f"Tentative {attempt + 1}/{max_retries} - Échec de connexion à Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Nouvelle tentative dans {retry_delay} secondes...")
                time.sleep(retry_delay)
            else:
                logger.critical("Impossible de se connecter à Kafka après plusieurs tentatives")
                raise


def send_message(topic: str, message: str) -> None:
    """
    Envoie un message à Kafka avec confirmation de livraison
    
    Args:
        topic: Le topic Kafka de destination
        message: Le message à envoyer (JSON string)
    """
    if producer is None:
        raise Exception("Producer is not connected")
    
    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Échec de livraison du message: {err}")
        else:
            logger.debug(f"Message livré à {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    try:
        producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Déclenche les callbacks
        producer.flush(timeout=10)  # Assure la livraison du message
    except BufferError:
        logger.error("Buffer plein, impossible d'envoyer le message")
        producer.poll(1)  # Vide le buffer
        raise
    except KafkaException as e:
        logger.error(f"Erreur Kafka lors de l'envoi: {e}")
        raise


def is_connected() -> bool:
    """Vérifie si le producer est connecté"""
    return producer is not None


def close() -> None:
    """Ferme proprement la connexion Kafka"""
    global producer
    if producer is not None:
        logger.info("Fermeture du producer Kafka...")
        producer.flush(timeout=30)  # Attendre l'envoi de tous les messages en attente
        producer = None
        logger.info("Kafka Producer déconnecté.")