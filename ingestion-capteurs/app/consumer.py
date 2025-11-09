from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
from psycopg2.extras import execute_values
import json
import os
import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "agrotrace-consumer-group")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "capteur_data")

# Configuration PostgreSQL/TimescaleDB
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "agrotrace_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Commit manuel pour plus de contrôle
    'max.poll.interval.ms': 300000,  # 5 minutes
    'session.timeout.ms': 10000
}

consumer: Optional[Consumer] = None
db_connection: Optional[psycopg2.extensions.connection] = None


def connect_to_database(max_retries: int = 5, retry_delay: int = 5) -> psycopg2.extensions.connection:
    """
    Établit la connexion à TimescaleDB avec retry logic
    
    Args:
        max_retries: Nombre maximum de tentatives de connexion
        retry_delay: Délai en secondes entre chaque tentative
    
    Returns:
        Connection PostgreSQL
    """
    for attempt in range(max_retries):
        try:
            connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info(f"Connecté à TimescaleDB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return connection
        except psycopg2.OperationalError as e:
            logger.error(f"Tentative {attempt + 1}/{max_retries} - Échec de connexion à la base de données: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Nouvelle tentative dans {retry_delay} secondes...")
                time.sleep(retry_delay)
            else:
                logger.critical("Impossible de se connecter à la base de données après plusieurs tentatives")
                raise


def create_table_if_not_exists() -> None:
    """
    Crée la table des capteurs si elle n'existe pas
    et active TimescaleDB sur cette table
    """
    global db_connection
    
    try:
        cursor = db_connection.cursor()
        
        # Créer la table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_capteur_data (
                id SERIAL,
                capteur_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                temperature DOUBLE PRECISION,
                humidite DOUBLE PRECISION,
                humidite_sol DOUBLE PRECISION,
                niveau_ph DOUBLE PRECISION,
                luminosite DOUBLE PRECISION,
                is_cleaned BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (capteur_id, timestamp)
            );
        """)
        
        # Convertir en hypertable TimescaleDB si ce n'est pas déjà fait
        cursor.execute("""
            SELECT create_hypertable('raw_capteur_data', 'timestamp', 
                                      if_not_exists => TRUE);
        """)
        
        db_connection.commit()
        logger.info("Table 'raw_capteur_data' créée/vérifiée avec succès")
        cursor.close()
        
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table: {e}")
        db_connection.rollback()
        raise


def insert_capteur_data(data: Dict[str, Any]) -> bool:
    """
    Insère les données d'un capteur dans la base de données
    
    Args:
        data: Dictionnaire contenant les données du capteur
    
    Returns:
        True si l'insertion a réussi, False sinon
    """
    global db_connection
    
    try:
        cursor = db_connection.cursor()
        
        insert_query = """
            INSERT INTO raw_capteur_data 
            (capteur_id, timestamp, temperature, humidite, humidite_sol, niveau_ph, luminosite, is_cleaned)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (capteur_id, timestamp) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidite = EXCLUDED.humidite,
                humidite_sol = EXCLUDED.humidite_sol,
                niveau_ph = EXCLUDED.niveau_ph,
                luminosite = EXCLUDED.luminosite,
                is_cleaned = EXCLUDED.is_cleaned;
        """
        
        cursor.execute(insert_query, (
            data.get('capteur_id'),
            data.get('timestamp'),
            data.get('temperature'),
            data.get('humidite'),
            data.get('humidite_sol'),
            data.get('niveau_ph'),
            data.get('luminosite'),
            False  
        ))
        
        db_connection.commit()
        cursor.close()
        logger.debug(f"Données du capteur {data.get('capteur_id')} insérées avec succès")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des données: {e}")
        db_connection.rollback()
        return False


def process_message(message_value: str) -> bool:
    """
    Traite un message Kafka et l'insère dans la base de données
    
    Args:
        message_value: Valeur du message (JSON string)
    
    Returns:
        True si le traitement a réussi, False sinon
    """
    try:
        data = json.loads(message_value)
        logger.info(f"Traitement du message pour le capteur: {data.get('capteur_id')}")
        
        # Validation basique
        if not data.get('capteur_id') or not data.get('timestamp'):
            logger.warning("Message invalide: capteur_id ou timestamp manquant")
            return False
        
        # Insertion dans la base de données
        return insert_capteur_data(data)
        
    except json.JSONDecodeError as e:
        logger.error(f"Erreur de décodage JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Erreur lors du traitement du message: {e}")
        return False


def connect_consumer(max_retries: int = 5, retry_delay: int = 5) -> Consumer:
    """
    Établit la connexion au consumer Kafka avec retry logic
    
    Args:
        max_retries: Nombre maximum de tentatives de connexion
        retry_delay: Délai en secondes entre chaque tentative
    
    Returns:
        Consumer Kafka
    """
    for attempt in range(max_retries):
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([KAFKA_TOPIC])
            logger.info(f"Consumer connecté à Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
            return consumer
        except KafkaException as e:
            logger.error(f"Tentative {attempt + 1}/{max_retries} - Échec de connexion Kafka consumer: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Nouvelle tentative dans {retry_delay} secondes...")
                time.sleep(retry_delay)
            else:
                logger.critical("Impossible de se connecter au consumer Kafka après plusieurs tentatives")
                raise


def start_consuming() -> None:
    """
    Démarre la boucle de consommation des messages Kafka
    """
    global consumer, db_connection
    
    logger.info("Démarrage du consumer...")
    
    # Connexion à la base de données
    db_connection = connect_to_database()
    create_table_if_not_exists()
    
    # Connexion au consumer Kafka
    consumer = connect_consumer()
    
    try:
        logger.info("Consumer en attente de messages...")
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Fin de partition atteinte: {msg.partition()}")
                else:
                    logger.error(f"Erreur consumer: {msg.error()}")
                continue
            
            # Traiter le message
            message_value = msg.value().decode('utf-8')
            logger.debug(f"Message reçu: offset={msg.offset()}, partition={msg.partition()}")
            
            if process_message(message_value):
                # Commit seulement si le traitement a réussi
                consumer.commit(msg)
                logger.debug("Message committé avec succès")
            else:
                logger.warning(f"Échec du traitement du message, offset: {msg.offset()}")
                # Option: implémenter une logique de dead letter queue ici
                
    except KeyboardInterrupt:
        logger.info("Interruption par l'utilisateur...")
    except Exception as e:
        logger.critical(f"Erreur fatale dans la boucle de consommation: {e}")
    finally:
        cleanup()


def cleanup() -> None:
    """
    Nettoie les ressources (connexions Kafka et DB)
    """
    global consumer, db_connection
    
    logger.info("Nettoyage des ressources...")
    
    if consumer is not None:
        consumer.close()
        logger.info("Consumer Kafka fermé")
    
    if db_connection is not None:
        db_connection.close()
        logger.info("Connexion à la base de données fermée")


if __name__ == "__main__":
    # Configuration du logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=== Consumer AgroTrace démarré ===")
    start_consuming()