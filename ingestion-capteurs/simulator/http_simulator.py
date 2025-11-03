"""
HTTP Sensor Simulator for AgroTrace
Sends sensor data via HTTP POST to the ingestion service
"""

from datetime import datetime
import asyncio
import random
import requests
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sensor configuration
SENSOR_IDS = ["TEMP001", "HUM001", "SOIL001", "PH001", "LIGHT001"]
INGESTION_URL = "http://ingestion-service:8000/ingest"
SEND_INTERVAL = 3  # seconds between messages


def generate_sensor_data() -> dict:
    """Generate sensor data matching CapteurData model"""
    return {
        "capteur_id": random.choice(SENSOR_IDS),
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidite": round(random.uniform(30.0, 90.0), 2),
        "humidite_sol": round(random.uniform(20.0, 80.0), 2),
        "niveau_ph": round(random.uniform(5.5, 8.0), 2),
        "luminosite": round(random.uniform(0.0, 100000.0), 2)
    }


def send_data(data: dict) -> bool:
    """Send sensor data to ingestion service"""
    try:
        response = requests.post(
            INGESTION_URL,
            json=data,
            timeout=5
        )
        if response.status_code == 200:
            logger.info(f"✓ Envoyé: {data['capteur_id']} - {data['temperature']}°C")
            return True
        else:
            logger.error(f"✗ Erreur {response.status_code}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"✗ Impossible de se connecter: {e}")
        return False


def main():
    logger.info(f"Démarrage du simulateur - cible: {INGESTION_URL}")
    logger.info(f"Interval: {SEND_INTERVAL}s")
    
    message_count = 0
    while True:
        try:
            data = generate_sensor_data()
            send_data(data)
            message_count += 1
            
            asyncio.run(asyncio.sleep(SEND_INTERVAL))
        except KeyboardInterrupt:
            logger.info(f"\nArrêt après {message_count} messages")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Erreur: {e}")
            asyncio.run(asyncio.sleep(SEND_INTERVAL))


if __name__ == "__main__":
    main()
