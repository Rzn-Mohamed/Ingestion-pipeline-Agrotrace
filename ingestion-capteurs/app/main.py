from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from .models import CapteurData
from . import kafka_producer
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Démarrage de l'application...")
    kafka_producer.connect()
    yield
    # Shutdown
    logger.info("Arrêt de l'application...")
    kafka_producer.close()


app = FastAPI(
    title="Service d'Ingestion AgroTrace",
    description="Service pour ingérer les données des capteurs agricoles via Kafka",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Endpoint de vérification de l'état du service"""
    return {
        "status": "healthy",
        "service": "ingestion-capteurs",
        "kafka_connected": kafka_producer.is_connected()
    }


@app.post("/ingest")
async def ingest_data(data: CapteurData):
    try:
        logger.info(f"Réception des données du capteur {data.capteur_id}")
        kafka_producer.send_message(
            topic="capteur_data",
            message=data.model_dump_json()
        )
        logger.info(f"Données du capteur {data.capteur_id} envoyées à Kafka avec succès")
        return {
            "status": "success",
            "message": "Données ingérées avec succès",
            "capteur_id": data.capteur_id
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'ingestion des données: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'ingestion des données: {str(e)}")