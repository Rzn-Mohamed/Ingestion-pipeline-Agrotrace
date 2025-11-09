"""
Orchestrator - Planification et exécution du pipeline ETL
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

from pipeline.bronze import BronzeExtractor
from pipeline.silver import SilverTransformer
from pipeline.gold import GoldLoader

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()


class ETLOrchestrator:
    """Orchestrateur du pipeline ETL de nettoyage de données"""
    
    def __init__(self):
        self.db_connection = None
        self.bronze_extractor = None
        self.silver_transformer = None
        self.gold_loader = None
        self.scheduler = BlockingScheduler()
    
    def connect_database(self):
        """Établit la connexion à la base de données"""
        try:
            self.db_connection = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                database=os.getenv("DB_NAME", "agrotrace_db"),
                user=os.getenv("DB_USER", "admin"),
                password=os.getenv("DB_PASSWORD", "password")
            )
            logger.info("Connexion à la base de données établie")
            
            # Initialiser les composants du pipeline
            self.bronze_extractor = BronzeExtractor(self.db_connection)
            self.silver_transformer = SilverTransformer()
            self.gold_loader = GoldLoader(self.db_connection)
            
            # Créer la table de données nettoyées
            self.gold_loader.create_clean_table()
            
        except Exception as e:
            logger.error(f"Erreur de connexion à la base de données: {e}")
            raise
    
    def run_etl_pipeline(self):
        """Exécute le pipeline ETL complet"""
        try:
            start_time = datetime.now()
            logger.info("=" * 60)
            logger.info(f"Démarrage du pipeline ETL - {start_time}")
            logger.info("=" * 60)
            
            # BRONZE: Extraction des données brutes
            raw_df = self.bronze_extractor.extract_raw_data(batch_size=1000)
            
            if raw_df is None or raw_df.empty:
                logger.info("Aucune donnée à traiter, fin du cycle")
                return
            
            # Sauvegarder les IDs pour la mise à jour ultérieure
            processed_ids = raw_df['id'].tolist()
            
            # SILVER: Nettoyage et transformation
            cleaned_df = self.silver_transformer.transform(raw_df)
            
            # GOLD: Chargement des données nettoyées
            loaded_count = self.gold_loader.load_clean_data(cleaned_df)
            
            # Marquer les données comme nettoyées
            updated_count = self.gold_loader.mark_as_cleaned(processed_ids)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info(f"Pipeline ETL terminé en {duration:.2f}s")
            logger.info(f"Enregistrements traités: {loaded_count}")
            logger.info(f"Enregistrements marqués: {updated_count}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du pipeline: {e}", exc_info=True)
    
    def start_scheduler(self):
        """Démarre le planificateur pour exécuter le pipeline toutes les 5 minutes"""
        logger.info("Démarrage du planificateur ETL")
        logger.info("Fréquence: Toutes les 5 minutes")
        
        # Ajouter le job au planificateur
        self.scheduler.add_job(
            func=self.run_etl_pipeline,
            trigger=IntervalTrigger(minutes=5),
            id='etl_cleaning_job',
            name='Nettoyage des données de capteurs',
            replace_existing=True
        )
        
        # Exécuter immédiatement au démarrage
        logger.info("Exécution initiale du pipeline...")
        self.run_etl_pipeline()
        
        # Démarrer le planificateur
        logger.info("Planificateur actif - En attente du prochain cycle")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Arrêt du planificateur demandé")
            self.cleanup()
    
    def cleanup(self):
        """Nettoie les ressources"""
        logger.info("Nettoyage des ressources...")
        
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Planificateur arrêté")
        
        if self.db_connection:
            self.db_connection.close()
            logger.info("Connexion à la base de données fermée")


def main():
    """Point d'entrée principal"""
    logger.info("🚀 Démarrage du Worker ETL - Nettoyage des Données de Capteurs")
    logger.info("=" * 60)
    
    orchestrator = ETLOrchestrator()
    
    try:
        orchestrator.connect_database()
        orchestrator.start_scheduler()
    except KeyboardInterrupt:
        logger.info("⚠️ Interruption par l'utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}", exc_info=True)
    finally:
        orchestrator.cleanup()
        logger.info("👋 Worker ETL arrêté")


if __name__ == "__main__":
    main()
