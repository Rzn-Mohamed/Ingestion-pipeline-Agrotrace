"""
Bronze Layer - Extraction des données brutes depuis TimescaleDB
"""

import psycopg2
import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class BronzeExtractor:
    """Extraction des données brutes non nettoyées"""
    
    def __init__(self, db_connection: psycopg2.extensions.connection):
        self.db_connection = db_connection
    
    def extract_raw_data(self, batch_size: int = 1000) -> Optional[pd.DataFrame]:
        """
        Extrait les données non nettoyées de la table raw_capteur_data
        
        Args:
            batch_size: Nombre maximum de lignes à extraire
            
        Returns:
            DataFrame avec les données brutes ou None si aucune donnée
        """
        query = """
            SELECT 
                id,
                capteur_id,
                timestamp,
                temperature,
                humidite,
                humidite_sol,
                niveau_ph,
                luminosite
            FROM raw_capteur_data
            WHERE is_cleaned = FALSE
            ORDER BY timestamp ASC
            LIMIT %s
        """
        
        try:
            df = pd.read_sql_query(query, self.db_connection, params=(batch_size,))
            
            if df.empty:
                logger.info("Aucune donnée à traiter")
                return None
            
            logger.info(f"Extraction de {len(df)} enregistrements bruts")
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction des données: {e}")
            raise
