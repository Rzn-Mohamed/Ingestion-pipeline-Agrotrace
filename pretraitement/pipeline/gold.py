"""
Gold Layer - Chargement des données nettoyées
"""

import psycopg2
import pandas as pd
from typing import List
import logging

logger = logging.getLogger(__name__)


class GoldLoader:
    """Chargement des données nettoyées dans la base de données"""
    
    def __init__(self, db_connection: psycopg2.extensions.connection):
        self.db_connection = db_connection
    
    def create_clean_table(self):
        """Crée la table clean_sensor_data si elle n'existe pas"""
        try:
            cursor = self.db_connection.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clean_sensor_data (
                    id SERIAL,
                    capteur_id VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    temperature DOUBLE PRECISION,
                    humidite DOUBLE PRECISION,
                    humidite_sol DOUBLE PRECISION,
                    niveau_ph DOUBLE PRECISION,
                    luminosite DOUBLE PRECISION,
                    processed_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (capteur_id, timestamp)
                );
            """)
            
            # Hypertable pour optimisation temporelle
            cursor.execute("""
                SELECT create_hypertable('clean_sensor_data', 'timestamp', 
                                          if_not_exists => TRUE);
            """)
            
            self.db_connection.commit()
            logger.info("Table clean_sensor_data créée/vérifiée")
            cursor.close()
            
        except Exception as e:
            logger.error(f"Erreur lors de la création de la table: {e}")
            self.db_connection.rollback()
            raise
    
    def load_clean_data(self, df: pd.DataFrame) -> int:
        """
        Insère les données nettoyées dans clean_sensor_data
        
        Args:
            df: DataFrame nettoyé
            
        Returns:
            Nombre d'enregistrements insérés
        """
        if df is None or df.empty:
            return 0
        
        try:
            cursor = self.db_connection.cursor()
            
            insert_query = """
                INSERT INTO clean_sensor_data 
                (capteur_id, timestamp, temperature, humidite, humidite_sol, niveau_ph, luminosite)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (capteur_id, timestamp) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidite = EXCLUDED.humidite,
                    humidite_sol = EXCLUDED.humidite_sol,
                    niveau_ph = EXCLUDED.niveau_ph,
                    luminosite = EXCLUDED.luminosite,
                    processed_at = NOW();
            """
            
            # Insertion en batch
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['capteur_id'],
                    row['timestamp'],
                    row['temperature'],
                    row['humidite'],
                    row['humidite_sol'],
                    row['niveau_ph'],
                    row['luminosite']
                ))
            
            cursor.executemany(insert_query, records)
            self.db_connection.commit()
            
            logger.info(f"{len(records)} enregistrements chargés dans clean_sensor_data")
            cursor.close()
            
            return len(records)
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des données: {e}")
            self.db_connection.rollback()
            raise
    
    def mark_as_cleaned(self, ids: List[int]) -> int:
        """
        Marque les enregistrements comme nettoyés dans raw_capteur_data
        
        Args:
            ids: Liste des IDs à marquer
            
        Returns:
            Nombre d'enregistrements mis à jour
        """
        if not ids:
            return 0
        
        try:
            cursor = self.db_connection.cursor()
            
            update_query = """
                UPDATE raw_capteur_data
                SET is_cleaned = TRUE
                WHERE id = ANY(%s)
            """
            
            cursor.execute(update_query, (ids,))
            self.db_connection.commit()
            
            updated_count = cursor.rowcount
            logger.info(f"{updated_count} enregistrements marqués comme nettoyés")
            cursor.close()
            
            return updated_count
            
        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour du statut: {e}")
            self.db_connection.rollback()
            raise
