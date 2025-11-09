"""
Silver Layer - Nettoyage et transformation des données
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Nettoyage et transformation des données de capteurs"""
    
    # Plages valides pour chaque métrique
    VALID_RANGES = {
        'temperature': (-10, 50),      # °C
        'humidite': (0, 100),          # %
        'humidite_sol': (0, 100),      # %
        'niveau_ph': (0, 14),          # pH
        'luminosite': (0, 150000)      # lux
    }
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie et transforme les données
        
        Args:
            df: DataFrame brut
            
        Returns:
            DataFrame nettoyé
        """
        if df is None or df.empty:
            return df
        
        logger.info(f"Début du nettoyage de {len(df)} enregistrements")
        
        # Copie pour éviter les modifications directes
        cleaned_df = df.copy()
        
        # 1. Remplir les valeurs manquantes par interpolation
        cleaned_df = self._fill_missing_values(cleaned_df)
        
        # 2. Corriger les anomalies avec clipping
        cleaned_df = self._fix_anomalies(cleaned_df)
        
        logger.info(f"Nettoyage terminé: {len(cleaned_df)} enregistrements")
        
        return cleaned_df
    
    def _fill_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remplit les valeurs manquantes par interpolation linéaire
        """
        numeric_cols = ['temperature', 'humidite', 'humidite_sol', 'niveau_ph', 'luminosite']
        
        for col in numeric_cols:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    # Interpolation linéaire, remplissage des bords avec forward/backward fill
                    df[col] = df[col].interpolate(method='linear', limit_direction='both')
                    logger.debug(f"{col}: {missing_count} valeurs manquantes interpolées")
        
        return df
    
    def _fix_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Corrige les anomalies en utilisant le clipping (écrêtage)
        """
        for col, (min_val, max_val) in self.VALID_RANGES.items():
            if col in df.columns:
                # Compter les anomalies avant correction
                anomalies = ((df[col] < min_val) | (df[col] > max_val)).sum()
                
                if anomalies > 0:
                    # Appliquer le clipping
                    df[col] = df[col].clip(lower=min_val, upper=max_val)
                    logger.debug(f"{col}: {anomalies} anomalies corrigées par clipping")
        
        return df
