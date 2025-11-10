"""
Script de test pour vérifier le pipeline ETL
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def check_database_status():
    """Vérifie l'état de la base de données et des tables"""
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "agrotrace_db"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "password")
        )
        
        cursor = conn.cursor()
        
        print("=" * 60)
        print("📊 STATUT DU PIPELINE ETL")
        print("=" * 60)
        
        # Compter les données brutes non nettoyées
        cursor.execute("""
            SELECT COUNT(*) FROM raw_capteur_data WHERE is_cleaned = FALSE
        """)
        uncleaned_count = cursor.fetchone()[0]
        print(f"\n🔴 Données brutes à nettoyer: {uncleaned_count}")
        
        # Compter les données brutes nettoyées
        cursor.execute("""
            SELECT COUNT(*) FROM raw_capteur_data WHERE is_cleaned = TRUE
        """)
        cleaned_count = cursor.fetchone()[0]
        print(f"✅ Données brutes nettoyées: {cleaned_count}")
        
        # Compter les données dans clean_sensor_data
        cursor.execute("""
            SELECT COUNT(*) FROM clean_sensor_data
        """)
        clean_count = cursor.fetchone()[0]
        print(f"🧹 Données dans clean_sensor_data: {clean_count}")
        
        # Dernières données nettoyées
        cursor.execute("""
            SELECT capteur_id, timestamp, processed_at
            FROM clean_sensor_data
            ORDER BY processed_at DESC
            LIMIT 5
        """)
        recent_cleaned = cursor.fetchall()
        
        if recent_cleaned:
            print(f"\n📋 Dernières données nettoyées:")
            for row in recent_cleaned:
                print(f"  • Capteur: {row[0]} | Timestamp: {row[1]} | Nettoyé le: {row[2]}")
        
        # Statistiques par capteur
        cursor.execute("""
            SELECT 
                capteur_id,
                COUNT(*) as total,
                AVG(temperature) as temp_moy,
                AVG(humidite) as hum_moy
            FROM clean_sensor_data
            GROUP BY capteur_id
            ORDER BY total DESC
        """)
        stats = cursor.fetchall()
        
        if stats:
            print(f"\n📈 Statistiques par capteur:")
            for row in stats:
                print(f"  • {row[0]}: {row[1]} mesures | Temp moy: {row[2]:.1f}°C | Hum moy: {row[3]:.1f}%")
        
        print("\n" + "=" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur: {e}")


if __name__ == "__main__":
    check_database_status()
