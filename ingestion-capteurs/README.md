# 📡 IngestionCapteurs - Microservice de Collecte de Données IoT

## 🎯 Vue d'ensemble

**IngestionCapteurs** est le premier microservice de la plateforme Agrotrace. Il est responsable de la collecte, validation et harmonisation des données provenant des capteurs IoT agricoles et des stations météorologiques.

### Rôle Principal
Collecter les données en temps réel issues des capteurs IoT (humidité du sol, température, luminosité, pH) et des stations météorologiques, puis les diffuser vers les autres microservices de la plateforme.

### Fonctionnalités Clés
- ✅ **Ingestion de données** : Réception de flux JSON via API REST
- ✅ **Validation et harmonisation** : Vérification de la cohérence des données
- ✅ **Diffusion temps réel** : Publication des données vers Kafka
- ✅ **Stockage temporel** : Persistance dans TimescaleDB
- ✅ **Traçabilité** : Horodatage précis et identification des capteurs

---

## 🏗️ Architecture

![Architecture du microservice IngestionCapteurs](micro-service1.png)

Le microservice suit le flux suivant :
1. **Capteurs IoT** envoient des données JSON via HTTP
2. **FastAPI** reçoit et valide les données avec Pydantic
3. Les données sont stockées dans **TimescaleDB** pour l'historique
4. **Kafka Producer** publie les données vers Kafka pour le traitement temps réel
5. Le microservice de **Prétraitement** consomme les messages Kafka

---

## 📊 Modèle de Données

### Structure CapteurData

```json
{
  "capteur_id": "SENSOR_001",
  "timestamp": "2025-11-01T14:30:00Z",
  "temperature": 22.5,
  "humidite": 65.0,
  "humidite_sol": 45.0,
  "niveau_ph": 6.8,
  "luminosite": 850.0
}
```

### Champs

| Champ | Type | Obligatoire | Description |
|-------|------|-------------|-------------|
| `capteur_id` | string | ✅ | Identifiant unique du capteur |
| `timestamp` | datetime | ✅ | Horodatage de la mesure (ISO 8601) |
| `temperature` | float | ❌ | Température en °C |
| `humidite` | float | ❌ | Humidité de l'air en % |
| `humidite_sol` | float | ❌ | Humidité du sol en % |
| `niveau_ph` | float | ❌ | Niveau de pH du sol (0-14) |
| `luminosite` | float | ❌ | Luminosité en lux |

---

## 🚀 Installation et Démarrage

### Prérequis

- Docker & Docker Compose
- Python 3.9+
- Git

### 1. Cloner le Repository

```bash
git clone https://github.com/Rzn-Mohamed/Ingestion-pipeline-Agrotrace.git
cd Ingestion-pipeline-Agrotrace
```

### 2. Démarrer l'Infrastructure

```bash
# Démarrer Kafka, Zookeeper et TimescaleDB
docker-compose up -d
```

Vérifier que les services sont actifs :
```bash
docker-compose ps
```

### 3. Configuration de l'Environnement

Créer un fichier `.env` dans le dossier `ingestion-capteurs/` :

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=capteurs-data

# TimescaleDB Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=agrotrace_db
DB_USER=admin
DB_PASSWORD=password

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

### 4. Installer les Dépendances

```bash
cd ingestion-capteurs
python -m venv env
.\env\Scripts\Activate.ps1  # Windows PowerShell
pip install -r requirements.txt
```

### 5. Lancer le Microservice

```bash
# Mode développement
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Ou avec Docker
docker build -t ingestion-capteurs .
docker run -p 8000:8000 ingestion-capteurs
```

---

## 📡 API REST

### Base URL
```
http://localhost:8000
```

### Endpoints

#### 1. Health Check
```http
GET /health
```

**Réponse :**
```json
{
  "status": "healthy",
  "service": "IngestionCapteurs",
  "timestamp": "2025-11-01T14:30:00Z"
}
```

#### 2. Ingérer des Données de Capteur
```http
POST /api/v1/capteurs/ingest
Content-Type: application/json
```

**Corps de la requête :**
```json
{
  "capteur_id": "SENSOR_001",
  "timestamp": "2025-11-01T14:30:00Z",
  "temperature": 22.5,
  "humidite": 65.0,
  "humidite_sol": 45.0,
  "niveau_ph": 6.8,
  "luminosite": 850.0
}
```

**Réponse (201 Created) :**
```json
{
  "status": "success",
  "message": "Données ingérées avec succès",
  "capteur_id": "SENSOR_001",
  "timestamp": "2025-11-01T14:30:00Z"
}
```

#### 3. Ingestion par Lot
```http
POST /api/v1/capteurs/ingest/batch
Content-Type: application/json
```

**Corps de la requête :**
```json
{
  "data": [
    {
      "capteur_id": "SENSOR_001",
      "timestamp": "2025-11-01T14:30:00Z",
      "temperature": 22.5,
      "humidite_sol": 45.0
    },
    {
      "capteur_id": "SENSOR_002",
      "timestamp": "2025-11-01T14:30:05Z",
      "temperature": 23.0,
      "luminosite": 900.0
    }
  ]
}
```

#### 4. Récupérer les Données d'un Capteur
```http
GET /api/v1/capteurs/{capteur_id}/data?start_date=2025-11-01&end_date=2025-11-02
```

**Paramètres :**
- `capteur_id` : Identifiant du capteur
- `start_date` : Date de début (YYYY-MM-DD)
- `end_date` : Date de fin (YYYY-MM-DD)

---

## 🧪 Simulation de Capteurs IoT

Un script de simulation est fourni pour générer des données réalistes de capteurs.

### Utilisation du Simulateur

```bash
cd ingestion-capteurs
python simulator/iot_simulator.py
```

### Options du Simulateur

```bash
# Simuler 5 capteurs envoyant des données toutes les 10 secondes
python simulator/iot_simulator.py --capteurs 5 --interval 10

# Mode batch : envoyer 100 mesures par capteur
python simulator/iot_simulator.py --capteurs 3 --mode batch --count 100

# Avec des paramètres personnalisés
python simulator/iot_simulator.py --capteurs 2 --temp-min 15 --temp-max 30 --interval 5
```

### Paramètres Disponibles

| Paramètre | Description | Défaut |
|-----------|-------------|--------|
| `--capteurs` | Nombre de capteurs à simuler | 3 |
| `--interval` | Intervalle entre envois (secondes) | 15 |
| `--mode` | Mode : `realtime` ou `batch` | realtime |
| `--count` | Nombre de mesures en mode batch | 50 |
| `--temp-min` | Température minimale (°C) | 10 |
| `--temp-max` | Température maximale (°C) | 35 |
| `--api-url` | URL de l'API | http://localhost:8000 |

---

## 🗄️ Base de Données TimescaleDB

### Initialisation de la Base

```sql
-- Connexion à la base
psql -h localhost -p 5432 -U admin -d agrotrace_db

-- Créer l'extension TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Créer la table des données capteurs
CREATE TABLE capteurs_data (
    time TIMESTAMPTZ NOT NULL,
    capteur_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidite DOUBLE PRECISION,
    humidite_sol DOUBLE PRECISION,
    niveau_ph DOUBLE PRECISION,
    luminosite DOUBLE PRECISION
);

-- Convertir en hypertable (optimisée pour les séries temporelles)
SELECT create_hypertable('capteurs_data', 'time');

-- Créer un index sur capteur_id
CREATE INDEX idx_capteur_id ON capteurs_data (capteur_id, time DESC);
```

### Requêtes Utiles

```sql
-- Données des dernières 24 heures
SELECT * FROM capteurs_data
WHERE time > NOW() - INTERVAL '24 hours'
ORDER BY time DESC;

-- Moyenne horaire par capteur
SELECT 
    time_bucket('1 hour', time) AS hour,
    capteur_id,
    AVG(temperature) as temp_moy,
    AVG(humidite_sol) as hum_sol_moy
FROM capteurs_data
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY hour, capteur_id
ORDER BY hour DESC;
```

---

## 🔧 Technologies Utilisées

| Technologie | Version | Rôle |
|-------------|---------|------|
| **FastAPI** | 0.120+ | Framework API REST |
| **Uvicorn** | 0.38+ | Serveur ASGI |
| **Pydantic** | 2.12+ | Validation des données |
| **Confluent Kafka** | 2.12+ | Client Kafka Python |
| **Psycopg2** | 2.9+ | Driver PostgreSQL |
| **TimescaleDB** | Latest | Base de données temporelle |
| **Apache Kafka** | 7.3+ | Broker de messages |
| **Docker** | - | Conteneurisation |

---

## 📈 Kafka Topics

### Topic Principal : `capteurs-data`

**Format des messages :**
```json
{
  "capteur_id": "SENSOR_001",
  "timestamp": "2025-11-01T14:30:00Z",
  "data": {
    "temperature": 22.5,
    "humidite": 65.0,
    "humidite_sol": 45.0,
    "niveau_ph": 6.8,
    "luminosite": 850.0
  }
}
```

### Commandes Kafka Utiles

```bash
# Lister les topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Créer le topic
docker exec -it kafka kafka-topics --create --bootstrap-server localhost:9092 \
  --topic capteurs-data --partitions 3 --replication-factor 1

# Consommer les messages (debug)
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic capteurs-data --from-beginning
```

---

## 🔍 Monitoring et Logs

### Logs du Microservice

```bash
# Logs en temps réel
docker logs -f ingestion-capteurs

# Dernières 100 lignes
docker logs --tail 100 ingestion-capteurs
```

### Métriques Disponibles

```http
GET /metrics
```

Retourne :
- Nombre de requêtes traitées
- Latence moyenne
- Erreurs de validation
- Messages Kafka publiés

---

## 🧪 Tests

### Tests Unitaires

```bash
# Installer les dépendances de test
pip install pytest pytest-asyncio httpx

# Lancer les tests
pytest tests/ -v

# Avec couverture
pytest tests/ --cov=app --cov-report=html
```

### Tests d'Intégration

```bash
# Tester l'API
curl -X POST http://localhost:8000/api/v1/capteurs/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "capteur_id": "TEST_001",
    "timestamp": "2025-11-01T14:30:00Z",
    "temperature": 22.5,
    "humidite_sol": 45.0
  }'
```

---

## 🚨 Gestion des Erreurs

### Codes d'Erreur

| Code | Description |
|------|-------------|
| 400 | Données invalides ou manquantes |
| 500 | Erreur interne du serveur |
| 503 | Service temporairement indisponible (Kafka/DB) |

### Exemple de Réponse d'Erreur

```json
{
  "error": "ValidationError",
  "message": "Le champ 'capteur_id' est obligatoire",
  "details": {
    "field": "capteur_id",
    "received": null
  },
  "timestamp": "2025-11-01T14:30:00Z"
}
```

---

## 📝 Bonnes Pratiques

### Validation des Données
- ✅ Toujours valider les timestamps (format ISO 8601)
- ✅ Vérifier les plages de valeurs (température, pH, etc.)
- ✅ Rejeter les données avec `capteur_id` manquant

### Performance
- 🚀 Utiliser l'endpoint `/batch` pour les envois multiples
- 🚀 Limiter la fréquence d'envoi (recommandé : 1 mesure/15 secondes)
- 🚀 Configurer les pools de connexion DB correctement

### Sécurité
- 🔒 Utiliser HTTPS en production
- 🔒 Implémenter l'authentification API (JWT tokens)
- 🔒 Valider et nettoyer toutes les entrées

---

## 🔄 Workflow de Traitement

```
1. Réception de données (API REST)
           ↓
2. Validation Pydantic
           ↓
3. Enrichissement (timestamp serveur si manquant)
           ↓
4. Vérification de cohérence
           ↓
5. Stockage TimescaleDB (asynchrone)
           ↓
6. Publication Kafka
           ↓
7. Transmission au microservice de Prétraitement
```

---

## 🤝 Contribution

Ce microservice fait partie du projet Agrotrace. Pour contribuer :

1. Fork le repository
2. Créer une branche feature (`git checkout -b feature/amelioration`)
3. Commit les changements (`git commit -m 'Ajout fonctionnalité X'`)
4. Push vers la branche (`git push origin feature/amelioration`)
5. Ouvrir une Pull Request

---

## 📞 Support

Pour toute question ou problème :
- 📧 Email : support@agrotrace.com
- 📖 Documentation : [Wiki du projet](https://github.com/Rzn-Mohamed/Ingestion-pipeline-Agrotrace/wiki)
- 🐛 Issues : [GitHub Issues](https://github.com/Rzn-Mohamed/Ingestion-pipeline-Agrotrace/issues)

---

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.


---

**Version :** 1.0.0  
**Dernière mise à jour :** Novembre 2025  
**Auteur :** Rzn-Mohamed
