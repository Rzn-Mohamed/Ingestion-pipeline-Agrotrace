# Utilise une image Python officielle
FROM python:3.11-slim

# Définit le répertoire de travail
WORKDIR /app

# Copie les fichiers de dépendances
COPY requirements.txt .

# Installe les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copie le code de l'application
COPY ./app ./app

# Expose le port sur lequel l'application va tourner (pour le service API)
EXPOSE 8000

# Variables d'environnement par défaut
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# La commande par défaut sera surchargée dans docker-compose.yml
# pour différencier le service API du consumer
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
