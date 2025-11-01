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

# Expose le port sur lequel l'application va tourner
EXPOSE 8000

# Variables d'environnement par défaut
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Commande pour démarrer l'application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
