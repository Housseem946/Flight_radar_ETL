FROM python:3.9-slim

# Crée un répertoire pour le projet
WORKDIR /app

# Copie les fichiers
COPY . /app

# Installe les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Commande par défaut : exécute ton script ETL
CMD ["python", "etl/pipeline.py"]
