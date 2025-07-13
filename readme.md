# ✈️ FlightRadar24 - ETL Pipeline avec CRON

## Objectif

Ce projet vise à construire un pipeline **ETL industrialisé**, **tolérant aux erreurs**, et **observable**, qui récupère toutes les **2 heures** les données de vol en temps réel depuis l’API [FlightRadar24](https://github.com/JeanExtreme002/FlightRadarAPI), les nettoie, les stocke au format **CSV** (nomenclature horodatée), puis lance une **analyse Spark** pour générer des **indicateurs métier** sur le trafic aérien mondial.

---

## ⚙️ Architecture du pipeline

```
+----------------------+     +----------------------+     +----------------------+
|     EXTRACT          | --> |     TRANSFORM        | --> |        LOAD          |
|  API FlightRadar24   |     |  Nettoyage, EDA      |     |  Parquet horodaté    |
+----------------------+     +----------------------+     +----------------------+

                          Orchestration toutes les 2h

                                 |
                                 v

                            +-------------+
                            |  PySpark    |
                            |  Analyses   |
                            +-------------+
```

---

## Structure du projet

```
.
├── etl/
│   ├── extract.py         # Récupération des vols depuis FlightRadar24
│   ├── transform.py       # Exploration et nettoyage des données
│   └── load.py            # Sauvegarde CSV horodatée
|   └── pipeline.py        # Orchestration ETL en script Python
│
├── .gitignore                # Fichiers ignorés par Git
|
|   
│
├── data/
│   └── rawzone/           # Données Parquet structurées : tech_year=YYYY/...
│
├── notebooks/
│   └── Flight_radar_ETL.ipynb    # Notebook principal pour l'affichage des résultats des analyses des indicateurs via PySpark
│
|── spark_analysis.py        # Script PySpark pour les analyses
|
├── README.md              # README
└── requirements.txt       # Dépendances (Airflow, pandas, FlightRadarAPI...)
└── conception.md          # Schéma d’architecture 

```

---

## Stack technique

- **Python** 3.9.5
- **FlightRadar24 API** (librairie `FlightRadarAPI`)
- **pandas** (EDA + nettoyage)
- **CRON Python** (orchestration via boucle infinie avec `sleep`)
- **Airflow** (orchestration toutes les 2h) ( Abondonné pour le moment)
- **Parquet** (stockage optimisé)  ( Abondonné pour le moment)
- **PySpark** (analyses distribuées)(3.5.0)
- **Logging Python** (observabilité)


## 🔁 Orchestration avec CRON Python (Job 2H)

Un script Python (`pipeline.py`) encapsule le pipeline complet et est lancé automatiquement toutes les **2 heures** via une boucle ou tâche planifiée (cronjob).  
Pour test local, on peut définir `sleep(120)` (2 minutes).

Exemple :
```bash
python pipeline.py  # Lancement unique
```
Ou dans une boucle infinie :

```
while True:
    run_pipeline()
    time.sleep(2 * 60 * 60)  # 2 heures
```
---

## 🔁 Orchestration via Airflow ( Pas utilisé pour le moment )

Le fichier `scheduler/flightradar_dag.py` définit un DAG Airflow déclenché toutes les **2 heures**, composé de 3 tâches :
- `extract_task` → `transform_task` → `load_task`

L'exécution du pipeline génère un fichier Parquet partitionné par :
```
data/rawzone/tech_year=YYYY/tech_month=YYYY-MM/tech_day=YYYY-MM-DD/
```
---

## Indicateurs métier calculés

Les fichiers générés sont analysés via **PySpark** dans `spark_analysis.py`.  
Voici les **indicateurs extraits** :

1. Compagnie avec le plus de vols en cours

2. Par continent, la compagnie avec le plus de vols régionaux

3. Vol en cours avec le trajet le plus long

4.  Moyenne des distances de vol par continent

5.  Constructeur d’avions avec le plus de vols actifs

6.  Top 3 modèles d’avions utilisés par pays de la compagnie

---

## Stockage horodaté

Les fichiers CSV sont sauvegardés dans la structure suivante :

```bash

Flights/rawzone/
  └── tech_year=2025/
        └── tech_month=2025-07/
              └── tech_day=2025-07-13/
                    └── flights_20250713122027.csv
```

## Tolérance aux erreurs

- Bloc try/except autour de chaque étape (extract, transform, load)
- Les messages apparaissent aussi dans le terminal ou le notebook
- Les erreurs Spark sont également loggées

## Comment exécuter le pipeline manuellement

```bash
# Lancer manuellement le pipeline ETL
python etl/pipeline.py

# Lancer l'analyse Spark
python spark_analysis.py
```

Ou depuis le notebook :

```bash

run_pipeline()
```
---

## Observabilité

- Les logs d’extraction, de transformation, de nettoyage et de sauvegarde sont disponibles à chaque run.
- **Les logs Airflow permettent une visibilité complète de l’exécution**. ( Ou les logs du Cronjob python)

---

## Fréquence de mise à jour

Le pipeline est déclenché **toutes les 2 heures** pour capter les nouvelles positions des vols, et maintenir des analyses **à jour en quasi temps réel**.

---

## Comment exécuter le pipeline manuellement

```bash
# Lancer manuellement l'ETL
python main.py

# Lancer les analyses PySpark
spark-submit notebooks/spark_analysis.py

## ou bien 
python spark_analysis.py
```
Ou bien lancer le notebook Flight_radar_ETL
---

## Améliorations possibles

- Utilisation d'un systéme de stockage en base de données ( postgre par ex) et remplacer le Cronjob Python par un orchestrateur comme Airflow
- Dashboard en live via **Grafana** ou **Tableau** 
- Monitoring via Grafana / Prometheus

---

## Logique d’exécution

1. CRON déclenche le job pipeline.py toutes les 2 heures

2. Le pipeline :

      - extrait les données de l’API

      - nettoie avec pandas

      - sauvegarde en CSV dans Flights/rawzone/...


3. Le script spark_analysis.py lit le dernier fichier CSV et affiche les résultats métiers


## Remarques

J’ai opté pour un cronjob Python temporaire à la place d’Airflow en raison de contraintes de compatibilité (notamment avec WSL).
Une version orchestrée Airflow sera proposée dans une future branche.

## Authored By me 

N'hésitez pas à me contacter sur  LinkedIn en cas de problème ou piste d'amélioration.