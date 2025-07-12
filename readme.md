# ✈️ FlightRadar24 - ETL & Airflow Pipeline

## Objectif

Ce projet a pour but de construire un pipeline **ETL industrialisé**, tolérant aux erreurs et observable, qui récupère les données de vol en temps réel depuis l’API FlightRadar24 toutes les **2 heures**, les nettoie, les transforme, puis les stocke sous **format Parquet**.  
Ces données sont ensuite analysées via **PySpark** pour générer des **indicateurs métier** sur le trafic aérien mondial.

---

## ⚙️ Architecture du pipeline

```
+----------------------+     +----------------------+     +----------------------+
|     EXTRACT          | --> |     TRANSFORM        | --> |        LOAD          |
|  API FlightRadar24   |     |  Nettoyage, EDA      |     |  Parquet horodaté    |
+----------------------+     +----------------------+     +----------------------+

                          Orchestration toutes les 2h via Airflow

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
│   └── load.py            # Sauvegarde en Parquet (partitionné)
│
├── scheduler/
|   |
│   |__ flightradar_dag.py # DAG Airflow (run toutes les 2h)
|   |
│   └── dag_functions.py    # regroupe les fichiers d'etl
|   
│
├── data/
│   └── rawzone/           # Données Parquet structurées : tech_year=YYYY/...
│
├── notebooks/
│   └── spark_analysis.py  # Affichage des résultats des analyses des indicateurs via PySpark
│
├── README.md              # README
└── requirements.txt       # Dépendances (Airflow, pandas, FlightRadarAPI...)
```

---

## Stack technique

- **Python** 3.9.5
- **FlightRadar24 API** (librairie `FlightRadarAPI`)
- **pandas** (EDA + nettoyage)
- **Airflow** (orchestration toutes les 2h)
- **Parquet** (stockage optimisé)
- **PySpark** (analyses distribuées)(3.5.0)
- **Logging Python** (observabilité)

---

## 🔁 Orchestration via Airflow

Le fichier `scheduler/flightradar_dag.py` définit un DAG Airflow déclenché toutes les **2 heures**, composé de 3 tâches :
- `extract_task` → `transform_task` → `load_task`

L'exécution du pipeline génère un fichier Parquet partitionné par :
```
data/rawzone/tech_year=YYYY/tech_month=YYYY-MM/tech_day=YYYY-MM-DD/
```
---

## Indicateurs métier calculés

Les fichiers générés sont analysés via **PySpark** dans `notebooks/spark_analysis.py`.  
Voici les **indicateurs extraits** :

1. ** Compagnie avec le plus de vols en cours**
   > Ex : `La compagnie avec le plus de vols en cours est : Lufthansa (DLH) avec 92 vols.`

2. ** Par continent, la compagnie avec le plus de vols régionaux**
   > Ex : `En Europe, Air France a le plus de vols intra-continentaux.`

3. ** Vol en cours avec le trajet le plus long (en distance géographique)**
   > Ex : `Le vol le plus long actuellement est : SQ24 entre SIN → JFK (distance : 15 349 km)`

4. ** Moyenne de la longueur des vols régionaux par continent**
   > Ex : `En Asie, les vols régionaux ont une distance moyenne de 1520 km.`

5. ** Constructeur d’avions avec le plus de vols actifs**
   > Ex : `Le constructeur ayant le plus d’avions en vol est : Boeing`

6. ** Top 3 des modèles d’avions actifs par pays (compagnie)**
   > Ex : `En France : A320, B737, A321`

---

## Tolérance aux erreurs

- Chaque étape est encapsulée avec des `try/except` + logs.
- Fichier vide ou API indisponible = ETL arrêté proprement.
- Les erreurs sont tracées sans bloquer l’ensemble du DAG.

---

## Observabilité

- Les logs d’extraction, de transformation, de nettoyage et de sauvegarde sont disponibles à chaque run.
- **Les logs Airflow permettent une visibilité complète de l’exécution**.

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

---

## Améliorations possibles

- Utilisation d'un systéme de stockage en base de données ( postgre par ex)
- Dashboard en live via **Grafana** ou **Tableau** 

---

## Auteur By me 
