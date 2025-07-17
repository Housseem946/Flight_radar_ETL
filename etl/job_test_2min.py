####################
#                  #
# Authored BY : me #
#                  #  
####################

# Lancement du Cronjonb à fréquence de 2 min pour tester

import time
import logging
import traceback
from datetime import datetime

from pipeline import run_pipeline

log_file = "pipeline.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FlightRadarETL")

def run_pipeline_job():
    logger.info(" Lancement du job ETL")
    print("\n[⏳] Lancement du job ETL...")

    try:
        run_pipeline()  
        logger.info("Pipeline terminé avec succès.")
        print("Pipeline terminé avec succès.")
    except Exception as e:
        logger.error(f" Erreur dans le pipeline : {e}")
        logger.error(traceback.format_exc())
        print(f"Erreur dans le pipeline : {e}")

# Boucle de test : exécuter toutes les 2 minutes
for i in range(3):  
    print(f"\n===========  Lancement #{i+1} à {datetime.now().strftime('%H:%M:%S')} ===========")
    run_pipeline_job()
    print("\n ")
    print(f"[🕒] Prochain lancement dans 2 minutes...\n")
    time.sleep(2 * 60)
