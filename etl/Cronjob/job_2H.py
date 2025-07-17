####################
#                  #
# Authored BY : me #
#                  #  
####################

# Lancement d'un Job chaque 2 heures ( while True: pour exécution infinie )

import time

from pipeline import run_pipeline

while True:
    try:
        run_pipeline()
        print(" Pipeline exécutée avec succès.")
    except Exception as e:
        print(f" Erreur : {e}")
    print("\n ")
    print("⏳ En attente de 2h...")
    print("\n ")
    time.sleep(2 * 60 * 60)
