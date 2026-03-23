import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 1. SÉCURITÉ ET CONNEXION POSTGRESQL

load_dotenv() 

UTILISATEUR = "postgres"
MOT_DE_PASSE = os.getenv("admin") 
HOTE = "172.16.145.32"
PORT = "5432"
NOM_BDD = "DB1"

chaine_connexion = f"postgresql://{UTILISATEUR}:{MOT_DE_PASSE}@{HOTE}:{PORT}/{NOM_BDD}"
moteur = create_engine(chaine_connexion)

# 2. LIENS RAW GITHUB DES FICHIERS PROPRES

URL_PARTIS = "https://raw.githubusercontent.com/ThomasVillain04/MSPR-prediction-election/refs/heads/main/Parti_Politique_clean.csv"
URL_CANDIDATS = "https://raw.githubusercontent.com/ThomasVillain04/MSPR-prediction-election/refs/heads/main/Candidat_clean.csv"
URL_RESULTATS = "https://raw.githubusercontent.com/ThomasVillain04/MSPR-prediction-election/refs/heads/main/input/Resultats_Presidentielles_clean.csv"
URL_DONNEES = "https://raw.githubusercontent.com/ThomasVillain04/MSPR-prediction-election/refs/heads/main/Donnees_Angers_clean.csv"

def charger_donnees_propres():

    fichiers_a_charger = {
        'Parti_Politique': URL_PARTIS,
        'Candidat': URL_CANDIDATS,
        'Resultats_Presidentielles': URL_RESULTATS,
        'Donnees_Angers': URL_DONNEES
    }

    # 3. BOUCLE DE CHARGEMENT AUTOMATIQUE
    for nom_table, url_github in fichiers_a_charger.items():
        print(f"\nTéléchargement et insertion dans la table : '{nom_table}'...")
        try:
            df = pd.read_csv(url_github)

            df.to_sql(nom_table, moteur, if_exists='append', index=False)
            print(f"-> ✅ Succès : Données insérées dans {nom_table}.")
            
        except Exception as e:
             print(f"-> ❌ Erreur lors du traitement de {nom_table} : {e}")

# Lancement du script
if __name__ == "__main__":
    charger_donnees_propres()