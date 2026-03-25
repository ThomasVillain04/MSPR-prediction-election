import pandas as pd
import os
import glob

# CONFIGURATION DES DOSSIERS
DOSSIER_INPUT = "input"
DOSSIER_OUTPUT = "output"

os.makedirs(DOSSIER_OUTPUT, exist_ok=True)

def nettoyage_generique():
    print("Lancement du nettoyage générique automatisé...")
    
    # On cherche tous les fichiers .csv dans le dossier input/
    fichiers_csv = glob.glob(os.path.join(DOSSIER_INPUT, "*.csv"))
    
    if not fichiers_csv:
        print(f"Aucun fichier CSV trouvé dans '{DOSSIER_INPUT}'.")
        return

    # On boucle sur chaque fichier trouvé
    for chemin_fichier in fichiers_csv:
        nom_fichier = os.path.basename(chemin_fichier)
        print(f"\nTraitement de : {nom_fichier}...")
        
        try:
            # 1. Lecture intelligente
            try:
                df = pd.read_csv(chemin_fichier, sep=';')
                if len(df.columns) == 1: 
                    df = pd.read_csv(chemin_fichier, sep=',')
            except Exception:
                 df = pd.read_csv(chemin_fichier, sep=',')

            # 2. Standardisation des colonnes
            df.columns = df.columns.str.lower().str.strip()

            # 3. Nettoyage pur (doublons et lignes vides)
            taille_avant = len(df)
            df = df.dropna(how='all') # Supprime les lignes où TOUT est vide
            df = df.drop_duplicates() # Supprime les lignes strictement identiques
            taille_apres = len(df)
            
            print(f"-> {taille_avant - taille_apres} ligne(s) 'poubelle' supprimée(s).")
            
            # 4. Sauvegarde dans output/
            nom_sortie = nom_fichier.replace('.csv', '_clean.csv')
            chemin_sortie = os.path.join(DOSSIER_OUTPUT, nom_sortie)
            
            # On exporte avec une virgule standard pour la suite
            df.to_csv(chemin_sortie, index=False)
            print(f"->Sauvegardé sous : {chemin_sortie}")
            
        except Exception as e:
            print(f"->Erreur sur {nom_fichier} : {e}")

if __name__ == "__main__":
    nettoyage_generique()