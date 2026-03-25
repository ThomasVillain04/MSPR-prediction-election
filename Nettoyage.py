import pandas as pd
import os

# CONFIGURATION DES DOSSIERS

DOSSIER_INPUT = "input"
DOSSIER_OUTPUT = "output"

os.makedirs(DOSSIER_OUTPUT, exist_ok=True)

def nettoyage_generique():
    print("Lancement du nettoyage générique automatisé (CSV & Excel)...")
    
    # On vérifie si le dossier input existe, sinon on le crée pour éviter un plantage
    os.makedirs(DOSSIER_INPUT, exist_ok=True)
    
    # On liste tous les fichiers CSV et Excel dans input/
    fichiers = [f for f in os.listdir(DOSSIER_INPUT) if f.endswith(('.csv', '.xlsx', '.xls'))]
    
    if not fichiers:
        print(f"Aucun fichier CSV ou Excel trouvé dans '{DOSSIER_INPUT}'.")
        return

    # On boucle sur chaque fichier trouvé
    for nom_fichier in fichiers:
        chemin_fichier = os.path.join(DOSSIER_INPUT, nom_fichier)
        print(f"\nTraitement de : {nom_fichier}...")
        
        try:
            # 1. LECTURE INTELLIGENTE SELON L'EXTENSION
            if nom_fichier.endswith('.csv'):
                try:
                    df = pd.read_csv(chemin_fichier, sep=';')
                    if len(df.columns) == 1: 
                        df = pd.read_csv(chemin_fichier, sep=',')
                except Exception:
                     df = pd.read_csv(chemin_fichier, sep=',')
            else:
                # Si c'est un fichier Excel (.xlsx ou .xls)
                df = pd.read_excel(chemin_fichier)

            # 2. STANDARDISATION DES COLONNES
            # On s'assure que tout est en texte (str), en minuscules, et sans espaces invisibles
            df.columns = df.columns.astype(str).str.lower().str.strip()

            # 3. NETTOYAGE PUR (Doublons et lignes vides)
            taille_avant = len(df)
            df = df.dropna(how='all') # Supprime les lignes où TOUTES les colonnes sont vides
            df = df.drop_duplicates() # Supprime les lignes strictement identiques
            taille_apres = len(df)
            
            print(f"-> {taille_avant - taille_apres} ligne(s) 'poubelle' ou doublon(s) supprimée(s).")
            
            # 4. SAUVEGARDE UNIFORME EN CSV
            # On enlève la vieille extension (.xlsx, .csv) et on met _clean.csv
            nom_sans_extension = os.path.splitext(nom_fichier)[0]
            nom_sortie = f"{nom_sans_extension}_clean.csv"
            chemin_sortie = os.path.join(DOSSIER_OUTPUT, nom_sortie)
            
            # On exporte toujours en CSV avec une virgule standard
            df.to_csv(chemin_sortie, index=False)
            print(f"-> Converti et sauvegardé sous : {chemin_sortie}")
            
        except Exception as e:
            print(f"-> Erreur sur {nom_fichier} : {e}")

if __name__ == "__main__":
    nettoyage_generique()