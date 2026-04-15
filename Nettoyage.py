import pandas as pd
import os
import chardet

# CONFIGURATION DES DOSSIERS

DOSSIER_INPUT = "input"
DOSSIER_OUTPUT = "output"

os.makedirs(DOSSIER_OUTPUT, exist_ok=True)


def detecter_encodage(chemin_fichier: str) -> str:
    """
    Détecte l'encodage d'un fichier en lisant ses octets bruts.
    Retourne l'encodage détecté (ex: 'utf-8', 'iso-8859-1', 'windows-1252'...).
    """
    with open(chemin_fichier, 'rb') as f:
        octets_bruts = f.read()
    resultat = chardet.detect(octets_bruts)
    encodage = resultat.get('encoding') or 'utf-8'
    confiance = resultat.get('confidence', 0)
    print(f"-> Encodage détecté : {encodage} (confiance : {confiance:.0%})")
    return encodage


def lire_csv_avec_encodage(chemin_fichier: str, encodage: str) -> pd.DataFrame:
    """
    Tente de lire un CSV avec l'encodage détecté, puis en fallback sur utf-8,
    et en dernier recours avec errors='replace' pour ne jamais bloquer.
    """
    for sep in [';', ',']:
        # Tentative 1 : encodage détecté
        try:
            df = pd.read_csv(chemin_fichier, sep=sep, encoding=encodage)
            if len(df.columns) > 1:
                print(f"-> Lu avec encodage '{encodage}' et séparateur '{sep}'.")
                return df
        except (UnicodeDecodeError, Exception):
            pass

        # Tentative 2 : utf-8 strict
        try:
            df = pd.read_csv(chemin_fichier, sep=sep, encoding='utf-8')
            if len(df.columns) > 1:
                print(f"-> Lu en utf-8 strict avec séparateur '{sep}'.")
                return df
        except (UnicodeDecodeError, Exception):
            pass

        # Tentative 3 : utf-8 avec remplacement des caractères illisibles
        try:
            df = pd.read_csv(chemin_fichier, sep=sep, encoding='utf-8', errors='replace')
            if len(df.columns) > 1:
                print(f"-> Lu en utf-8 (mode remplacement) avec séparateur '{sep}'.")
                return df
        except Exception:
            pass

    # Dernier recours : latin-1 (accepte tous les octets sans exception)
    df = pd.read_csv(chemin_fichier, sep=',', encoding='latin-1')
    print("-> Lu en latin-1 (dernier recours).")
    return df


def nettoyage_generique():
    print("Lancement du nettoyage générique automatisé (CSV & Excel)...")

    os.makedirs(DOSSIER_INPUT, exist_ok=True)

    fichiers = [f for f in os.listdir(DOSSIER_INPUT) if f.endswith(('.csv', '.xlsx', '.xls'))]

    if not fichiers:
        print(f"Aucun fichier CSV ou Excel trouvé dans '{DOSSIER_INPUT}'.")
        return

    for nom_fichier in fichiers:
        chemin_fichier = os.path.join(DOSSIER_INPUT, nom_fichier)
        print(f"\nTraitement de : {nom_fichier}...")

        try:
            # 1. DÉTECTION DE L'ENCODAGE (CSV uniquement, Excel gère ça en interne)
            if nom_fichier.endswith('.csv'):
                encodage_detecte = detecter_encodage(chemin_fichier)
                df = lire_csv_avec_encodage(chemin_fichier, encodage_detecte)
            else:
                df = pd.read_excel(chemin_fichier)
                print("-> Fichier Excel lu (encodage géré nativement).")

            # 2. STANDARDISATION DES COLONNES
            df.columns = df.columns.astype(str).str.lower().str.strip()

            # 3. NETTOYAGE DES VALEURS TEXTE : correction des caractères mal encodés résiduels
            # On passe par encode/decode pour éliminer les caractères illisibles persistants
            for col in df.select_dtypes(include='object').columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.encode('utf-8', errors='replace')   # encode en utf-8, remplace ce qui ne passe pas
                    .str.decode('utf-8', errors='replace')   # redécode proprement
                    .str.replace('\ufffd', '', regex=False)  # supprime les caractères de remplacement (U+FFFD)
                    .str.strip()
                )

            # 4. NETTOYAGE PUR (Doublons et lignes vides)
            taille_avant = len(df)
            df = df.dropna(how='all')
            df = df.drop_duplicates()
            taille_apres = len(df)

            print(f"-> {taille_avant - taille_apres} ligne(s) 'poubelle' ou doublon(s) supprimée(s).")

            # 5. SAUVEGARDE EN CSV UTF-8
            nom_sans_extension = os.path.splitext(nom_fichier)[0]
            nom_sortie = f"{nom_sans_extension}_clean.csv"
            chemin_sortie = os.path.join(DOSSIER_OUTPUT, nom_sortie)

            # encoding='utf-8-sig' : ajoute un BOM pour une compatibilité Excel optimale
            df.to_csv(chemin_sortie, index=False, encoding='utf-8-sig')
            print(f"-> Sauvegardé en UTF-8 sous : {chemin_sortie}")

        except Exception as e:
            print(f"-> Erreur sur {nom_fichier} : {e}")


if __name__ == "__main__":
    nettoyage_generique()