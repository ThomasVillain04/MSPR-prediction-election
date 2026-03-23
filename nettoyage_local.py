import pandas as pd
import os

# ==========================================
# 1. CONFIGURATION DU DOSSIER
# ==========================================
DOSSIER_DONNEES = r"C:\Users\villa\OneDrive - Ifag Paris\MSPR DATA"

def nettoyer_et_preparer_fichiers():
    print("Démarrage du grand nettoyage automatisé...")

    # ==========================================
    # TABLE 1 : PARTI POLITIQUE
    # ==========================================
    print("1/4 Préparation de 'Parti_Politique'...")
    chemin_libele = os.path.join(DOSSIER_DONNEES, 'Libele.csv')
    df_parti = pd.read_csv(chemin_libele)
    
    # Standardisation : tout en minuscules
    df_parti.columns = df_parti.columns.str.lower()
    
    # Transformation
    df_parti = df_parti[['libelle']].drop_duplicates().rename(columns={'libelle': 'nom_parti'})
    df_parti.insert(0, 'id', range(1, len(df_parti) + 1))
    df_parti.to_csv(os.path.join(DOSSIER_DONNEES, 'Parti_Politique_clean.csv'), index=False)

    # ==========================================
    # LECTURE DES ÉLECTIONS (Sert pour Table 2 et 3)
    # ==========================================
    chemin_elections = os.path.join(DOSSIER_DONNEES, 'Elections_Angers_Complet.csv')
    df_elec = pd.read_csv(chemin_elections, sep=';')
    
    # Standardisation : tout en minuscules (nom, prenom, annee, tour, voix...)
    df_elec.columns = df_elec.columns.str.lower()

    # ==========================================
    # TABLE 2 : CANDIDAT
    # ==========================================
    print("2/4 Préparation de 'Candidat'...")
    # Plus besoin de renommer Nom et Prenom, c'est déjà fait !
    df_cand = df_elec[['nom', 'prenom']].drop_duplicates()
    df_cand.insert(0, 'id', range(1, len(df_cand) + 1))
    
    df_cand['parti_politique_id'] = None 
    df_cand['popularité'] = None
    
    df_cand = df_cand[['id', 'nom', 'prenom', 'parti_politique_id', 'popularité']]
    df_cand.to_csv(os.path.join(DOSSIER_DONNEES, 'Candidat_clean.csv'), index=False)

    # ==========================================
    # TABLE 3 : RÉSULTATS PRÉSIDENTIELLES
    # ==========================================
    print("3/4 Préparation de 'Resultats_Presidentielles'...")
    # On ne renomme que les mots qui changent vraiment pour ton SQL
    df_res = df_elec.rename(columns={'voix': 'nb_voix', 'tour': 'num_tour', 'annee': 'année'})
    
    # Le Mapping (nom et prenom sont identiques dans les deux tables)
    df_res = pd.merge(df_res, df_cand[['id', 'nom', 'prenom']], on=['nom', 'prenom'], how='left')
    df_res = df_res.rename(columns={'id': 'candidat_id'})
    
    df_res.insert(0, 'id', range(1, len(df_res) + 1))
    df_res['parti_politique_id'] = None 
    
    df_res_final = df_res[['id', 'nb_voix', 'num_tour', 'année', 'candidat_id', 'parti_politique_id']]
    df_res_final.to_csv(os.path.join(DOSSIER_DONNEES, 'Resultats_Presidentielles_clean.csv'), index=False)

    # ==========================================
    # TABLE 4 : DONNÉES ANGERS
    # ==========================================
    print("4/4 Préparation de 'Donnees_Angers'...")
    chemin_chomage = os.path.join(DOSSIER_DONNEES, 'Chomage_Angers_Annuel.csv')
    chemin_securite = os.path.join(DOSSIER_DONNEES, 'Securite_Angers_Clean.csv')
    
    df_chom = pd.read_csv(chemin_chomage, sep=';')
    df_sec = pd.read_csv(chemin_securite, sep=';', decimal=',')
    
    # Standardisation : tout en minuscules pour les deux fichiers
    df_chom.columns = df_chom.columns.str.lower()
    df_sec.columns = df_sec.columns.str.lower()
    
    # "annee" est déjà en minuscules, on renomme juste les taux
    df_sec_grp = df_sec.groupby('annee')['taux_pour_mille'].sum().reset_index().rename(columns={'taux_pour_mille': 'taux_criminalite'})
    df_chom = df_chom.rename(columns={'taux': 'taux_chomage'})
    
    # Fusion
    df_donnees = pd.merge(df_chom, df_sec_grp, on='annee', how='left')
    df_donnees = df_donnees.rename(columns={'annee': 'num_trimestre'})
    df_donnees.insert(0, 'id', range(1, len(df_donnees) + 1))
    
    df_donnees['revenu_median'] = None
    df_donnees['taux_pauvrete'] = None
    
    df_donnees_final = df_donnees[['id', 'num_trimestre', 'taux_chomage', 'revenu_median', 'taux_pauvrete', 'taux_criminalite']]
    df_donnees_final.to_csv(os.path.join(DOSSIER_DONNEES, 'Donnees_Angers_clean.csv'), index=False)

    print("\n🎉 Nettoyage terminé ! Le code est beaucoup plus propre et professionnel.")

# Lancement
nettoyer_et_preparer_fichiers()