import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import warnings

# Désactiver les avertissements de Pandas pour un affichage propre
warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURATION ET CONNEXION
# ==========================================
DOSSIER_DATA = "input"

load_dotenv() 
UTILISATEUR = "postgres"
MOT_DE_PASSE = os.getenv("admin") 
HOTE = "172.16.145.32"
PORT = "5432"
NOM_BDD = "DB1"

chaine_connexion = f"postgresql://{UTILISATEUR}:{MOT_DE_PASSE}@{HOTE}:{PORT}/{NOM_BDD}"
moteur = create_engine(chaine_connexion)

# ==========================================
# DICTIONNAIRE DE MAPPING UNIVERSEL
# ==========================================
# Fait correspondre chaque candidat/liste
MAPPING_CANDIDATS_PARTIS = {
    # Présidentielles 2022
    'Nathalie Arthaud': 'Extrême gauche',
    'Fabien Roussel': 'Parti communiste français',
    'Emmanuel Macron': 'Renaissance',
    'Jean Lassalle': 'Divers',
    'Marine Le Pen': 'Rassemblement National',
    'Eric Zemmour': 'Reconquête',
    'Jean-Luc Mélenchon': 'La France insoumise',
    'Anne Hidalgo': 'Parti socialiste',
    'Yannick Jadot': 'Les Écologistes',
    'Valérie Pécresse': 'Les Républicains',
    'Philippe Poutou': 'Extrême gauche',
    'Nicolas Dupont-Aignan': 'Droite souverainiste',
    # Municipales Angers 2020
    'ANGERS POUR VOUS': 'Divers droite',
    'LUTTE OUVRIERE': 'Extrême gauche',
    'ANGERS CITOYENNE ET POPULAIRE': 'Divers gauche',
    'AIMER ANGERS 2020': 'Divers gauche',
    'ANGERS ECOLOGIQUE ET SOLIDAIRE': 'Ecologiste',
    'CHOISIR ANGERS': 'Divers centre'
}

def executer_pipeline_complet():
    print("🚀 Démarrage du pipeline...")
    
    try: # <--- LE TRY MANQUANT ÉTAIT ICI
        
        # ==========================================
        # ÉTAPE 1 : BLOC ET PARTI POLITIQUE
        # ==========================================
        print("1/6 - Traitement des Blocs et Partis...")
        df_bloc_source = pd.read_csv(os.path.join(DOSSIER_DATA, 'bloc.csv'), sep=';')
        
        # 1.1 Table BLOC
        df_bloc = pd.DataFrame({'nom_bloc': df_bloc_source['bloc'].dropna().unique()})
        df_bloc.insert(0, 'id_bloc', range(1, len(df_bloc) + 1))
        df_bloc.to_sql('BLOC', moteur, if_exists='append', index=False)
        
        # 1.2 Table PARTI_POLITIQUE
        df_parti = df_bloc_source[['libelle_nuance', 'bloc']].drop_duplicates().dropna()
        df_parti = df_parti.rename(columns={'libelle_nuance': 'nom_parti'})
        df_parti = pd.merge(df_parti, df_bloc, left_on='bloc', right_on='nom_bloc', how='left')
        df_parti_final = df_parti[['nom_parti', 'id_bloc']]
        df_parti_final.insert(0, 'id_parti', range(1, len(df_parti_final) + 1))
        df_parti_final.to_sql('PARTI_POLITIQUE', moteur, if_exists='append', index=False)
        print("   ✅ Référentiels politiques intégrés.")

        # ==========================================
        # ÉTAPE 2 : LIEU
        # ==========================================
        print("2/6 - Traitement des Lieux...")
        # On crée directement la commune d'Angers pour y rattacher nos bureaux et indicateurs
        df_lieu = pd.DataFrame([{
            'id_lieu': 1,
            'code_insee': '49007',
            'nom_commune': 'Angers',
            'departement': 'Maine-et-Loire'
        }])
        df_lieu.to_sql('LIEU', moteur, if_exists='append', index=False)
        print("   ✅ Lieux intégrés.")

        # ==========================================
        # ÉTAPE 3 : BUREAUX DE VOTE
        # ==========================================
        print("3/6 - Traitement des Bureaux de vote...")
        df_angers22_t1 = pd.read_csv(os.path.join(DOSSIER_DATA, 'election-presidentielle-2022-premier-tour-angers.csv'), sep=';')
        df_angers20_mun = pd.read_csv(os.path.join(DOSSIER_DATA, 'elections-municipales-1-tour-angers-2020.xlsx - Feuil1.csv'), sep=',')
        
        # On récupère tous les numéros de bureaux uniques depuis les fichiers d'Angers
        bureaux_22 = df_angers22_t1['bureau vote'].dropna().astype(str).unique()
        bureaux_20 = df_angers20_mun['bureau_de_vote'].dropna().astype(str).unique()
        tous_bureaux = list(set(list(bureaux_22) + list(bureaux_20)))
        
        df_bureau = pd.DataFrame({'numero_bureau': tous_bureaux})
        df_bureau['id_lieu'] = 1 # Rattaché à Angers
        df_bureau.insert(0, 'id_bureau', range(1, len(df_bureau) + 1))
        df_bureau.to_sql('BUREAU_VOTE', moteur, if_exists='append', index=False)
        print("   ✅ Bureaux de vote intégrés.")

        # ==========================================
        # ÉTAPE 4 : CANDIDATS
        # ==========================================
        print("4/6 - Traitement des Candidats...")
        df_candidat = pd.DataFrame({'nom_complet': list(MAPPING_CANDIDATS_PARTIS.keys())})
        df_candidat['nom_parti_cible'] = df_candidat['nom_complet'].map(MAPPING_CANDIDATS_PARTIS)
        
        # On relie avec la table PARTI_POLITIQUE pour récupérer l'id_parti
        df_candidat = pd.merge(df_candidat, df_parti_final, left_on='nom_parti_cible', right_on='nom_parti', how='left')
        
        # Séparation Nom / Prénom basique
        df_candidat['prenom'] = df_candidat['nom_complet'].apply(lambda x: x.split(' ')[0] if len(x.split(' ')) > 1 else '')
        df_candidat['nom'] = df_candidat['nom_complet'].apply(lambda x: ' '.join(x.split(' ')[1:]) if len(x.split(' ')) > 1 else x)
        
        df_candidat_final = df_candidat[['nom', 'prenom', 'id_parti']]
        # On garde le nom_complet temporairement pour l'étape 6
        df_candidat_final.insert(0, 'id_candidat', range(1, len(df_candidat_final) + 1))
        
        # Insertion en base
        df_candidat_final[['id_candidat', 'nom', 'prenom', 'id_parti']].to_sql('CANDIDAT', moteur, if_exists='append', index=False)
        
        # Dictionnaire pour l'étape 6 (associer le nom de la colonne CSV à l'id_candidat généré)
        dict_candidat_id = dict(zip(df_candidat['nom_complet'], df_candidat_final['id_candidat']))
        print("   ✅ Candidats intégrés.")

        # ==========================================
        # ÉTAPE 5 : INDICATEURS SOCIO-ECO
        # ==========================================
        print("5/6 - Traitement des Indicateurs (Police & Chômage)...")
        # --- POLICE ---
        df_police = pd.read_csv(os.path.join(DOSSIER_DATA, 'donnee-police.csv'), sep=';', decimal=',')
        df_police['CODGEO_2025'] = df_police['CODGEO_2025'].astype(str)
        # On filtre sur Angers (49007)
        df_police_angers = df_police[df_police['CODGEO_2025'] == '49007']
        df_pol_grp = df_police_angers.groupby('annee')['taux_pour_mille'].sum().reset_index().rename(columns={'taux_pour_mille': 'taux_criminalite'})
        
        # --- CHOMAGE ---
        df_chomage = pd.read_csv(os.path.join(DOSSIER_DATA, 'chomage-zone-t1-2003-t2-2025.xlsx - txcho_ze.csv'), sep=',', skiprows=3)
        # Angers est la zone d'emploi 5201
        df_chom_angers = df_chomage[df_chomage["Code de la zone d'emploi 2020"] == '5201']
        
        # Transformation des colonnes trimestres en lignes (Melt)
        df_chom_melt = pd.melt(df_chom_angers, id_vars=["Code de la zone d'emploi 2020"], var_name='trimestre', value_name='taux_chomage')
        df_chom_melt = df_chom_melt[df_chom_melt['trimestre'].str.contains('trimestre', na=False)]
        df_chom_melt['annee'] = df_chom_melt['trimestre'].str[-4:].astype(int)
        df_chom_grp = df_chom_melt.groupby('annee')['taux_chomage'].mean().reset_index()

        # --- FUSION INDICATEURS ---
        df_ind = pd.merge(df_pol_grp, df_chom_grp, on='annee', how='outer')
        df_ind['id_lieu'] = 1
        df_ind.insert(0, 'id_indicateur', range(1, len(df_ind) + 1))
        df_ind.to_sql('INDICATEURS_SOCIO_ECO', moteur, if_exists='append', index=False)
        print(" Indicateurs socio-économiques intégrés.")

        # ==========================================
        # ÉTAPE 6 : RÉSULTATS (Le coeur du métier)
        # ==========================================
        print("6/6 - Traitement des Résultats électoraux...")
        dfs_resultats = []

        # Fonction générique pour pivoter (Melt) les fichiers d'élections
        def preparer_resultats(df_source, col_bureau, colonnes_candidats, annee, tour):
            df_melt = pd.melt(df_source, id_vars=[col_bureau, 'inscrits', 'abstentions'], value_vars=colonnes_candidats, var_name='candidat_str', value_name='nb_voix')
            df_melt['annee_election'] = annee
            df_melt['tour'] = tour
            df_melt['numero_bureau'] = df_melt[col_bureau].astype(str)
            return df_melt

        # 6.1 - Angers 2022 Tour 1
        cands_22_t1 = ['Nathalie Arthaud', 'Fabien Roussel', 'Emmanuel Macron', 'Jean Lassalle', 'Marine Le Pen', 'Eric Zemmour', 'Jean-Luc Mélenchon', 'Anne Hidalgo', 'Yannick Jadot', 'Valérie Pécresse', 'Philippe Poutou', 'Nicolas Dupont-Aignan']
        res_22_t1 = preparer_resultats(df_angers22_t1, 'bureau vote', cands_22_t1, 2022, 1)
        dfs_resultats.append(res_22_t1)

        # 6.2 - Angers 2022 Tour 2
        df_angers22_t2 = pd.read_csv(os.path.join(DOSSIER_DATA, 'election-presidentielle-2022-second-tour-angers.csv'), sep=';')
        cands_22_t2 = ['Emmanuel Macron', 'Marine Le Pen']
        res_22_t2 = preparer_resultats(df_angers22_t2, 'bureau vote', cands_22_t2, 2022, 2)
        dfs_resultats.append(res_22_t2)

        # 6.3 - Angers 2020 Municipales Tour 1
        cands_20_t1 = ['ANGERS POUR VOUS', 'LUTTE OUVRIERE', 'ANGERS CITOYENNE ET POPULAIRE', 'AIMER ANGERS 2020', 'ANGERS ECOLOGIQUE ET SOLIDAIRE', 'CHOISIR ANGERS']
        res_20_t1 = preparer_resultats(df_angers20_mun, 'bureau_de_vote', cands_20_t1, 2020, 1)
        dfs_resultats.append(res_20_t1)

        # Concaténation finale
        df_tous_resultats = pd.concat(dfs_resultats, ignore_index=True)

        # Remplacement des strings par les bons IDs (Clés étrangères)
        df_tous_resultats['id_candidat'] = df_tous_resultats['candidat_str'].map(dict_candidat_id)
        
        # Mapping de l'id_bureau
        dict_bureau_id = dict(zip(df_bureau['numero_bureau'], df_bureau['id_bureau']))
        df_tous_resultats['id_bureau'] = df_tous_resultats['numero_bureau'].map(dict_bureau_id)

        # On nettoie et on insère
        df_tous_resultats = df_tous_resultats.dropna(subset=['nb_voix', 'id_candidat', 'id_bureau'])
        df_res_final = df_tous_resultats[['id_bureau', 'id_candidat', 'annee_election', 'tour', 'inscrits', 'abstentions', 'nb_voix']]
        df_res_final.insert(0, 'id_resultat', range(1, len(df_res_final) + 1))
        
        df_res_final.to_sql('RESULTATS', moteur, if_exists='append', index=False)
        print("Résultats électoraux intégrés.")

        print("\n INCROYABLE ! Ton architecture en étoile est 100% opérationnelle !")

    except Exception as e:
        print(f"\n  Erreur critique : {e}")

if __name__ == "__main__":
    executer_pipeline_complet()