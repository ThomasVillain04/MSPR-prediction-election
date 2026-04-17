import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore')


#####################################################################################
#                               MSPR Data                                             #
#   Groupe 2    :                                                                     #
#     Kévin CALLAUD                                                                   #
#     Charlélie JAMARD                                                                #
#     Thomas VILLAIN                                                                  #
#     Donat RAVA                                                                      #
#                                                                                     #
#                                                                                     #
######################################################################################

# --- CONFIGURATION ---
DOSSIER_DATA = "output"
load_dotenv()
UTILISATEUR, MOT_DE_PASSE, HOTE, PORT, NOM_BDD = "postgres", "admin", "172.16.145.32", "5432", "DB1"
engine = create_engine(f"postgresql://{UTILISATEUR}:{MOT_DE_PASSE}@{HOTE}:{PORT}/{NOM_BDD}")

def nettoyer_tables():
    """Nettoie les tables avant insertion."""
    with engine.connect() as conn:
        conn.execute(text('TRUNCATE TABLE "Resultats_Municipales", "Resultats_Presidentielles", "Candidat", "Parti_Politique", "Donnees_Angers" RESTART IDENTITY CASCADE;'))
        conn.commit()
    print("🧹 Tables nettoyées.")

def executer_etl_mcd():
    print("🚀 ETL MCD démarré...")
    nettoyer_tables()
    
    # --- ÉTAPE 1: PARTI_POLITIQUE ---
    print("1/5 - Insertion Parti_Politique...")
    df_bloc = pd.read_csv(os.path.join(DOSSIER_DATA, 'bloc_clean.csv'))
    df_parti = df_bloc[['signification', 'bloc']].drop_duplicates().dropna()
    df_parti.columns = ['nom_parti', 'bord']
    df_parti.insert(0, 'id', range(1, len(df_parti) + 1))
    df_parti.to_sql('Parti_Politique', engine, if_exists='append', index=False)
    dict_parti_id = dict(zip(df_parti['nom_parti'], df_parti['id']))

    print(f"   ✅ Partis politiques insérés.")

    # --- ÉTAPE 2: CANDIDAT ---
    print("2/5 - Insertion Candidat...")
    MAPPING_CANDIDATS = {
        # Présidentielles 2012
        'Eva Joly': ('Eva', 'Joly', 'Les Écologistes'),
        'Nicolas Sarkozy': ('Nicolas', 'Sarkozy', 'Les Républicains'),
        'Jean-Luc Melenchon': ('Jean-Luc', 'Mélenchon', 'La France insoumise'),
        'Jacques Cheminade': ('Jacques', 'Cheminade', 'Divers'),
        'François Bayrou': ('François', 'Bayrou', 'Divers centre'),
        'François Hollande': ('François', 'Hollande', 'Parti socialiste'),
        # Présidentielles 2017+
        'Nathalie Arthaud': ('Nathalie', 'Arthaud', 'Extrême gauche'),
        'Fabien Roussel': ('Fabien', 'Roussel', 'Parti communiste français'),
        'Emmanuel Macron': ('Emmanuel', 'Macron', 'Renaissance'),
        'Jean Lassalle': ('Jean', 'Lassalle', 'Divers'),
        'Marine Le Pen': ('Marine', 'Le Pen', 'Rassemblement National'),
        'Eric Zemmour': ('Eric', 'Zemmour', 'Reconquête'),
        'Jean-Luc Mélenchon': ('Jean-Luc', 'Mélenchon', 'La France insoumise'),
        'Anne Hidalgo': ('Anne', 'Hidalgo', 'Parti socialiste'),
        'Yannick Jadot': ('Yannick', 'Jadot', 'Les Écologistes'),
        'Valérie Pécresse': ('Valérie', 'Pécresse', 'Les Républicains'),
        'Philippe Poutou': ('Philippe', 'Poutou', 'Extrême gauche'),
        'Nicolas Dupont-Aignan': ('Nicolas', 'Dupont-Aignan', 'Droite souverainiste'),
        'François Fillon': ('François', 'Fillon', 'Les Républicains'),
        'Benoît Hamon': ('Benoît', 'Hamon', 'Parti socialiste'),
        # Municipales 2014 & 2020
        'ANGERS POUR VOUS': ('Christophe', 'Béchu', 'Divers droite'),
        'LUTTE OUVRIERE': ('Céline', 'LHuillier', 'Extrême gauche'),
        'ANGERS CITOYENNE ET POPULAIRE': ('Claire', 'Schweitzer', 'Divers gauche'),
        'AIMER ANGERS': ('Silvia', 'Camara-Tombini', 'Divers gauche'),
        'ANGERS ECOLOGIQUE ET SOLIDAIRE': ('Yves', 'Aurégan', 'Ecologiste'),
        'CHOISIR ANGERS': ('Stéphane', 'Piednoir', 'Divers centre'),
        # Municipales 2026 (Seulement les nouveaux candidats)
        'DEMAIN ANGERS': ('Romain', 'Laveau', 'Les Écologistes'),
        'ANGERS OUVRIERE REVOLUTIONNAIRE': ('Nicolas', 'Cuisinier', 'Extrême gauche'),
        'ANGERS POPULAIRE': ('Arash', 'Saeidi', 'Divers gauche'),
        'ANGERS 2026': ('Valentin', 'Rambault', 'Divers'),
        'RASSEMBLEMENT POUR ANGERS': ('Aurore', 'Lahondès', 'Rassemblement National')
    }
    print(f"   ✅ Mapping des candidats : OK.")

    
    candidats_list = []
    for i, (key, (prenom, nom, parti_nom)) in enumerate(MAPPING_CANDIDATS.items(), 1):
        p_id = dict_parti_id.get(parti_nom)
        candidats_list.append({
            'id': i, 'nom': nom, 'prenom': prenom, 'parti_politique_id': p_id, 
            'popularite': None, 'key_search': key.upper()
        })
    
    df_cand_ref = pd.DataFrame(candidats_list)
    df_cand_ref.drop(columns=['key_search']).to_sql('Candidat', engine, if_exists='append', index=False)
    
    # Mapping candidat
    dict_cand_id = {}
    for _, row in df_cand_ref.iterrows():
        key = row['key_search']
        dict_cand_id[key] = row['id']
        dict_cand_id[key.replace(' ', '_')] = row['id']  # Gestion des espaces et underscores
        dict_cand_id[f"{row['prenom']} {row['nom']}".upper()] = row['id']

    # Alias pour les variantes orthographiques présentes dans les fichiers sources 2012
    # Les XLS 2012 utilisent 'MELENCHON' sans accent et 'AIMER ANGERS' sans '2020'
    dict_cand_id['JEAN-LUC MELENCHON'] = dict_cand_id.get('JEAN-LUC MÉLENCHON')
    dict_cand_id['JEAN-LUC_MELENCHON'] = dict_cand_id.get('JEAN-LUC MÉLENCHON')
    dict_cand_id['AIMER ANGERS 2020'] = dict_cand_id.get('AIMER ANGERS')
    dict_cand_id['AIMER_ANGERS_2020'] = dict_cand_id.get('AIMER ANGERS')

    print(f"   ✅ Candidats insérés.")


    # --- ÉTAPE 3: DONNEES_ANGERS ---
    print("3/5 - Insertion Donnees_Angers (Criminalité, Pauvreté, Revenu)...")
    
    df_c = pd.read_csv(os.path.join(DOSSIER_DATA, 'chomage-angers-trim-2003-2025_clean.csv'))
    df_crim = pd.read_csv(os.path.join(DOSSIER_DATA, 'taux_criminalite_clean.csv'))
    df_pauv = pd.read_csv(os.path.join(DOSSIER_DATA, 'taux_pauvrete_clean.csv'))
    df_rev = pd.read_csv(os.path.join(DOSSIER_DATA, 'revenu_median_angers_clean.csv'))

    df_c_ang = df_c[df_c['libze2020'] == 'Angers']
    cols_trims = [c for c in df_c_ang.columns if '-t' in c]
    df_c_melt = pd.melt(df_c_ang, value_vars=cols_trims, var_name='periode', value_name='taux_chomage')
    df_c_melt['annee'] = df_c_melt['periode'].str.split('-t').str[0].astype(int)
    df_c_melt['num_trimestre'] = df_c_melt['periode'].str.split('-t').str[1].astype(int)
    df_base = df_c_melt[['annee', 'num_trimestre', 'taux_chomage']]

    for df in [df_crim, df_pauv, df_rev]:
        if 'trimestre' in df.columns:
            df.rename(columns={'trimestre': 'num_trimestre'}, inplace=True)

    df_final_angers = pd.merge(df_base, df_crim[['annee', 'num_trimestre', 'taux_criminalite']], on=['annee', 'num_trimestre'], how='left')
    df_final_angers = pd.merge(df_final_angers, df_pauv[['annee', 'num_trimestre', 'taux_pauvrete']], on=['annee', 'num_trimestre'], how='left')
    df_final_angers = pd.merge(df_final_angers, df_rev[['annee', 'num_trimestre', 'revenu_median']], on=['annee', 'num_trimestre'], how='left')

    df_insert_angers = df_final_angers[['num_trimestre', 'annee', 'taux_chomage', 'revenu_median', 'taux_pauvrete', 'taux_criminalite']]
    df_insert_angers.insert(0, 'id', range(1, len(df_insert_angers) + 1))
    
    df_insert_angers.to_sql('Donnees_Angers', engine, if_exists='append', index=False)
    print(f"   ✅ Donnees_Angers insérées ({len(df_insert_angers)} lignes).")

    # --- ÉTAPE 4: RESULTATS_PRESIDENTIELLES ---
    print("4/5 - Insertion Resultats_Presidentielles...")
    fichiers_pres = [
        # 2012 : colonnes candidats nommées directement (même logique que 2022)
        {"annee": 2012, "tour": 1, "file": 'elections-presidentielles-1-tour-angers-2012_clean.csv', "col_bureau": 'bureaux'},
        {"annee": 2012, "tour": 2, "file": 'elections-presidentielles-2-tour-angers-2012_clean.csv', "col_bureau": 'bureaux'},
        {"annee": 2017, "tour": 1, "file": 'elections-presidentielles-1-tour-angers-2017_clean.csv', "col_bureau": 'bureaux'},
        {"annee": 2017, "tour": 2, "file": 'elections-presidentielles-2-tour-angers-2017_clean.csv', "col_bureau": 'bureau'},
        {"annee": 2022, "tour": 1, "file": 'elections-presidentielles-1-tour-angers-2022_clean.csv', "col_bureau": 'bureau vote'},
        {"annee": 2022, "tour": 2, "file": 'elections-presidentielles-2-tour-angers-2022_clean.csv', "col_bureau": 'bureau vote'}
    ]

    p_idx = 1
    for f_info in fichiers_pres:
        path = os.path.join(DOSSIER_DATA, f_info["file"])
        if os.path.exists(path):
            df = pd.read_csv(path)
            if f_info["annee"] == 2017:
                cols_cands = [c for c in df.columns if c.startswith('nb_voix_')]
                df_melted = pd.melt(df, id_vars=[f_info["col_bureau"]], value_vars=cols_cands, var_name='cand_brut', value_name='nb_voix')
                df_melted['nom_recherche'] = df_melted['cand_brut'].str.replace('nb_voix_', '').str.replace('_', ' ').str.upper()
            else:
                cols_exclues = ['bureau vote', 'bureaux', 'bureau', 'nom bureau vote', 'circonscription', 'inscrits', 'abstentions', 'votants_urne', 'votants_emarg', 'blancs', 'nuls', 'exprimes', 'geo shape', 'geo_point_2d']
                cols_cands = [c for c in df.columns if c not in cols_exclues]
                df_melted = pd.melt(df, id_vars=[f_info["col_bureau"]], value_vars=cols_cands, var_name='nom_recherche', value_name='nb_voix')
                df_melted['nom_recherche'] = df_melted['nom_recherche'].str.upper()

            df_melted['candidat_id'] = df_melted['nom_recherche'].map(dict_cand_id)
            df_melted = df_melted.dropna(subset=['candidat_id'])
            df_final = pd.merge(df_melted, df_cand_ref[['id', 'nom', 'prenom']], left_on='candidat_id', right_on='id')

            df_to_insert = df_final[['nom', 'prenom', 'nb_voix', 'candidat_id']].copy()
            df_to_insert['num_tour'] = f_info["tour"]
            df_to_insert['annee'] = f_info["annee"]
            df_to_insert.insert(0, 'id', range(p_idx, p_idx + len(df_to_insert)))
            p_idx += len(df_to_insert)
            
            df_to_insert.to_sql('Resultats_Presidentielles', engine, if_exists='append', index=False)
            print(f"   ✅ {f_info['annee']} Tour {f_info['tour']} inséré.")

    # --- ÉTAPE 5: RESULTATS_MUNICIPALES ---
    print("5/5 - Insertion Resultats_Municipales...")
    
    config_municipales = [
        # --- Municipales 2014 - Tour 1 ---
        # Seuls ANGERS POUR VOUS et LUTTE OUVRIERE ont un candidat_id dans le mapping.
        # AIMER ANGERS (sans '2020') est résolu via l'alias ajouté dans dict_cand_id.
        # Les autres listes (ANGERS VIVRE MIEUX, etc.) sont absentes du mapping et seront
        # ignorées automatiquement par le dropna(subset=['candidat_id']).
        {
            'fichier': 'elections-municipales-1-tour-angers-2014_clean.csv',
            'annee': 2014, 'tour': 1,
            'cands': ['angers_pour_vous', 'lutte_ouvriere', 'aimer_angers']
        },
        # --- Municipales 2014 - Tour 2 ---
        {
            'fichier': 'elections-municipales-2-tour-angers-2014_clean.csv',
            'annee': 2014, 'tour': 2,
            'cands': ['angers_pour_vous', 'aimer_angers']
        },
        # --- Municipales 2020 ---
        {
            'fichier': 'elections-municipales-1-tour-angers-2020_clean.csv',
            'annee': 2020, 'tour': 1,
            'cands': ['angers_pour_vous', 'lutte_ouvriere', 'angers_citoyenne_et_populaire', 'aimer_angers_2020', 'angers_ecologique_et_solidaire', 'choisir_angers']
        },
        # --- Municipales 2026 - Tour 1 ---
        {
            'fichier': 'elections-municipales-1-tour-angers-2026_clean.csv',
            'annee': 2026, 'tour': 1,
            'cands': ['demain_angers', 'angers_ouvriere_revolutionnaire', 'angers_pour_vous', 'angers_populaire', 'angers_2026', 'lutte_ouvriere', 'rassemblement_pour_angers']
        },
        # --- Municipales 2026 - Tour 2 ---
        {
            'fichier': 'elections-municipales-2-tour-angers-2026_clean.csv',
            'annee': 2026, 'tour': 2,
            'cands': ['demain_angers', 'angers_pour_vous']
        }
    ]

    for conf in config_municipales:
        path = os.path.join(DOSSIER_DATA, conf['fichier'])
        if os.path.exists(path):
            df = pd.read_csv(path, sep=None, engine='python')
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            cols_valides = [c for c in conf['cands'] if c in df.columns]
            
            df_melt = pd.melt(df, value_vars=cols_valides, var_name='liste', value_name='nb_voix')
            
            df_melt['candidat_id'] = df_melt['liste'].str.upper().map(dict_cand_id)
            
            df_final = pd.merge(df_melt, df_cand_ref[['id', 'nom', 'prenom']], left_on='candidat_id', right_on='id')
            
            df_to_ins = df_final[['nom', 'prenom', 'nb_voix', 'candidat_id']].copy()
            df_to_ins['num_tour'] = conf['tour']
            df_to_ins['annee'] = conf['annee']
            
            df_to_ins.to_sql('Resultats_Municipales', engine, if_exists='append', index=False)
            print(f"   ✅ {conf['annee']} Tour {conf['tour']} inséré ({len(df_to_ins)} lignes).")
        else:
            print(f"   ⚠️ Fichier manquant : {conf['fichier']}")
    
    print("🚀 ETL Terminé avec succès !")

if __name__ == "__main__":
    try:
        executer_etl_mcd()
    except Exception as e:
        print(f"❌ Erreur critique lors de l'exécution : {e}")