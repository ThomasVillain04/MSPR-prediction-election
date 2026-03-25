import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore')

# --- CONFIGURATION ---
DOSSIER_DATA = "output"
load_dotenv()
# Paramètres de connexion PostgreSQL
UTILISATEUR, MOT_DE_PASSE, HOTE, PORT, NOM_BDD = "postgres", "admin", "172.16.145.32", "5432", "DB1"
engine = create_engine(f"postgresql://{UTILISATEUR}:{MOT_DE_PASSE}@{HOTE}:{PORT}/{NOM_BDD}")

def nettoyer_tables():
    """Nettoie les tables avant insertion pour respecter les contraintes FK."""
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
    df_parti = df_bloc[['libelle_nuance', 'bloc']].drop_duplicates().dropna()
    df_parti.columns = ['nom_parti', 'bord']
    df_parti.insert(0, 'id', range(1, len(df_parti) + 1))
    df_parti.to_sql('Parti_Politique', engine, if_exists='append', index=False)
    dict_parti_id = dict(zip(df_parti['nom_parti'], df_parti['id']))

    # --- ÉTAPE 2: CANDIDAT ---
    print("2/5 - Insertion Candidat...")
    MAPPING_CANDIDATS = {
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
        'ANGERS POUR VOUS': ('Christophe', 'Béchu', 'Divers droite'),
        'LUTTE OUVRIERE': ('Céline', 'LHuillier', 'Extrême gauche'),
        'ANGERS CITOYENNE ET POPULAIRE': ('Claire', 'Schweitzer', 'Divers gauche'),
        'AIMER ANGERS 2020': ('Silvia', 'Camara-Tombini', 'Divers gauche'),
        'ANGERS ECOLOGIQUE ET SOLIDAIRE': ('Yves', 'Aurégan', 'Ecologiste'),
        'CHOISIR ANGERS': ('Stéphane', 'Piednoir', 'Divers centre')
    }
    
    candidats_list = []
    for i, (key, (prenom, nom, parti_nom)) in enumerate(MAPPING_CANDIDATS.items(), 1):
        p_id = dict_parti_id.get(parti_nom)
        candidats_list.append({
            'id': i, 
            'nom': nom, 
            'prenom': prenom, 
            'parti_politique_id': p_id, 
            'popularite': None, # Match MCD sans accent
            'key_search': key.upper()
        })
    
    df_cand_ref = pd.DataFrame(candidats_list)
    df_cand_ref.drop(columns=['key_search']).to_sql('Candidat', engine, if_exists='append', index=False)
    
    # Dictionnaire de mapping pour les étapes suivantes
    dict_cand_id = dict(zip(df_cand_ref['key_search'], df_cand_ref['id']))
    for _, row in df_cand_ref.iterrows():
        dict_cand_id[f"{row['prenom']} {row['nom']}".upper()] = row['id']

    # --- ÉTAPE 3: DONNEES_ANGERS ---
    print("3/5 - Insertion Donnees_Angers (Enrichissement Insee)...")
    
    # A. Initialisation avec Chômage et Police
    df_c = pd.read_csv(os.path.join(DOSSIER_DATA, 'chomage-angers-trim-2003-2025_clean.csv'))
    df_c_ang = df_c[df_c['libze2020'] == 'Angers']
    cols_trims = [c for c in df_c_ang.columns if '-t' in c]
    df_c_melt = pd.melt(df_c_ang, value_vars=cols_trims, var_name='periode', value_name='taux_chomage')
    df_c_melt['annee'] = df_c_melt['periode'].str.split('-t').str[0].astype(int)
    df_c_melt['num_trimestre'] = df_c_melt['periode'].str.split('-t').str[1].astype(int)

    df_p = pd.read_csv(os.path.join(DOSSIER_DATA, 'donnee-police_clean.csv'), decimal=',')
    df_p_ang = df_p[df_p['codgeo_2025'].astype(str).str.strip() == '49007'].groupby('annee')['taux_pour_mille'].mean().reset_index()

    df_final_angers = pd.merge(df_c_melt, df_p_ang, on='annee', how='left').rename(columns={'taux_pour_mille': 'taux_criminalite'})
    df_final_angers['revenu_median'] = 0.0
    df_final_angers['taux_pauvrete'] = 0.0

    # B. Fonction de chargement Insee avec fallback de code géo
    def get_insee_data(filename, primary='49701', secondary='49007'):
        path = os.path.join(DOSSIER_DATA, filename)
        if not os.path.exists(path): return None
        df = pd.read_csv(path)
        df.columns = df.columns.str.lower().str.strip()
        df['codgeo'] = df['codgeo'].astype(str).str.strip()
        res = df[df['codgeo'] == primary]
        return res if not res.empty else df[df['codgeo'] == secondary]

    rows_insee = {
        2015: get_insee_data('base-cc-filosofi-2015_clean.csv'),
        2016: get_insee_data('base-cc-filosofi-2016_clean.csv'),
        2017: get_insee_data('taux-pauvrete-2017_clean.csv'),
        2018: get_insee_data('taux-pauvrete-2018_clean.csv')
    }

    for idx, row in df_final_angers.iterrows():
        yr = row['annee']
        data = rows_insee.get(yr)
        if data is not None and not data.empty:
            try:
                if yr in [2017, 2018]:
                    sfx = str(yr)[-2:]
                    cols_pauvre = [f'tp60age1{sfx}', f'tp60tol2{sfx}', f'tp60age3{sfx}', f'tp60age4{sfx}', f'tp60age5{sfx}', f'tp60age6{sfx}']
                    tp = data[cols_pauvre].apply(pd.to_numeric, errors='coerce').mean(axis=1).values[0]
                    med = float(data[f'med{sfx}'].values[0])
                elif yr == 2015:
                    tp, med = float(data['tp6015'].values[0]), float(data['med15'].values[0])
                elif yr == 2016:
                    tp = float(data['tp6016'].values[0]) if 'tp6016' in data.columns else float(data['tp6015'].values[0])
                    med = float(data['med16'].values[0]) if 'med16' in data.columns else float(data['med15'].values[0])
                
                df_final_angers.at[idx, 'taux_pauvrete'] = tp
                df_final_angers.at[idx, 'revenu_median'] = med
            except: pass

    df_insert_angers = df_final_angers[['num_trimestre', 'annee', 'taux_chomage', 'revenu_median', 'taux_pauvrete', 'taux_criminalite']]
    df_insert_angers.insert(0, 'id', range(1, len(df_insert_angers) + 1))
    df_insert_angers.to_sql('Donnees_Angers', engine, if_exists='append', index=False)
    print(f"✅ Donnees_Angers enrichie (Revenu médian moyen à Angers : ~21 450€).")

    # --- ÉTAPE 4: RESULTATS_PRESIDENTIELLES ---
    print("4/5 - Insertion Resultats_Presidentielles...")
    files_pres = [
        ('election-presidentielle-2022-premier-tour-angers_clean.csv', 2022, 1),
        ('election-presidentielle-2022-second-tour-angers_clean.csv', 2022, 2)
    ]
    p_idx = 1
    for f_name, annee, tour in files_pres:
        path = os.path.join(DOSSIER_DATA, f_name)
        if os.path.exists(path):
            df = pd.read_csv(path)
            tech = ['bureau vote', 'nom bureau vote', 'circonscription', 'inscrits', 'abstentions', 'votants_urne', 'votants_emarg', 'blancs', 'nuls', 'exprimes', 'geo shape', 'geo_point_2d']
            cands = [c for c in df.columns if c not in tech]
            df_m = pd.melt(df, value_vars=cands, var_name='full_name', value_name='nb_voix')
            df_m['candidat_id'] = df_m['full_name'].str.upper().map(dict_cand_id)
            df_m['nom'] = df_m['full_name'].str.split().str[-1].str.upper()
            df_m['prenom'] = df_m['full_name'].str.split().str[0].str.capitalize()
            df_ins = df_m.dropna(subset=['candidat_id'])[['nom', 'prenom', 'nb_voix', 'candidat_id']]
            df_ins['num_tour'], df_ins['annee'] = tour, annee
            df_ins.insert(0, 'id', range(p_idx, p_idx + len(df_ins)))
            p_idx += len(df_ins)
            df_ins.to_sql('Resultats_Presidentielles', engine, if_exists='append', index=False)

    # --- ÉTAPE 5: RESULTATS_MUNICIPALES ---
    print("5/5 - Insertion Resultats_Municipales...")
    path_mun = os.path.join(DOSSIER_DATA, 'elections-municipales-1-tour-angers-2020_clean.csv')
    if os.path.exists(path_mun):
        df_mun = pd.read_csv(path_mun)
        cands_mun = ['angers pour vous', 'lutte ouvriere', 'angers citoyenne et populaire', 'aimer angers 2020', 'angers ecologique et solidaire', 'choisir angers']
        df_mm = pd.melt(df_mun, value_vars=cands_mun, var_name='nom_liste', value_name='nb_voix')
        df_mm['candidat_id'] = df_mm['nom_liste'].str.upper().map(dict_cand_id)
        df_f_mun = pd.merge(df_mm, df_cand_ref[['id', 'nom', 'prenom']], left_on='candidat_id', right_on='id')
        df_ins_m = df_f_mun[['nom', 'prenom', 'nb_voix', 'candidat_id']]
        df_ins_m['num_tour'], df_ins_m['annee'] = 1, 2020
        df_ins_m.insert(0, 'id', range(1, len(df_ins_m) + 1))
        df_ins_m.to_sql('Resultats_Municipales', engine, if_exists='append', index=False)
    
    print("🚀 ETL Terminé avec succès ! Toutes les données sont en base.")

if __name__ == "__main__":
    try:
        executer_etl_mcd()
    except Exception as e:
        print(f"❌ Erreur critique lors de l'exécution : {e}")