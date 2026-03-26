# 📊 MSPR – Prédiction des tendances électorales

**Electio-Analytics | Big Data & Business Intelligence**

---

## 🧠 Contexte

Dans le cadre de la certification **RNCP35584 – Expert en Informatique et Systèmes d’Information**, ce projet vise à réaliser une **preuve de concept (POC)** permettant de prédire des tendances électorales à partir de données publiques.

L’objectif est d’aider la société *Electio-Analytics* à exploiter des données socio-économiques pour **anticiper les comportements électoraux**.

---

## 🎯 Objectif du projet

Le projet consiste à :

* Collecter des données électorales et socio-économiques
* Les intégrer dans une base de données PostgreSQL
* Nettoyer et normaliser les données
* Produire un dataset exploitable
* Préparer les données pour une future analyse et modélisation prédictive

👉 Ce dépôt se concentre principalement sur la **phase ETL (Extract – Transform – Load)**.

---

## 📂 Structure du projet

```
MSPR-prediction-election/
│
├── input/                              # Données brutes (CSV)
├── output/                             # Données nettoyées / transformées
│
├── Import_des_donnees_basePostgres.py  # Script d'import en base PostgreSQL
├── Nettoyage.py                        # Script de traitement et nettoyage
│
└── README.md
```

---

## ⚙️ Fonctionnement global

Le projet repose sur un pipeline de traitement des données en deux étapes principales :

```
CSV bruts (input)
        ↓
Import en base PostgreSQL
        ↓
Nettoyage et transformation
        ↓
Export CSV propres (output)
```

---

## 🧩 1. Import des données (`Import_des_donnees_basePostgres.py`)

### 📌 Rôle

Ce script permet de charger les données brutes dans une base de données **PostgreSQL**.

### ⚙️ Fonctionnement

* Lecture des fichiers CSV présents dans le dossier `input/`
* Connexion à une base PostgreSQL
* Création ou alimentation de tables
* Insertion des données dans la base

### 🎯 Objectif

* Centraliser les données
* Structurer l’information
* Préparer un environnement exploitable pour la BI

👉 Cette étape correspond à la phase **LOAD** d’un pipeline ETL.

---

## 🧹 2. Nettoyage des données (`Nettoyage.py`)

### 📌 Rôle

Ce script assure la **qualité des données** en les nettoyant et en les transformant.

### 🔧 Traitements réalisés

* Suppression ou gestion des valeurs manquantes
* Harmonisation des formats (colonnes, types)
* Filtrage des données incohérentes
* Normalisation des variables
* Structuration des datasets

### 📤 Résultat

* Génération de fichiers propres dans le dossier `output/`

👉 Ces données sont prêtes pour :

* analyse exploratoire
* modélisation
* visualisation (Power BI, Python, etc.)

---

## 🏗️ Architecture Data

Le projet met en place une architecture simple mais conforme aux attentes BI :

* **Sources** : fichiers CSV (open data)
* **Stockage intermédiaire** : PostgreSQL
* **Traitement** : scripts Python
* **Sortie** : datasets propres (CSV)

👉 Cette architecture permet :

* la traçabilité
* la reproductibilité
* la scalabilité

---

## 📊 Qualité des données

Des actions sont mises en place pour garantir :

* Cohérence des données
* Fiabilité des informations
* Uniformisation des formats
* Réduction des erreurs

👉 Répond aux exigences MSPR :

* qualité
* gouvernance des données
* exploitabilité

---

## 🔐 Sécurité & conformité

* Utilisation d’une base PostgreSQL (gestion des accès possible)
* Données issues de sources publiques
* Respect des principes RGPD (pas de données personnelles sensibles)

---

## 🚀 Utilisation

### 1. Pré-requis

* Python 3.x
* PostgreSQL installé et configuré
* Bibliothèques Python (cf. scripts - ex : pandas, psycopg2)

---

### 2. Étapes d’exécution


#### 🧹 Étape 1 : Nettoyage

```bash
python Nettoyage.py
```

#### 📥 Étape 2 : Import des données

```bash
python Import_des_donnees_basePostgres.py
```

---

## 📈 Exploitation des données

Les données générées dans `output/` peuvent être utilisées pour :

* Analyse exploratoire
* Création de dashboards (Power BI)
* Modélisation prédictive (Machine Learning)

---

## 🧪 Perspectives d’évolution

* Ajout d’un modèle prédictif (machine learning)
* Automatisation complète du pipeline ETL
* Intégration dans un Data Warehouse
* Création de dashboards interactifs
* Ajout de nouvelles sources (INSEE, sécurité, emploi…)

---

## 📌 Alignement avec les compétences MSPR

✔ Collecte et structuration des données
✔ Mise en place d’un pipeline ETL
✔ Création d’une base de données exploitable
✔ Qualité et gouvernance des données
✔ Préparation à l’analyse décisionnelle

---

## 👥 Projet pédagogique

Projet réalisé dans le cadre de la MSPR Big Data & BI.

---

## 💡 Remarque

Ce dépôt constitue une **brique technique (data engineering)** du projet global.
La partie **modélisation prédictive et visualisation** peut être développée en complément.
