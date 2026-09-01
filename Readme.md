# Entrepôt épidémiologique — CHU de Treichville

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.2-150458?logo=pandas&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Supabase-PostgreSQL_15-3ECF8E?logo=supabase&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Licence](https://img.shields.io/badge/Usage-Académique-lightgrey)

> Projet de fin de module **Data Engineering** — Sujet 3, santé publique.
> Pipeline complet d'alimentation d'un entrepôt épidémiologique, de l'export
> brut du système d'information hospitalier jusqu'au tableau de bord.

**Binôme :** *[à compléter]* · **Enseignant :** GOUAH Tato Serge · **Promotion 2024-2025**

---

## Contexte

La Direction Générale de la Santé confie au CHU de Treichville la construction
d'un entrepôt épidémiologique permettant d'analyser les admissions, les
pathologies et l'occupation des services, afin d'optimiser l'allocation des
ressources hospitalières.

Deux contraintes structurent le projet :

- **Réglementaire** — les données de santé sont des données sensibles au sens de
  la loi ivoirienne n° 2019-992. Aucune donnée identifiante ne peut entrer dans
  l'entrepôt.
- **Qualité** — l'export du SIH est un fichier brut, alimenté par des dizaines
  d'agents de saisie : codifications divergentes, doublons, valeurs aberrantes.

---

## Architecture du pipeline

```
┌──────────────────┐
│  SIH — CHU       │   Export CSV brut
│  Treichville     │   122 400 lignes · 21 colonnes · 24,7 Mo
└────────┬─────────┘
         │
    ┌────▼─────────────────────────────────────────────────────┐
    │  EXTRACTION            src/pipeline_etl.py               │
    │  Audit de complétude, cardinalité, doublons              │
    └────┬─────────────────────────────────────────────────────┘
         │
    ┌────▼─────────────────────────────────────────────────────┐
    │  TRANSFORMATION        Pandas                            │
    │  1. Dédoublonnage           4. Valeurs aberrantes        │
    │  2. Normalisation           5. Incohérences              │
    │  3. Typage                  6. Imputation par pathologie │
    └────┬─────────────────────────────────────────────────────┘
         │
    ┌────▼─────────────────────────────────────────────────────┐
    │  PSEUDONYMISATION      Loi n° 2019-992                   │
    │  SHA-256 salé · suppression · généralisation             │
    │  → journal d'audit horodaté                              │
    └────┬─────────────────────────────────────────────────────┘
         │
    ┌────▼─────────────────────────────────────────────────────┐
    │  ENRICHISSEMENT        7 colonnes calculées              │
    │  CONTRÔLE QUALITÉ      8 règles, dont 6 bloquantes       │
    └────┬─────────────────────────────────────────────────────┘
         │
    ┌────▼─────────────────────────────────────────────────────┐
    │  CHARGEMENT            Supabase / PostgreSQL 15          │
    │  Schéma en étoile : 6 dimensions + 1 table de faits      │
    └────┬─────────────────────────────────────────────────────┘
         │
    ┌────▼──────────────┐        ┌───────────────────────────┐
    │  ANALYSES SQL     │        │  TABLEAU DE BORD          │
    │  6 requêtes       │        │  6 graphiques Matplotlib  │
    │  dont 3 fenêtrées │        │  PNG 150 dpi              │
    └───────────────────┘        └───────────────────────────┘

        Orchestration : Apache Airflow — DAG quotidien à 02h00
        Conteneurisation : Docker Compose — 3 services
```

---

## Résultats clés

| Indicateur | Valeur |
|---|---|
| **Lignes traitées** | 122 400 en entrée → 120 000 chargées |
| **Patients distincts** (pseudonymisés) | 96 286 |
| **Période couverte** | 01/01/2023 → 31/12/2024 (731 jours) |
| **Durée moyenne de séjour** | 6,9 jours |
| **Taux de létalité global** | 6,00 % |
| **Part du paludisme** dans les admissions | 16,2 % |
| **Taux de réadmission à 30 jours** | 2,07 % |
| **Règles de qualité satisfaites** | 7/8 (1 avertissement documenté) |
| **Temps de chargement** | *[à relever sur votre instance]* |

### Trois enseignements

**1. L'activité hospitalière suit le calendrier des pluies.** Les admissions
pour paludisme passent de 398 cas en février à 1 321 en juin, soit une
progression de 232 %. Les pics d'activité de l'établissement sont donc
prévisibles plusieurs semaines à l'avance.

**2. Les urgences franchissent la saturation en juin.** Le taux d'occupation
atteint 101,2 % en juin 2024, contre une moyenne annuelle de 78 %. Le service
fonctionne au-delà de sa capacité déclarée pendant le pic palustre.

**3. Volume et gravité ne se superposent pas.** La tranche 25-44 ans concentre
le plus grand nombre d'admissions avec une létalité de 4,1 %, tandis que les
65 ans et plus, quatre fois moins nombreux, atteignent 17,8 %.

---

## Installation

### Option 1 — Docker (recommandée)

```bash
git clone https://github.com/VOTRE-COMPTE/projet-de-chu-treichville.git
cd projet-de-chu-treichville

cp .env.example .env        # renseigner SUPABASE_DB_URL et DE_SEL_PSEUDO
python src/generate_dataset.py && mv admissions_chu_treichville.csv data/raw/

cd docker
docker compose --env-file ../.env up -d --build
```

| Interface | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Jupyter | http://localhost:8888 | sans jeton |

### Option 2 — Environnement local

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # renseigner les deux variables
python src/generate_dataset.py && mv admissions_chu_treichville.csv data/raw/

jupyter notebook notebooks/pipeline_etl.ipynb
```

Créer ensuite le schéma dans l'éditeur SQL de Supabase :

```bash
# copier-coller le contenu de sql/01_schema_etoile.sql
```

### Variables d'environnement

| Variable | Rôle |
|---|---|
| `SUPABASE_DB_URL` | Connexion PostgreSQL — **port 5432 (direct)**, pas 6543 |
| `DE_SEL_PSEUDO` | Sel de pseudonymisation — à générer et à ne jamais versionner |

> Le sel doit rester **constant entre deux exécutions**, sans quoi le même
> patient recevrait un pseudonyme différent à chaque chargement et le chaînage
> des séjours serait perdu.

---

## Structure du dépôt

```
.
├── notebooks/pipeline_etl.ipynb    Pipeline complet, exécutable de bout en bout
├── src/
│   ├── generate_dataset.py         Générateur du jeu de données source
│   ├── referentiels.py             Tables de correspondance métier
│   ├── pipeline_etl.py             Fonctions ETL — utilisées par le notebook ET le DAG
│   └── dashboard.py                Six graphiques Matplotlib
├── sql/
│   ├── 01_schema_etoile.sql        DDL PostgreSQL du schéma dimensionnel
│   └── 02_analyses.sql             Six requêtes analytiques commentées
├── dags/dag_chu_admissions.py      DAG Airflow — 5 tâches enchaînées
├── docker/
│   ├── Dockerfile                  Image Airflow + dépendances du projet
│   └── docker-compose.yml          PostgreSQL + Airflow + Jupyter
├── dashboards/                     PNG 150 dpi générés par le pipeline
├── rapport/                        Rapport technique
├── requirements.txt
└── .env.example
```

Le fichier CSV (25 Mo) n'est pas versionné : le dépôt contient le générateur qui
le reproduit à l'identique (`random.seed` figé).

---

## Modèle de données

Schéma en étoile — grain : **une ligne = une admission hospitalière**.

| Table | Lignes | Rôle |
|---|---|---|
| `faits_admissions` | 120 000 | Mesures : durée, coût, réadmission |
| `dim_date` | 731 | Calendrier avec saison climatique ivoirienne |
| `dim_pathologie` | 31 | Diagnostics et leur catégorie épidémiologique |
| `dim_service` | 8 | Services et capacité en lits |
| `dim_commune` | 13 | Communes du district d'Abidjan |
| `dim_tranche_age` | 7 | Découpage OMS simplifié |
| `dim_issue` | 4 | Issue du séjour |
| `audit_rgpd` | — | Journal des traitements de pseudonymisation |

`id_patient_pseudo` est une **dimension dégénérée** : stockée dans la table de
faits, car une dimension patient ne porterait aucun attribut exploitable après
pseudonymisation.

---

## Conformité — loi n° 2019-992

| Donnée source | Traitement | Justification |
|---|---|---|
| `id_patient` | SHA-256 salé, tronqué à 16 caractères | Préserve le chaînage des séjours |
| `nom`, `prénom`, `téléphone` | Suppression | Aucune valeur analytique |
| `date_naissance` | Généralisation à l'année | Quasi-identifiant |

Le traitement produit une **pseudonymisation**, non une anonymisation : la
réidentification reste possible pour qui détient le sel. C'est le régime attendu,
le CHU devant pouvoir remonter au dossier patient en cas d'alerte sanitaire.

Un contrôle bloquant (`R2`) vérifie l'absence de toute colonne identifiante avant
chargement. Chaque exécution alimente la table `audit_rgpd`.

---

## Reproduire les résultats

```bash
# Pipeline complet sans chargement en base
python -c "
import sys; sys.path.insert(0, 'src')
from pipeline_etl import executer_pipeline
r = executer_pipeline('data/raw/admissions_chu_treichville.csv')
print(r['journal'])
"

# Tableau de bord seul
python src/dashboard.py
```

---

## Outils d'IA

Conformément à la section 6.3 du sujet : *[à compléter honnêtement — outil
utilisé et tâche concernée]*.
