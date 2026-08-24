# Entrepôt épidémiologique — CHU de Treichville

> Projet de fin de module **Data Engineering** — Sujet 3, santé publique.

**Etudiant :** KOUAME Guy Marc Axel
**Enseignant :** GOUAH Tato Serge — 

---

## Objet

Construction d'un entrepôt de données épidémiologiques pour le CHU de
Treichville, permettant d'analyser les admissions, les pathologies et
l'occupation des services.

Le pipeline couvre l'ensemble de la chaîne : extraction de l'export brut du
système d'information hospitalier, nettoyage, pseudonymisation réglementaire,
chargement dans un entrepôt dimensionnel, analyses SQL et restitution.


## Structure du dépôt

```
.
├── notebooks/      Pipeline exécutable de bout en bout
├── src/            Code métier (générateur, ETL, restitution)
├── sql/            DDL du schéma en étoile et requêtes analytiques
├── dags/           Orchestration Airflow
├── docker/         Conteneurisation
├── dashboards/     Graphiques exportés
├── docs/           Cadrage et notes de conception
├── rapport/        Rapport technique et annexes
└── data/           Données locales (non versionnées)
```

## Environnement

Le projet est développé sous **Anaconda**. Deux fichiers décrivent
l'environnement, chacun pour un usage distinct :

| Fichier | Usage |
|---|---|
| `environment.yml` | Environnement conda de développement |
| `requirements.txt` | Dépendances pip, lues par l'image Docker qui exécute Airflow |

### Installation avec conda

```bash
conda env create -f environment.yml
conda activate chu-data-eng
```

Si vous disposez déjà d'un environnement de data science, ajoutez-y simplement
les dépendances du projet :

```bash
conda activate <votre-environnement>
conda install -c conda-forge pandas numpy
```

### Installation avec pip

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### Noyau Jupyter

Pour que le notebook s'exécute dans le bon environnement :

```bash
python -m ipykernel install --user --name chu-data-eng \
       --display-name "Python (CHU Data Eng)"
```

## Génération du jeu de données

Le fichier source (25 Mo) n'est pas versionné : le dépôt contient le générateur
qui le reproduit à l'identique.

```bash
python src/generate_dataset.py
mv admissions_chu_treichville.csv data/raw/
```

