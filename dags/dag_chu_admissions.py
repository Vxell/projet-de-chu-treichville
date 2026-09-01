"""
=============================================================================
 DAG AIRFLOW — PIPELINE QUOTIDIEN DES ADMISSIONS DU CHU DE TREICHVILLE
=============================================================================
 Orchestration de l'alimentation de l'entrepôt épidémiologique.

 Enchaînement : extraction → transformation → contrôle qualité → chargement
                → restitution

 Le DAG n'implémente aucune logique métier : il appelle les fonctions du
 module src/pipeline_etl.py, celles-là mêmes qui sont exécutées dans le
 notebook. Cette séparation garantit que le pipeline automatisé et le
 pipeline exploratoire produisent exactement le même résultat.

 Les données transitent d'une tâche à l'autre par fichiers Parquet et non par
 XCom : XCom stocke ses valeurs dans la base de métadonnées d'Airflow et
 n'est dimensionné que pour de petits volumes (quelques kilo-octets). Y faire
 passer 120 000 lignes saturerait la base de métadonnées. Seuls les chemins
 de fichiers et les compteurs circulent par XCom.
=============================================================================
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Le module ETL est monté dans le conteneur Airflow sous /opt/airflow/src
sys.path.insert(0, "/opt/airflow/src")

REPERTOIRE_TRAVAIL = "/opt/airflow/data/processed"
FICHIER_SOURCE = os.environ.get(
    "DE_FICHIER_SOURCE", "/opt/airflow/data/raw/admissions_chu_treichville.csv")

# =============================================================================
#  PARAMÈTRES PAR DÉFAUT
# =============================================================================
#  retries=2 avec un délai croissant : les échecs de chargement observés sont
#  presque toujours des coupures réseau vers Supabase, résolues par une simple
#  reprise. Une alerte n'est envoyée qu'après épuisement des tentatives, pour
#  ne pas noyer l'exploitant sous des alertes sans objet.
# =============================================================================
default_args = {
    "owner": "equipe-data-chu",
    "depends_on_past": False,
    "email": ["data@chu-treichville.ci"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}


# =============================================================================
#  TÂCHES
# =============================================================================

def tache_extraire(**contexte):
    """
    Extrait le fichier du jour et en produit un audit.

    L'audit est conservé à chaque exécution : c'est lui qui permettra de
    détecter une dérive progressive de la qualité de la source (un champ qui
    se vide petit à petit du fait d'un changement dans le SIH).
    """
    from pipeline_etl import extraire, resumer_audit

    df = extraire(FICHIER_SOURCE)
    resume = resumer_audit(df)

    os.makedirs(REPERTOIRE_TRAVAIL, exist_ok=True)
    chemin = f"{REPERTOIRE_TRAVAIL}/01_brut.parquet"
    df.to_parquet(chemin, index=False)

    # Seules les métadonnées transitent par XCom
    contexte["ti"].xcom_push(key="audit", value=resume)
    contexte["ti"].xcom_push(key="chemin", value=chemin)
    return resume["lignes"]


def tache_transformer(**contexte):
    """
    Applique le nettoyage, la pseudonymisation et l'enrichissement.

    Le sel de pseudonymisation est lu dans une variable d'environnement du
    conteneur, alimentée par un secret Docker. Il n'apparaît ni dans le code,
    ni dans les journaux d'Airflow.
    """
    import pandas as pd
    from pipeline_etl import (JOURNAL, enrichir, nettoyer, pseudonymiser,
                              verifier_absence_donnees_personnelles)

    chemin = contexte["ti"].xcom_pull(task_ids="extraire", key="chemin")
    df = pd.read_parquet(chemin)

    df = nettoyer(df)
    df, audit_rgpd = pseudonymiser(df)
    df = enrichir(df)
    verifier_absence_donnees_personnelles(df)

    df.to_parquet(f"{REPERTOIRE_TRAVAIL}/02_transforme.parquet", index=False)
    audit_rgpd.to_parquet(f"{REPERTOIRE_TRAVAIL}/02_audit_rgpd.parquet",
                          index=False)
    pd.DataFrame(JOURNAL).to_csv(
        f"{REPERTOIRE_TRAVAIL}/02_journal_nettoyage.csv", index=False)

    contexte["ti"].xcom_push(key="lignes", value=len(df))
    return len(df)


def tache_controler_qualite(**contexte):
    """
    Applique les huit règles de validation.

    Une règle bloquante en échec lève une exception : la tâche de chargement
    ne s'exécutera pas et l'entrepôt conservera les données de la veille.
    Charger des données invalides serait pire que ne rien charger — les
    analyses en aval seraient fausses sans que personne ne s'en aperçoive.
    """
    import pandas as pd
    from pipeline_etl import controler_qualite

    df = pd.read_parquet(f"{REPERTOIRE_TRAVAIL}/02_transforme.parquet")
    rapport = controler_qualite(df, bloquant=True)
    rapport.to_csv(f"{REPERTOIRE_TRAVAIL}/03_rapport_qualite.csv", index=False)

    avertissements = int((rapport["statut"] == "AVERTISSEMENT").sum())
    contexte["ti"].xcom_push(key="avertissements", value=avertissements)
    return f"{len(rapport)} règles évaluées, {avertissements} avertissement(s)"


def tache_charger(**contexte):
    """Construit le schéma en étoile et le charge dans Supabase."""
    import pandas as pd
    from pipeline_etl import (charger_entrepot, construire_dimensions,
                              construire_faits, obtenir_moteur)

    df = pd.read_parquet(f"{REPERTOIRE_TRAVAIL}/02_transforme.parquet")
    audit_rgpd = pd.read_parquet(f"{REPERTOIRE_TRAVAIL}/02_audit_rgpd.parquet")

    dims = construire_dimensions(df)
    faits = construire_faits(df, dims)
    durees = charger_entrepot(dims, faits, audit_rgpd, obtenir_moteur())

    contexte["ti"].xcom_push(key="duree_chargement_s",
                             value=round(sum(durees.values()), 1))
    return len(faits)


def tache_restituer(**contexte):
    """Régénère le tableau de bord à partir des données fraîchement chargées."""
    import pandas as pd
    from dashboard import generer_dashboard
    from pipeline_etl import construire_dimensions, construire_faits

    df = pd.read_parquet(f"{REPERTOIRE_TRAVAIL}/02_transforme.parquet")
    dims = construire_dimensions(df)
    faits = construire_faits(df, dims)
    fichiers = generer_dashboard(dims, faits, dossier="/opt/airflow/dashboards")
    return f"{len(fichiers)} graphiques régénérés"


# =============================================================================
#  DÉFINITION DU DAG
# =============================================================================

with DAG(
    dag_id="chu_treichville_admissions",
    description="Alimentation quotidienne de l'entrepôt épidémiologique",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # Exécution à 02h00 chaque jour : le SIH clôture ses écritures à minuit,
    # deux heures de marge évitent de lire un extrait incomplet.
    schedule="0 2 * * *",
    catchup=False,          # pas de rattrapage des exécutions passées
    max_active_runs=1,      # une seule exécution simultanée : la base cible
                            # est en écriture exclusive pendant le chargement
    tags=["sante", "etl", "supabase", "chu-treichville"],
) as dag:

    debut = EmptyOperator(task_id="debut")

    extraire = PythonOperator(
        task_id="extraire",
        python_callable=tache_extraire,
        doc_md="Lecture du fichier source du SIH et audit de complétude.",
    )

    transformer = PythonOperator(
        task_id="transformer",
        python_callable=tache_transformer,
        doc_md="Nettoyage, pseudonymisation RGPD et enrichissement.",
    )

    controler = PythonOperator(
        task_id="controler_qualite",
        python_callable=tache_controler_qualite,
        doc_md="Huit règles de validation, dont six bloquantes.",
    )

    charger = PythonOperator(
        task_id="charger_supabase",
        python_callable=tache_charger,
        doc_md="Construction du schéma en étoile et chargement PostgreSQL.",
    )

    restituer = PythonOperator(
        task_id="generer_dashboard",
        python_callable=tache_restituer,
        doc_md="Régénération des six graphiques du tableau de bord.",
    )

    fin = EmptyOperator(task_id="fin")

    # Enchaînement linéaire : chaque étape dépend du succès de la précédente
    debut >> extraire >> transformer >> controler >> charger >> restituer >> fin
