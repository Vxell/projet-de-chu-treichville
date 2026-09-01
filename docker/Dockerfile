# =============================================================================
#  Image du pipeline ETL — CHU de Treichville
# =============================================================================
#  Construite sur l'image officielle Airflow plutôt que sur une image Python
#  nue : réinstaller Airflow soi-même impose de gérer manuellement le fichier
#  de contraintes de version, source classique d'incompatibilités.
#
#  Corollaire : on n'installe PAS par-dessus les dépendances qu'Airflow
#  contraint lui-même (SQLAlchemy, psycopg2). Voir requirements-airflow.txt.
# =============================================================================

FROM apache/airflow:2.10.3-python3.11

# --- Dépendances système -----------------------------------------------------
# En root : matplotlib et pyarrow exigent des bibliothèques de compilation.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Dépendances Python ------------------------------------------------------
# Retour à l'utilisateur airflow : l'image officielle interdit pip en root.
USER airflow

COPY docker/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# --- Code du projet ----------------------------------------------------------
COPY --chown=airflow:root src/  /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root sql/  /opt/airflow/sql/

# Rend le module ETL importable depuis les tâches du DAG
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"

WORKDIR /opt/airflow
