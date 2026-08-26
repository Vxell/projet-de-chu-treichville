-- =============================================================================
--  ENTREPÔT ÉPIDÉMIOLOGIQUE — CHU DE TREICHVILLE
--  Schéma en étoile : 6 dimensions + 1 table de faits
--  Cible : PostgreSQL 15 (Supabase)
-- =============================================================================
--  Ce script est idempotent : il peut être rejoué sans erreur. Il est exécuté
--  une seule fois, avant le premier chargement du pipeline.
--
--  Choix de modélisation
--  --------------------
--  Schéma en étoile plutôt qu'en flocon : les dimensions sont volontairement
--  dénormalisées (dim_pathologie porte sa catégorie, dim_date porte sa saison).
--  Cela crée une redondance limitée — 31 lignes dans dim_pathologie — mais
--  supprime un niveau de jointure sur chaque requête analytique. Sur une table
--  de faits de 120 000 lignes interrogée par des outils de restitution, ce
--  compromis favorise la lisibilité des requêtes et le temps de réponse.
-- =============================================================================

DROP TABLE IF EXISTS faits_admissions CASCADE;
DROP TABLE IF EXISTS dim_date        CASCADE;
DROP TABLE IF EXISTS dim_service     CASCADE;
DROP TABLE IF EXISTS dim_pathologie  CASCADE;
DROP TABLE IF EXISTS dim_tranche_age CASCADE;
DROP TABLE IF EXISTS dim_commune     CASCADE;
DROP TABLE IF EXISTS dim_issue       CASCADE;

-- =============================================================================
--  DIMENSIONS
-- =============================================================================

-- Dimension temporelle : une ligne par jour de la période couverte.
-- La clé est au format AAAAMMJJ, lisible directement lors du débogage.
CREATE TABLE dim_date (
    date_id      INTEGER      PRIMARY KEY,
    date_jour    DATE         NOT NULL,
    jour         SMALLINT     NOT NULL,
    mois         SMALLINT     NOT NULL,
    libelle_mois VARCHAR(12)  NOT NULL,
    trimestre    SMALLINT     NOT NULL,
    annee        SMALLINT     NOT NULL,
    annee_mois   VARCHAR(7)   NOT NULL,
    jour_semaine VARCHAR(10)  NOT NULL,
    est_weekend  BOOLEAN      NOT NULL,
    saison       VARCHAR(30)  NOT NULL   -- calendrier climatique ivoirien
);

-- Dimension service, enrichie de la capacité en lits : sans cette donnée,
-- on ne peut produire qu'un volume d'activité, jamais un taux d'occupation.
CREATE TABLE dim_service (
    service_id      INTEGER     PRIMARY KEY,
    libelle_service VARCHAR(50) NOT NULL UNIQUE,
    capacite_lits   SMALLINT    NOT NULL
);

-- Dimension pathologie avec sa hiérarchie : 31 diagnostics regroupés
-- en 5 catégories épidémiologiques.
CREATE TABLE dim_pathologie (
    pathologie_id      INTEGER     PRIMARY KEY,
    libelle_pathologie VARCHAR(60) NOT NULL UNIQUE,
    categorie          VARCHAR(50) NOT NULL
);

-- Dimension tranche d'âge : la colonne ordre permet de trier les tranches
-- dans l'ordre naturel plutôt que dans l'ordre alphabétique des libellés.
CREATE TABLE dim_tranche_age (
    tranche_id      INTEGER     PRIMARY KEY,
    ordre           SMALLINT    NOT NULL,
    libelle_tranche VARCHAR(40) NOT NULL UNIQUE,
    borne_min       SMALLINT,
    borne_max       SMALLINT
);

-- Dimension géographique : communes du district autonome d'Abidjan.
CREATE TABLE dim_commune (
    commune_id      INTEGER     PRIMARY KEY,
    libelle_commune VARCHAR(40) NOT NULL UNIQUE,
    district        VARCHAR(30) NOT NULL
);

-- Dimension issue du séjour. L'indicateur est_deces évite de coder la
-- comparaison textuelle dans chaque requête de mortalité.
CREATE TABLE dim_issue (
    issue_id      INTEGER     PRIMARY KEY,
    code_issue    VARCHAR(30) NOT NULL UNIQUE,
    libelle_issue VARCHAR(40) NOT NULL,
    est_deces     BOOLEAN     NOT NULL
);

-- =============================================================================
--  TABLE DE FAITS
-- =============================================================================
--  Grain : une ligne = une admission hospitalière.
--
--  id_patient_pseudo est une dimension dégénérée : stockée directement dans
--  la table de faits plutôt que dans une dimension patient. Une dimension
--  patient ne porterait aucun attribut exploitable — tous ont été supprimés
--  ou généralisés par la pseudonymisation — et constituerait un point de
--  concentration de données sensibles inutile.
-- =============================================================================

CREATE TABLE faits_admissions (
    id_admission              VARCHAR(12) PRIMARY KEY,

    -- Clés étrangères vers les dimensions
    date_id                   INTEGER     NOT NULL REFERENCES dim_date(date_id),
    service_id                INTEGER     NOT NULL REFERENCES dim_service(service_id),
    pathologie_id             INTEGER     NOT NULL REFERENCES dim_pathologie(pathologie_id),
    tranche_id                INTEGER     NOT NULL REFERENCES dim_tranche_age(tranche_id),
    commune_id                INTEGER     NOT NULL REFERENCES dim_commune(commune_id),
    issue_id                  INTEGER     NOT NULL REFERENCES dim_issue(issue_id),

    -- Dimension dégénérée
    id_patient_pseudo         VARCHAR(16) NOT NULL,

    -- Attributs de contexte
    sexe                      VARCHAR(15),
    age                       SMALLINT,
    annee_naissance           SMALLINT,
    mode_admission            VARCHAR(20),
    gravite                   VARCHAR(15),
    assurance                 VARCHAR(30),
    id_medecin                VARCHAR(15),
    heure_admission           SMALLINT,

    -- Mesures
    duree_sejour_j            NUMERIC(6,1) NOT NULL,
    cout_hospitalisation_fcfa NUMERIC(12,0),
    cout_journalier_fcfa      NUMERIC(12,0),
    temperature_entree_c      NUMERIC(4,1),
    sejour_prolonge           BOOLEAN,
    est_readmission_30j       BOOLEAN,
    delai_readmission_j       NUMERIC(8,1),

    -- Traçabilité : distingue les mesures observées des mesures imputées
    duree_imputee             BOOLEAN,

    CONSTRAINT chk_duree CHECK (duree_sejour_j > 0),
    CONSTRAINT chk_age   CHECK (age IS NULL OR age BETWEEN 0 AND 110)
);

-- =============================================================================
--  INDEX
-- =============================================================================
--  Créés APRÈS le chargement : les maintenir pendant l'insertion de
--  120 000 lignes multiplierait le temps de chargement.

CREATE INDEX idx_faits_date     ON faits_admissions(date_id);
CREATE INDEX idx_faits_service  ON faits_admissions(service_id);
CREATE INDEX idx_faits_patho    ON faits_admissions(pathologie_id);
CREATE INDEX idx_faits_patient  ON faits_admissions(id_patient_pseudo);

-- =============================================================================
--  TABLE D'AUDIT RGPD (hors schéma en étoile)
-- =============================================================================
--  Trace les traitements de pseudonymisation appliqués à chaque exécution.
--  Elle constitue la preuve de conformité exigée par la loi n° 2019-992 :
--  elle documente quelle colonne a subi quel traitement, quand, et sur
--  quel volume.

CREATE TABLE IF NOT EXISTS audit_rgpd (
    execution_id       VARCHAR(10),
    horodatage         TIMESTAMP,
    colonne            VARCHAR(40),
    operation          VARCHAR(50),
    justification      TEXT,
    nb_lignes_traitees INTEGER
);
