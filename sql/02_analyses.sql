-- =============================================================================
--  REQUÊTES ANALYTIQUES — ENTREPÔT ÉPIDÉMIOLOGIQUE DU CHU DE TREICHVILLE
-- =============================================================================
--  Six requêtes, dont deux mobilisent des fonctions de fenêtrage.
--  Chacune répond à une question posée par la direction de l'établissement.
-- =============================================================================


-- =============================================================================
--  R1 — TAUX D'OCCUPATION MENSUEL PAR SERVICE
-- -----------------------------------------------------------------------------
--  Question : quels services sont saturés, et à quelle période ?
--
--  Le taux d'occupation est le rapport entre les journées d'hospitalisation
--  réellement consommées et les journées-lits disponibles. Compter les
--  admissions ne suffirait pas : 100 accouchements de 2 jours mobilisent
--  moins de ressources que 30 AVC de 14 jours.
-- =============================================================================
WITH jours_par_mois AS (
    -- Nombre de jours calendaires de chaque mois, lu dans la dimension date
    -- plutôt que calculé : c'est le rôle d'une dimension temporelle.
    SELECT annee_mois, COUNT(*) AS nb_jours
    FROM dim_date
    GROUP BY annee_mois
),
activite AS (
    SELECT
        s.libelle_service,
        d.annee_mois,
        s.capacite_lits,
        COUNT(*)                        AS nb_admissions,
        ROUND(SUM(f.duree_sejour_j), 0) AS journees_hospitalisation
    FROM faits_admissions f
    JOIN dim_service s ON s.service_id = f.service_id
    JOIN dim_date    d ON d.date_id    = f.date_id
    GROUP BY s.libelle_service, d.annee_mois, s.capacite_lits
)
SELECT
    a.libelle_service,
    a.annee_mois,
    a.nb_admissions,
    a.journees_hospitalisation,
    a.capacite_lits * j.nb_jours AS journees_lits_disponibles,
    ROUND(100.0 * a.journees_hospitalisation
          / (a.capacite_lits * j.nb_jours), 1) AS taux_occupation_pct
FROM activite a
JOIN jours_par_mois j ON j.annee_mois = a.annee_mois
ORDER BY taux_occupation_pct DESC
LIMIT 12;


-- =============================================================================
--  R2 — SAISONNALITÉ DU PALUDISME (fonction de fenêtrage LAG)
-- -----------------------------------------------------------------------------
--  Question : de combien les admissions pour paludisme augmentent-elles
--  d'un mois à l'autre, et à quel moment de l'année faut-il renforcer les
--  stocks d'antipaludiques ?
--
--  LAG() récupère la valeur du mois précédent sur la même ligne, ce qui
--  permet de calculer une variation sans auto-jointure de la table sur
--  elle-même. C'est plus lisible et nettement plus rapide.
-- =============================================================================
WITH admissions_mensuelles AS (
    SELECT
        d.annee_mois,
        d.libelle_mois,
        d.saison,
        COUNT(*) AS nb_cas
    FROM faits_admissions f
    JOIN dim_date       d ON d.date_id       = f.date_id
    JOIN dim_pathologie p ON p.pathologie_id = f.pathologie_id
    WHERE p.libelle_pathologie IN ('Paludisme simple', 'Paludisme grave')
    GROUP BY d.annee_mois, d.libelle_mois, d.saison
)
SELECT
    annee_mois,
    libelle_mois,
    saison,
    nb_cas,
    LAG(nb_cas) OVER (ORDER BY annee_mois) AS nb_cas_mois_precedent,
    ROUND(100.0 * (nb_cas - LAG(nb_cas) OVER (ORDER BY annee_mois))
          / NULLIF(LAG(nb_cas) OVER (ORDER BY annee_mois), 0), 1)
        AS variation_pct
FROM admissions_mensuelles
ORDER BY annee_mois;


-- =============================================================================
--  R3 — DURÉE MOYENNE DE SÉJOUR ET LÉTALITÉ PAR PATHOLOGIE ET TRANCHE D'ÂGE
-- -----------------------------------------------------------------------------
--  Question : quelles combinaisons pathologie × âge concentrent le risque ?
--
--  Le HAVING écarte les combinaisons de moins de 100 séjours : un taux de
--  létalité calculé sur 7 cas n'a aucune valeur statistique et ferait
--  remonter du bruit en tête de classement.
-- =============================================================================
SELECT
    p.libelle_pathologie,
    t.libelle_tranche,
    COUNT(*)                                          AS nb_sejours,
    ROUND(AVG(f.duree_sejour_j)::numeric, 1)          AS duree_moyenne_j,
    ROUND(AVG(f.cout_hospitalisation_fcfa)::numeric, 0) AS cout_moyen_fcfa,
    COUNT(*) FILTER (WHERE i.est_deces)               AS nb_deces,
    ROUND(100.0 * COUNT(*) FILTER (WHERE i.est_deces) / COUNT(*), 2)
        AS taux_letalite_pct
FROM faits_admissions f
JOIN dim_pathologie  p ON p.pathologie_id = f.pathologie_id
JOIN dim_tranche_age t ON t.tranche_id    = f.tranche_id
JOIN dim_issue       i ON i.issue_id      = f.issue_id
GROUP BY p.libelle_pathologie, t.libelle_tranche
HAVING COUNT(*) >= 100
ORDER BY taux_letalite_pct DESC
LIMIT 12;


-- =============================================================================
--  R4 — CHARGE FINANCIÈRE PAR COUVERTURE ET PAR COMMUNE
-- -----------------------------------------------------------------------------
--  Question : les patients sans couverture maladie sont-ils concentrés dans
--  certaines communes, et quel volume financier représentent-ils ?
--
--  Enjeu de politique publique : cibler le déploiement de la Couverture
--  Maladie Universelle sur les communes où le reste à charge est le plus lourd.
-- =============================================================================
--  Attention au piège : la part de chaque couverture doit être calculée
--  AVANT le filtre sur les patients sans assurance. Filtrer en amont
--  ramènerait mécaniquement toutes les parts à 100 %, la fenêtre ne voyant
--  plus qu'une seule modalité par commune. D'où le découpage en deux CTE.
WITH par_commune_et_couverture AS (
    SELECT
        c.libelle_commune,
        f.assurance,
        COUNT(*)                                            AS nb_sejours,
        ROUND(AVG(f.cout_hospitalisation_fcfa)::numeric, 0) AS cout_moyen_fcfa,
        SUM(f.cout_hospitalisation_fcfa)                    AS cout_total_fcfa
    FROM faits_admissions f
    JOIN dim_commune c ON c.commune_id = f.commune_id
    GROUP BY c.libelle_commune, f.assurance
),
avec_part AS (
    SELECT *,
           ROUND(100.0 * nb_sejours
                 / SUM(nb_sejours) OVER (PARTITION BY libelle_commune), 1)
               AS part_dans_la_commune_pct
    FROM par_commune_et_couverture
)
SELECT libelle_commune, assurance, nb_sejours, part_dans_la_commune_pct,
       cout_moyen_fcfa, cout_total_fcfa
FROM avec_part
WHERE assurance = 'Aucune'
ORDER BY cout_total_fcfa DESC
LIMIT 10;


-- =============================================================================
--  R5 — TOP 3 DES PATHOLOGIES DE CHAQUE SERVICE (fonction de fenêtrage RANK)
-- -----------------------------------------------------------------------------
--  Question : sur quels diagnostics chaque service doit-il concentrer ses
--  moyens de formation et son approvisionnement ?
--
--  RANK() ... PARTITION BY établit un classement redémarrant à chaque
--  service. Sans fenêtrage, il faudrait une requête corrélée par service.
--  SUM(COUNT(*)) OVER (...) donne le total du service sur chaque ligne,
--  ce qui permet d'exprimer la part de chaque pathologie.
-- =============================================================================
WITH classement AS (
    SELECT
        s.libelle_service,
        p.libelle_pathologie,
        p.categorie,
        COUNT(*) AS nb_sejours,
        RANK() OVER (PARTITION BY s.libelle_service
                     ORDER BY COUNT(*) DESC) AS rang,
        ROUND(100.0 * COUNT(*)
              / SUM(COUNT(*)) OVER (PARTITION BY s.libelle_service), 1)
            AS part_du_service_pct
    FROM faits_admissions f
    JOIN dim_service    s ON s.service_id    = f.service_id
    JOIN dim_pathologie p ON p.pathologie_id = f.pathologie_id
    GROUP BY s.libelle_service, p.libelle_pathologie, p.categorie
)
SELECT libelle_service, rang, libelle_pathologie, categorie,
       nb_sejours, part_du_service_pct
FROM classement
WHERE rang <= 3
ORDER BY libelle_service, rang;


-- =============================================================================
--  R6 — TAUX DE RÉADMISSION À 30 JOURS PAR CATÉGORIE DE PATHOLOGIE
-- -----------------------------------------------------------------------------
--  Question : quels patients reviennent le plus vite après leur sortie ?
--
--  La réadmission à 30 jours est un indicateur de qualité des soins reconnu
--  internationalement : un taux élevé signale une sortie prématurée ou un
--  suivi ambulatoire défaillant. Il est calculé ici sur la colonne
--  est_readmission_30j produite lors de l'enrichissement, elle-même issue
--  du chaînage des séjours par identifiant patient pseudonymisé.
-- =============================================================================
SELECT
    p.categorie,
    COUNT(*)                                            AS nb_sejours,
    COUNT(DISTINCT f.id_patient_pseudo)                 AS nb_patients,
    ROUND(COUNT(*)::numeric
          / COUNT(DISTINCT f.id_patient_pseudo), 2)     AS sejours_par_patient,
    COUNT(*) FILTER (WHERE f.est_readmission_30j)       AS nb_readmissions_30j,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.est_readmission_30j)
          / COUNT(*), 2)                                AS taux_readmission_pct,
    ROUND(AVG(f.delai_readmission_j)::numeric, 0)       AS delai_moyen_retour_j
FROM faits_admissions f
JOIN dim_pathologie p ON p.pathologie_id = f.pathologie_id
GROUP BY p.categorie
ORDER BY taux_readmission_pct DESC;
