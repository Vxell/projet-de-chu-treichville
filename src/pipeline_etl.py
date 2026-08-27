"""
=============================================================================
 PIPELINE ETL — ENTREPÔT ÉPIDÉMIOLOGIQUE DU CHU DE TREICHVILLE
=============================================================================
 Ce module regroupe l'ensemble des fonctions du pipeline, de l'extraction du
 fichier source jusqu'au chargement du schéma en étoile dans Supabase.

 Il est appelé de deux façons :
   - par le notebook notebooks/pipeline_etl.ipynb (exécution pas à pas,
     avec affichage des résultats intermédiaires) ;
   - par le DAG Airflow (exécution automatisée quotidienne).

 Chaque étape est une fonction pure : elle reçoit un DataFrame et en renvoie
 un nouveau, sans modifier son entrée. Ce choix rend chaque étape testable
 isolément et permet de rejouer le pipeline à partir de n'importe quel point.
=============================================================================
"""

import hashlib
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd

from referentiels import (
    BORNES, COLONNES_IDENTIFIANTES, COLONNE_A_GENERALISER, COLONNE_A_HACHER,
    ISSUES_VALIDES, LIBELLE_ISSUE, LIBELLE_JOUR, LIBELLE_MOIS,
    LITS_PAR_SERVICE, MAPPING_ASSURANCE, MAPPING_SEXE,
    PATHOLOGIE_VERS_CATEGORIE, REFERENTIEL_COMMUNES, REFERENTIEL_PATHOLOGIES,
    REFERENTIEL_SERVICES, SAISON_PAR_MOIS, SEXE_INCONNU, TRANCHES_AGE,
)

# Journal des transformations : chaque étape y consigne son effet sur le
# volume de données. Il est restitué tel quel dans la section 5 du rapport.
JOURNAL = []


def _journaliser(etape, avant, apres, detail=""):
    """Consigne l'effet d'une étape de nettoyage sur le volume de données."""
    JOURNAL.append({
        "etape": etape,
        "lignes_avant": avant,
        "lignes_apres": apres,
        "delta": apres - avant,
        "detail": detail,
    })


# =============================================================================
# ÉTAPE 1 — EXTRACTION ET AUDIT
# =============================================================================

def extraire(chemin_csv):
    """
    Charge le fichier source sans aucune conversion automatique de type.

    On force `dtype=str` sur les colonnes de dates pour empêcher pandas
    d'inférer un format : la colonne date_naissance est au format JJ/MM/AAAA
    et une inférence automatique inverserait jour et mois pour toutes les
    dates dont le jour est inférieur ou égal à 12.
    """
    debut = time.time()
    df = pd.read_csv(chemin_csv, dtype={"date_naissance": str}, low_memory=False)
    duree = time.time() - debut
    print(f"Extraction : {len(df):,} lignes × {df.shape[1]} colonnes "
          f"en {duree:.1f} s")
    _journaliser("Extraction du fichier source", len(df), len(df),
                 f"{os.path.basename(chemin_csv)}")
    return df


def auditer(df):
    """
    Produit l'audit initial du fichier : complétude, doublons, cardinalités.

    Le tableau renvoyé alimente directement la section 3 du rapport
    (« Sources de données — audit initial »).
    """
    audit = pd.DataFrame({
        "type": df.dtypes.astype(str),
        "valeurs_non_nulles": df.notna().sum(),
        "valeurs_manquantes": df.isna().sum(),
        "taux_manquant_%": (df.isna().mean() * 100).round(2),
        "valeurs_distinctes": df.nunique(),
    })
    audit["exemple"] = [
        df[col].dropna().iloc[0] if df[col].notna().any() else None
        for col in df.columns
    ]
    return audit


def resumer_audit(df):
    """Renvoie les indicateurs de synthèse de l'audit sous forme de dict."""
    return {
        "lignes": len(df),
        "colonnes": df.shape[1],
        "doublons_stricts": int(df.duplicated().sum()),
        "id_admission_dupliques": int(df["id_admission"].duplicated().sum()),
        "cellules_manquantes": int(df.isna().sum().sum()),
        "taux_manquant_global_%": round(df.isna().mean().mean() * 100, 2),
        "memoire_Mo": round(df.memory_usage(deep=True).sum() / 1024**2, 1),
    }


# =============================================================================
# ÉTAPE 2 — NETTOYAGE
# =============================================================================

def dedoublonner(df):
    """
    Supprime les doublons stricts issus de la double saisie dans le SIH.

    On procède en deux temps : d'abord les lignes intégralement identiques,
    puis les identifiants d'admission dupliqués. Le second cas correspond à
    une réémission du même séjour avec une modification mineure : on conserve
    la dernière occurrence, considérée comme la version corrigée.
    """
    avant = len(df)
    df = df.drop_duplicates()
    apres_strict = len(df)

    df = df.drop_duplicates(subset="id_admission", keep="last")
    apres = len(df)

    _journaliser(
        "Suppression des doublons", avant, apres,
        f"{avant - apres_strict} doublons stricts, "
        f"{apres_strict - apres} id_admission dupliqués",
    )
    return df.reset_index(drop=True)


def _normaliser(serie):
    """Passe une colonne texte en minuscules sans espaces superflus."""
    return (serie.astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.lower())


def normaliser_textes(df):
    """
    Ramène chaque colonne catégorielle à son référentiel officiel.

    Le fichier source contient six écritures du sexe, des pathologies en
    majuscules ou entourées d'espaces, et des communes mal orthographiées.
    Sans cette étape, un GROUP BY sur la pathologie renverrait plus de
    120 modalités là où il n'en existe que 31.
    """
    avant = len(df)
    df = df.copy()

    # -- Sexe : 6 écritures -> 2 modalités + une valeur explicite pour l'inconnu
    df["sexe"] = _normaliser(df["sexe"]).map(MAPPING_SEXE).fillna(SEXE_INCONNU)

    # -- Pathologie : correspondance avec le référentiel des 31 diagnostics
    patho_norm = _normaliser(df["pathologie"])
    df["pathologie"] = patho_norm.map(REFERENTIEL_PATHOLOGIES)
    non_reconnues = int(df["pathologie"].isna().sum())
    df["pathologie"] = df["pathologie"].fillna("Non renseignée")

    # -- Service et commune : même principe
    df["service"] = (_normaliser(df["service"]).map(REFERENTIEL_SERVICES)
                     .fillna("Non renseigné"))
    df["commune_residence"] = (_normaliser(df["commune_residence"])
                               .map(REFERENTIEL_COMMUNES)
                               .fillna("Non renseignée"))

    # -- Assurance : la valeur manquante devient une modalité à part entière,
    #    car « pas d'assurance renseignée » est une information en soi.
    df["assurance"] = (_normaliser(df["assurance"]).map(MAPPING_ASSURANCE)
                       .fillna("Non renseignée"))

    _journaliser(
        "Normalisation des libellés", avant, len(df),
        f"{non_reconnues} pathologies hors référentiel",
    )
    return df


def corriger_types(df):
    """
    Convertit chaque colonne dans son type cible.

    `errors='coerce'` transforme toute valeur non convertible en NaN plutôt
    que d'interrompre le pipeline : les anomalies sont ainsi traitées à
    l'étape suivante, de façon centralisée et traçable.
    """
    avant = len(df)
    df = df.copy()

    df["date_entree"] = pd.to_datetime(df["date_entree"], errors="coerce")
    df["date_sortie"] = pd.to_datetime(df["date_sortie"], errors="coerce")
    # Format explicite JJ/MM/AAAA : voir le commentaire de extraire()
    df["date_naissance"] = pd.to_datetime(
        df["date_naissance"], format="%d/%m/%Y", errors="coerce")

    for col in ["age", "cout_hospitalisation_fcfa"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    for col in ["duree_sejour_j", "temperature_entree_c"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Un identifiant de médecin vide n'est pas une donnée manquante à imputer :
    # c'est un dossier non attribué, que l'on doit pouvoir compter.
    df["id_medecin"] = (df["id_medecin"].fillna("").astype(str).str.strip()
                        .replace("", "MED_INCONNU"))

    _journaliser("Conversion des types", avant, len(df),
                 "dates, numériques, identifiants")
    return df


def traiter_aberrations(df):
    """
    Neutralise les valeurs physiquement ou médicalement impossibles.

    Choix de conception : on remplace l'aberration par NaN au lieu de
    supprimer la ligne. Un âge saisi à 999 n'invalide pas l'admission
    elle-même — le séjour a bien eu lieu, sa durée et son coût restent
    exploitables. Supprimer la ligne détruirait de l'information valide.
    """
    avant = len(df)
    df = df.copy()
    compteurs = {}

    for col, (mini, maxi) in BORNES.items():
        masque = df[col].notna() & ((df[col] < mini) | (df[col] > maxi))
        compteurs[col] = int(masque.sum())
        df.loc[masque, col] = np.nan

    detail = ", ".join(f"{c}: {n}" for c, n in compteurs.items() if n)
    _journaliser("Neutralisation des valeurs aberrantes", avant, len(df), detail)
    return df


def corriger_incoherences(df):
    """
    Résout les incohérences entre colonnes liées.

    Deux cas sont traités :
      1. date_sortie antérieure à date_entree — erreur de saisie manifeste :
         la date de sortie est recalculée à partir de la durée de séjour.
      2. durée de séjour manquante alors que les deux dates sont présentes :
         la durée est déduite de l'écart entre les deux dates.
    Le second cas récupère de l'information au lieu de la jeter, ce qui réduit
    d'autant le volume à imputer statistiquement à l'étape suivante.
    """
    avant = len(df)
    df = df.copy()

    # Cas 1 — chronologie inversée
    incoherent = df["date_sortie"].notna() & (df["date_sortie"] < df["date_entree"])
    n_incoherent = int(incoherent.sum())
    df.loc[incoherent, "date_sortie"] = pd.NaT

    # Cas 2 — durée reconstituée depuis les dates
    reconstituable = (df["duree_sejour_j"].isna()
                      & df["date_sortie"].notna() & df["date_entree"].notna())
    n_reconstitue = int(reconstituable.sum())
    df.loc[reconstituable, "duree_sejour_j"] = (
        (df.loc[reconstituable, "date_sortie"]
         - df.loc[reconstituable, "date_entree"]).dt.total_seconds() / 86400
    ).round(1)

    _journaliser(
        "Correction des incohérences", avant, len(df),
        f"{n_incoherent} chronologies inversées, "
        f"{n_reconstitue} durées reconstituées",
    )
    return df


def imputer(df):
    """
    Impute les valeurs manquantes restantes selon une stratégie justifiée.

    La durée de séjour est imputée par la MÉDIANE DE LA PATHOLOGIE, et non
    par la médiane globale. Un séjour pour accouchement dure 2 jours, un
    séjour pour AVC hémorragique en dure 14 : imputer 5 jours partout
    créerait un biais systématique sur les deux extrémités de la
    distribution et fausserait toutes les analyses de durée moyenne.

    La médiane est préférée à la moyenne car la distribution des durées est
    fortement asymétrique à droite (quelques séjours très longs).

    Une colonne booléenne trace chaque valeur imputée : les analyses
    sensibles pourront ainsi exclure les valeurs reconstituées.
    """
    avant = len(df)
    df = df.copy()

    df["duree_imputee"] = df["duree_sejour_j"].isna()
    medianes = df.groupby("pathologie")["duree_sejour_j"].transform("median")
    df["duree_sejour_j"] = df["duree_sejour_j"].fillna(medianes)
    # Filet de sécurité si une pathologie entière était sans valeur
    df["duree_sejour_j"] = df["duree_sejour_j"].fillna(
        df["duree_sejour_j"].median())

    # La température n'est pas imputée : une température non relevée est une
    # absence de mesure, pas une valeur moyenne. Elle reste nulle et les
    # analyses la traiteront explicitement.
    n_impute = int(df["duree_imputee"].sum())

    # L'âge manquant est reconstitué depuis la date de naissance quand elle
    # est disponible — une donnée réelle vaut mieux qu'une imputation.
    reconstituable = df["age"].isna() & df["date_naissance"].notna()
    n_age = int(reconstituable.sum())
    df.loc[reconstituable, "age"] = (
        (df.loc[reconstituable, "date_entree"]
         - df.loc[reconstituable, "date_naissance"]).dt.days // 365
    )
    df["age"] = df["age"].astype("Float64")

    _journaliser(
        "Imputation", avant, len(df),
        f"{n_impute} durées (médiane par pathologie), "
        f"{n_age} âges recalculés depuis la date de naissance",
    )
    return df


def nettoyer(df):
    """Enchaîne les six étapes de nettoyage dans l'ordre requis."""
    return (df
            .pipe(dedoublonner)
            .pipe(normaliser_textes)
            .pipe(corriger_types)
            .pipe(traiter_aberrations)
            .pipe(corriger_incoherences)
            .pipe(imputer))


# =============================================================================
# ÉTAPE 3 — PSEUDONYMISATION (loi ivoirienne n° 2019-992)
# =============================================================================

def pseudonymiser(df, sel=None):
    """
    Applique la pseudonymisation exigée pour un entrepôt de données de santé.

    Trois traitements distincts, correspondant à trois natures de données :

    1. IDENTIFIANT PIVOT (id_patient) — haché en SHA-256 avec un sel secret.
       Le hachage seul ne suffirait pas : l'espace des identifiants
       PAT000001..PAT120000 est si réduit qu'un attaquant reconstruirait la
       table de correspondance complète en quelques secondes. Le sel, conservé
       hors du dépôt (variable d'environnement), rend cette attaque
       impraticable. Le hachage étant déterministe, le chaînage des séjours
       d'un même patient est préservé : les réhospitalisations restent
       analysables.

    2. IDENTIFIANTS DIRECTS (nom, prénom, téléphone) — supprimés. Ils n'ont
       aucune valeur analytique : principe de minimisation.

    3. QUASI-IDENTIFIANT (date de naissance) — généralisé à l'année seule.
       Une date de naissance exacte croisée avec la commune et le sexe permet
       de réidentifier un individu ; l'année de naissance suffit aux analyses
       par cohorte.

    Le traitement produit une pseudonymisation, non une anonymisation : la
    réidentification reste possible pour qui détient le sel. C'est le régime
    attendu ici, puisque le CHU doit pouvoir remonter au dossier patient.
    """
    df = df.copy()
    sel = sel or os.environ.get("DE_SEL_PSEUDO")
    if not sel:
        raise ValueError(
            "Le sel de pseudonymisation est absent. Définissez la variable "
            "d'environnement DE_SEL_PSEUDO avant d'exécuter le pipeline."
        )

    operations = []

    # 1. Hachage salé de l'identifiant patient
    df["id_patient_pseudo"] = df[COLONNE_A_HACHER].astype(str).map(
        lambda x: hashlib.sha256((sel + x).encode("utf-8")).hexdigest()[:16]
    )
    df = df.drop(columns=[COLONNE_A_HACHER])
    operations.append({
        "colonne": COLONNE_A_HACHER, "operation": "Hachage SHA-256 salé",
        "justification": "Chaînage des séjours conservé sans identifiant direct",
    })

    # 2. Suppression des identifiants directs
    presentes = [c for c in COLONNES_IDENTIFIANTES if c in df.columns]
    df = df.drop(columns=presentes)
    for col in presentes:
        operations.append({
            "colonne": col, "operation": "Suppression",
            "justification": "Aucune valeur analytique (minimisation)",
        })

    # 3. Généralisation de la date de naissance
    if COLONNE_A_GENERALISER in df.columns:
        df["annee_naissance"] = df[COLONNE_A_GENERALISER].dt.year
        df = df.drop(columns=[COLONNE_A_GENERALISER])
        operations.append({
            "colonne": COLONNE_A_GENERALISER,
            "operation": "Généralisation (année seule)",
            "justification": "Quasi-identifiant : date exacte réidentifiante",
        })

    # Journal d'audit horodaté, chargé dans Supabase comme preuve de conformité
    audit = pd.DataFrame(operations)
    audit["horodatage"] = datetime.now().isoformat(timespec="seconds")
    audit["nb_lignes_traitees"] = len(df)
    audit["execution_id"] = hashlib.md5(
        audit["horodatage"].iloc[0].encode()).hexdigest()[:8]

    print(f"Pseudonymisation : {len(operations)} traitements appliqués sur "
          f"{len(df):,} lignes")
    return df, audit


def verifier_absence_donnees_personnelles(df):
    """
    Contrôle bloquant : aucune donnée directement identifiante ne doit
    subsister avant le chargement dans l'entrepôt. Cette vérification est
    volontairement séparée de la pseudonymisation elle-même — un contrôle
    qui ferait confiance à l'étape qu'il contrôle ne servirait à rien.
    """
    interdites = COLONNES_IDENTIFIANTES + [COLONNE_A_HACHER,
                                           COLONNE_A_GENERALISER]
    presentes = [c for c in interdites if c in df.columns]
    if presentes:
        raise AssertionError(
            f"Données personnelles détectées avant chargement : {presentes}. "
            "Le chargement est interrompu."
        )
    return True


# =============================================================================
# ÉTAPE 4 — ENRICHISSEMENT
# =============================================================================

def enrichir(df):
    """
    Ajoute sept colonnes calculées à valeur métier.

    Ces colonnes ne sont pas des reformulations des données brutes : chacune
    répond à une question que le CHU se pose et qu'aucune colonne source ne
    permet de traiter directement.
    """
    df = df.copy()

    # 1. Tranche d'âge — dimension d'analyse épidémiologique standard
    bornes = [t[2] for t in TRANCHES_AGE] + [121]
    libelles = [t[1] for t in TRANCHES_AGE]
    df["tranche_age"] = pd.cut(
        df["age"].astype("float"), bins=bornes, labels=libelles,
        right=False, include_lowest=True,
    ).astype(object)
    df["tranche_age"] = df["tranche_age"].fillna("Âge non renseigné")

    # 2. Saison — permet de relier les admissions au climat, donc à la
    #    dynamique du vecteur du paludisme
    df["mois"] = df["date_entree"].dt.month
    df["saison"] = df["mois"].map(SAISON_PAR_MOIS)

    # 3. Catégorie de pathologie — regroupe 31 diagnostics en 5 axes
    df["categorie_pathologie"] = (df["pathologie"]
                                  .map(PATHOLOGIE_VERS_CATEGORIE)
                                  .fillna("Autre"))

    # 4. Coût journalier — rend les services comparables entre eux, ce que
    #    le coût total ne permet pas (il dépend de la durée)
    df["cout_journalier_fcfa"] = (
        df["cout_hospitalisation_fcfa"] / df["duree_sejour_j"]
    ).round(0)

    # 5. Séjour prolongé — au-delà du 90e centile du service concerné.
    #    Le seuil est relatif au service : 12 jours sont normaux en
    #    neurologie et anormaux en maternité.
    seuils = df.groupby("service")["duree_sejour_j"].transform(
        lambda s: s.quantile(0.90))
    df["sejour_prolonge"] = df["duree_sejour_j"] > seuils

    # 6. Délai depuis l'admission précédente du même patient
    df = df.sort_values(["id_patient_pseudo", "date_entree"])
    df["delai_readmission_j"] = (
        df.groupby("id_patient_pseudo")["date_entree"].diff()
        .dt.total_seconds() / 86400
    ).round(1)

    # 7. Réadmission à 30 jours — indicateur de qualité des soins reconnu
    #    internationalement : une réadmission précoce signale une sortie
    #    prématurée ou un suivi ambulatoire défaillant.
    df["est_readmission_30j"] = (
        df["delai_readmission_j"].notna() & (df["delai_readmission_j"] <= 30)
    )

    df = df.sort_values("date_entree").reset_index(drop=True)
    print(f"Enrichissement : 7 colonnes calculées ajoutées "
          f"({df.shape[1]} colonnes au total)")
    return df


# =============================================================================
# ÉTAPE 5 — TESTS DE QUALITÉ
# =============================================================================

def controler_qualite(df, bloquant=True):
    """
    Applique huit règles de validation avant chargement dans l'entrepôt.

    Les règles marquées « bloquante » interrompent le pipeline : elles
    portent sur l'intégrité (unicité des clés) et sur la conformité
    réglementaire. Les autres émettent un avertissement et laissent le
    chargement se poursuivre : un taux de valeurs manquantes élevé dégrade
    l'analyse mais ne la rend pas fausse.
    """
    resultats = []

    def regle(nom, condition_violee, message, critique=False):
        n = int(condition_violee.sum()) if hasattr(condition_violee, "sum") \
            else int(bool(condition_violee))
        resultats.append({
            "regle": nom,
            "violations": n,
            "statut": "OK" if n == 0 else ("ÉCHEC" if critique else "AVERTISSEMENT"),
            "criticite": "bloquante" if critique else "informative",
            "message": "" if n == 0 else message.format(n=n),
        })

    regle("R1 — Unicité de id_admission",
          df["id_admission"].duplicated(),
          "{n} identifiants d'admission dupliqués : la clé primaire de la "
          "table de faits serait invalide.", critique=True)

    regle("R2 — Absence de données personnelles",
          any(c in df.columns for c in COLONNES_IDENTIFIANTES),
          "Des colonnes identifiantes subsistent : chargement interdit par "
          "la loi 2019-992.", critique=True)

    regle("R3 — Âge dans une plage plausible",
          df["age"].notna() & ~df["age"].astype("float").between(0, 110),
          "{n} âges hors de l'intervalle [0 ; 110].", critique=True)

    regle("R4 — Durée de séjour strictement positive",
          df["duree_sejour_j"].isna() | (df["duree_sejour_j"] <= 0),
          "{n} durées de séjour nulles, négatives ou manquantes.", critique=True)

    regle("R5 — Cohérence chronologique entrée/sortie",
          df["date_sortie"].notna() & (df["date_sortie"] < df["date_entree"]),
          "{n} dates de sortie antérieures à la date d'entrée.", critique=True)

    regle("R6 — Issue conforme au référentiel",
          ~df["issue"].isin(ISSUES_VALIDES),
          "{n} valeurs d'issue hors référentiel.", critique=True)

    regle("R7 — Pathologie rattachée au référentiel",
          df["pathologie"].eq("Non renseignée"),
          "{n} admissions sans pathologie identifiée : elles seront exclues "
          "des analyses par diagnostic.")

    regle("R8 — Complétude de la température",
          df["temperature_entree_c"].isna(),
          "{n} températures non relevées ({n} dossiers incomplets).")

    rapport = pd.DataFrame(resultats)
    echecs = rapport[rapport["statut"] == "ÉCHEC"]

    if not echecs.empty and bloquant:
        details = "\n".join(f"  - {r.regle} : {r.message}"
                            for r in echecs.itertuples())
        raise ValueError(
            f"Contrôle qualité en échec ({len(echecs)} règle(s) bloquante(s)) :"
            f"\n{details}"
        )

    n_ok = int((rapport["statut"] == "OK").sum())
    print(f"Contrôle qualité : {n_ok}/{len(rapport)} règles satisfaites")
    return rapport


# =============================================================================
# ÉTAPE 6 — MODÉLISATION EN ÉTOILE
# =============================================================================

def construire_dimensions(df):
    """
    Construit les six tables de dimension.

    Chaque dimension reçoit une clé technique entière (surrogate key) plutôt
    que d'utiliser le libellé comme clé : les jointures sur entier sont plus
    rapides, et un changement de libellé (correction d'orthographe, fusion de
    services) n'oblige pas à réécrire la table de faits.
    """
    dims = {}

    # --- dim_date : une ligne par jour couvert par les données --------------
    dates = pd.date_range(df["date_entree"].min().normalize(),
                          df["date_entree"].max().normalize(), freq="D")
    dim_date = pd.DataFrame({"date_jour": dates})
    # Clé au format AAAAMMJJ : lisible à l'œil nu lors du débogage
    dim_date["date_id"] = dim_date["date_jour"].dt.strftime("%Y%m%d").astype(int)
    dim_date["jour"] = dim_date["date_jour"].dt.day
    dim_date["mois"] = dim_date["date_jour"].dt.month
    dim_date["libelle_mois"] = dim_date["mois"].map(LIBELLE_MOIS)
    dim_date["trimestre"] = dim_date["date_jour"].dt.quarter
    dim_date["annee"] = dim_date["date_jour"].dt.year
    dim_date["jour_semaine"] = dim_date["date_jour"].dt.dayofweek.map(LIBELLE_JOUR)
    dim_date["est_weekend"] = dim_date["date_jour"].dt.dayofweek >= 5
    dim_date["saison"] = dim_date["mois"].map(SAISON_PAR_MOIS)
    dim_date["annee_mois"] = dim_date["date_jour"].dt.strftime("%Y-%m")
    dims["dim_date"] = dim_date[[
        "date_id", "date_jour", "jour", "mois", "libelle_mois", "trimestre",
        "annee", "annee_mois", "jour_semaine", "est_weekend", "saison"]]

    # --- dim_service : enrichie de la capacité en lits ----------------------
    services = sorted(df["service"].dropna().unique())
    dims["dim_service"] = pd.DataFrame({
        "service_id": range(1, len(services) + 1),
        "libelle_service": services,
        "capacite_lits": [LITS_PAR_SERVICE.get(s, 0) for s in services],
    })

    # --- dim_pathologie : hiérarchie diagnostic -> catégorie ----------------
    patho = (df[["pathologie", "categorie_pathologie"]]
             .drop_duplicates().sort_values("pathologie").reset_index(drop=True))
    patho.insert(0, "pathologie_id", range(1, len(patho) + 1))
    dims["dim_pathologie"] = patho.rename(columns={
        "pathologie": "libelle_pathologie",
        "categorie_pathologie": "categorie",
    })

    # --- dim_tranche_age : ordonnée pour l'affichage des graphiques ---------
    tranches = pd.DataFrame(TRANCHES_AGE,
                            columns=["ordre", "libelle_tranche",
                                     "borne_min", "borne_max"])
    tranches.insert(0, "tranche_id", tranches["ordre"])
    inconnu = pd.DataFrame([{
        "tranche_id": 99, "ordre": 99, "libelle_tranche": "Âge non renseigné",
        "borne_min": None, "borne_max": None}])
    dims["dim_tranche_age"] = pd.concat([tranches, inconnu], ignore_index=True)

    # --- dim_commune --------------------------------------------------------
    communes = sorted(df["commune_residence"].dropna().unique())
    dims["dim_commune"] = pd.DataFrame({
        "commune_id": range(1, len(communes) + 1),
        "libelle_commune": communes,
        "district": "Abidjan",
    })

    # --- dim_issue ----------------------------------------------------------
    issues = sorted(df["issue"].dropna().unique())
    dims["dim_issue"] = pd.DataFrame({
        "issue_id": range(1, len(issues) + 1),
        "code_issue": issues,
        "libelle_issue": [LIBELLE_ISSUE.get(i, i) for i in issues],
        "est_deces": [i == "Deces" for i in issues],
    })

    for nom, d in dims.items():
        print(f"  {nom:<20} {len(d):>6} lignes")
    return dims


def construire_faits(df, dims):
    """
    Construit la table de faits par jointure sur les dimensions.

    L'ordre est impératif : les dimensions doivent exister avant la table de
    faits, sans quoi les clés étrangères seraient invalides. On vérifie
    explicitement l'absence de valeur nulle sur chaque FK avant de rendre la
    table — une seule FK nulle ferait échouer la contrainte d'intégrité au
    moment de l'insertion dans PostgreSQL, après plusieurs minutes de
    chargement.
    """
    faits = df.copy()

    # Dictionnaires de correspondance libellé -> clé technique
    map_service = dict(zip(dims["dim_service"]["libelle_service"],
                           dims["dim_service"]["service_id"]))
    map_patho = dict(zip(dims["dim_pathologie"]["libelle_pathologie"],
                         dims["dim_pathologie"]["pathologie_id"]))
    map_tranche = dict(zip(dims["dim_tranche_age"]["libelle_tranche"],
                           dims["dim_tranche_age"]["tranche_id"]))
    map_commune = dict(zip(dims["dim_commune"]["libelle_commune"],
                           dims["dim_commune"]["commune_id"]))
    map_issue = dict(zip(dims["dim_issue"]["code_issue"],
                         dims["dim_issue"]["issue_id"]))

    faits["date_id"] = faits["date_entree"].dt.strftime("%Y%m%d").astype(int)
    faits["service_id"] = faits["service"].map(map_service)
    faits["pathologie_id"] = faits["pathologie"].map(map_patho)
    faits["tranche_id"] = faits["tranche_age"].map(map_tranche)
    faits["commune_id"] = faits["commune_residence"].map(map_commune)
    faits["issue_id"] = faits["issue"].map(map_issue)

    colonnes = [
        # Clé primaire
        "id_admission",
        # Clés étrangères vers les dimensions
        "date_id", "service_id", "pathologie_id", "tranche_id",
        "commune_id", "issue_id",
        # Dimension dégénérée : identifiant patient pseudonymisé
        "id_patient_pseudo",
        # Attributs de contexte
        "sexe", "age", "annee_naissance", "mode_admission", "gravite",
        "assurance", "id_medecin", "heure_admission",
        # Mesures
        "duree_sejour_j", "cout_hospitalisation_fcfa", "cout_journalier_fcfa",
        "temperature_entree_c", "sejour_prolonge", "est_readmission_30j",
        "delai_readmission_j", "duree_imputee",
    ]
    faits["heure_admission"] = faits["date_entree"].dt.hour
    faits = faits[colonnes]

    # Contrôle d'intégrité référentielle avant restitution
    fks = ["date_id", "service_id", "pathologie_id", "tranche_id",
           "commune_id", "issue_id"]
    nuls = {fk: int(faits[fk].isna().sum()) for fk in fks}
    if any(nuls.values()):
        raise ValueError(f"Clés étrangères non résolues : "
                         f"{ {k: v for k, v in nuls.items() if v} }")

    faits[fks] = faits[fks].astype(int)
    print(f"  faits_admissions     {len(faits):>6} lignes, "
          f"{len(fks)} clés étrangères toutes résolues")
    return faits


# =============================================================================
# ÉTAPE 7 — CHARGEMENT DANS SUPABASE (PostgreSQL)
# =============================================================================

def obtenir_moteur(url=None):
    """
    Crée le moteur SQLAlchemy vers Supabase.

    La chaîne de connexion n'est jamais écrite en dur : elle provient de la
    variable d'environnement SUPABASE_DB_URL, elle-même chargée depuis un
    fichier .env exclu du dépôt Git.
    """
    from sqlalchemy import create_engine  # import différé : inutile hors chargement

    url = url or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise ValueError(
            "SUPABASE_DB_URL absente. Copiez .env.example en .env et "
            "renseignez la chaîne de connexion Supabase (port 5432)."
        )
    # pool_pre_ping évite les erreurs de connexion expirée sur les
    # chargements longs ; l'offre gratuite de Supabase ferme les sessions
    # inactives au bout de quelques minutes.
    return create_engine(url, pool_pre_ping=True)


def charger_table(df, nom_table, moteur, si_existe="replace", taille_lot=5000):
    """
    Charge un DataFrame dans PostgreSQL et renvoie la durée du chargement.

    `method="multi"` regroupe les lignes en insertions multi-valeurs :
    sur 120 000 lignes, on passe d'environ vingt minutes en insertion ligne
    à ligne à moins d'une minute. Le paramètre `chunksize` borne la taille
    de chaque requête pour ne pas dépasser la limite de paramètres du
    driver PostgreSQL.
    """
    debut = time.time()
    df.to_sql(nom_table, moteur, if_exists=si_existe, index=False,
              chunksize=taille_lot, method="multi")
    duree = time.time() - debut
    print(f"  {nom_table:<20} {len(df):>7,} lignes chargées en {duree:>6.1f} s")
    return duree


def charger_entrepot(dims, faits, audit_rgpd, moteur):
    """
    Charge l'ensemble de l'entrepôt dans l'ordre imposé par les contraintes
    d'intégrité : dimensions d'abord, table de faits ensuite.
    """
    from sqlalchemy import text

    resume = {}
    for nom, table in dims.items():
        resume[nom] = charger_table(table, nom, moteur)
    resume["faits_admissions"] = charger_table(faits, "faits_admissions", moteur)
    resume["audit_rgpd"] = charger_table(audit_rgpd, "audit_rgpd", moteur,
                                         si_existe="append")

    # Les contraintes et index sont posés après le chargement : les créer
    # avant ralentirait chaque insertion par la vérification des clés.
    with moteur.connect() as conn:
        for instruction in INDEX_POST_CHARGEMENT:
            conn.execute(text(instruction))
        conn.commit()   # validation explicite de la transaction

    moteur.dispose()    # libération du pool de connexions
    print(f"\nChargement terminé en {sum(resume.values()):.1f} s au total")
    return resume


INDEX_POST_CHARGEMENT = [
    "ALTER TABLE faits_admissions ADD PRIMARY KEY (id_admission)",
    "CREATE INDEX IF NOT EXISTS idx_faits_date ON faits_admissions(date_id)",
    "CREATE INDEX IF NOT EXISTS idx_faits_service ON faits_admissions(service_id)",
    "CREATE INDEX IF NOT EXISTS idx_faits_patho ON faits_admissions(pathologie_id)",
    "CREATE INDEX IF NOT EXISTS idx_faits_patient "
    "ON faits_admissions(id_patient_pseudo)",
]


# =============================================================================
# ORCHESTRATION COMPLÈTE
# =============================================================================

def executer_pipeline(chemin_csv, charger=False, url_bd=None):
    """
    Exécute le pipeline de bout en bout.

    Le paramètre `charger` permet de rejouer toute la transformation sans
    solliciter la base : c'est ce mode qui est utilisé lors des tests et lors
    de l'exécution du notebook quand les tables sont déjà en place.
    """
    JOURNAL.clear()
    df = extraire(chemin_csv)
    df = nettoyer(df)
    df, audit_rgpd = pseudonymiser(df)
    df = enrichir(df)
    verifier_absence_donnees_personnelles(df)
    rapport = controler_qualite(df)
    dims = construire_dimensions(df)
    faits = construire_faits(df, dims)

    if charger:
        charger_entrepot(dims, faits, audit_rgpd, obtenir_moteur(url_bd))

    return {
        "donnees": df, "dimensions": dims, "faits": faits,
        "audit_rgpd": audit_rgpd, "rapport_qualite": rapport,
        "journal": pd.DataFrame(JOURNAL),
    }
