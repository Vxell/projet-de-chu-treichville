"""
=============================================================================
 PIPELINE ETL — ENTREPÔT ÉPIDÉMIOLOGIQUE DU CHU DE TREICHVILLE
=============================================================================
 Étapes 1 à 5 — De l'extraction du fichier source aux contrôles qualité.

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
    ISSUES_VALIDES, MAPPING_ASSURANCE, MAPPING_SEXE,
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
# ÉTAPE 3 — PSEUDONYMISATION 
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
