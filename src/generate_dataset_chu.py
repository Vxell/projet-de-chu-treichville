"""
=============================================================================
 PROJET DE FIN DE MODULE - DATA ENGINEERING
 Sujet 3 : Sante publique - Suivi epidemiologique au CHU de Treichville
-----------------------------------------------------------------------------
 Generateur du dataset source : admissions hospitalieres sur 2 ans.

 Le dataset produit est VOLONTAIREMENT "sale" et non anonymise : c'est la
 matiere premiere du pipeline ETL (nettoyage + pseudonymisation RGPD).

 Sortie : admissions_chu_treichville.csv  (~122 000 lignes)
          dictionnaire_donnees.md         (documentation pour le rapport)
=============================================================================
"""

import hashlib
import random
import unicodedata
from calendar import monthrange

import numpy as np
import pandas as pd

# =============================================================================
# 0. PARAMETRES GLOBAUX
# =============================================================================

SEED = 2025
N_ADMISSIONS = 120_000          # nombre d'admissions "propres" generees
TAUX_DOUBLONS = 0.02            # 2% de lignes dupliquees (erreur de saisie SIH)
DATE_DEBUT = pd.Timestamp("2023-01-01")
NB_MOIS = 24                    # periode couverte : 2023-01 -> 2024-12
FICHIER_SORTIE = "admissions_chu_treichville.csv"

random.seed(SEED)
np.random.seed(SEED)
rng = np.random.default_rng(SEED)

# =============================================================================
# 1. REFERENTIELS METIER (contexte ivoirien / CHU de Treichville)
# =============================================================================

# --- Services hospitaliers et leur poids dans les admissions ----------------
SERVICES = {
    "Urgences":              0.24,
    "Pediatrie":             0.18,
    "Medecine interne":      0.16,
    "Maternite":             0.14,
    "Chirurgie":             0.12,
    "Cardiologie":           0.08,
    "Neurologie":            0.05,
    "Maladies infectieuses": 0.03,
}

# --- Pathologies observees dans chaque service ------------------------------
# Reflete le profil epidemiologique ivoirien : forte dominance du paludisme,
# poids des traumatismes routiers, des maladies infectieuses (TB, VIH) et
# de la transition epidemiologique (HTA, diabete, AVC).
PATHOLOGIES_PAR_SERVICE = {
    "Urgences": {
        "Paludisme grave": 0.18, "Traumatisme routier": 0.16,
        "Paludisme simple": 0.12, "Gastro-enterite": 0.08,
        "Fracture": 0.07, "Anemie severe": 0.06, "Fievre typhoide": 0.06,
        "AVC ischemique": 0.05, "Pneumopathie": 0.05,
        "Epilepsie": 0.04, "Diabete decompense": 0.04, "Covid-19": 0.04,
        "Brulure": 0.03, "Meningite": 0.02,
    },
    "Pediatrie": {
        "Paludisme simple": 0.28, "Paludisme grave": 0.14,
        "Gastro-enterite": 0.13, "Pneumopathie": 0.10,
        "Infection respiratoire aigue": 0.09, "Denutrition aigue": 0.08,
        "Drepanocytose (crise)": 0.06, "Prematurite": 0.05,
        "Anemie severe": 0.04, "Meningite": 0.03,
    },
    "Medecine interne": {
        "Diabete decompense": 0.16, "Hypertension arterielle": 0.14,
        "VIH - infection opportuniste": 0.12, "Tuberculose": 0.11,
        "Insuffisance renale": 0.09, "Anemie severe": 0.08,
        "Hepatite virale B": 0.08, "Drepanocytose (crise)": 0.07,
        "Paludisme simple": 0.06, "Fievre typhoide": 0.05, "Covid-19": 0.04,
    },
    "Maternite": {
        "Accouchement eutocique": 0.62, "Cesarienne": 0.22,
        "Pre-eclampsie": 0.10, "Anemie severe": 0.06,
    },
    "Chirurgie": {
        "Traumatisme routier": 0.26, "Fracture": 0.22,
        "Appendicite aigue": 0.18, "Hernie etranglee": 0.14,
        "Occlusion intestinale": 0.11, "Brulure": 0.09,
    },
    "Cardiologie": {
        "Hypertension arterielle": 0.34, "Insuffisance cardiaque": 0.28,
        "AVC ischemique": 0.12, "Diabete decompense": 0.10,
        "Insuffisance renale": 0.09, "Anemie severe": 0.07,
    },
    "Neurologie": {
        "AVC ischemique": 0.32, "Epilepsie": 0.22, "AVC hemorragique": 0.20,
        "Meningite": 0.14, "Traumatisme routier": 0.12,
    },
    "Maladies infectieuses": {
        "Tuberculose": 0.22, "VIH - infection opportuniste": 0.20,
        "Paludisme grave": 0.14, "Fievre typhoide": 0.12,
        "Covid-19": 0.12, "Hepatite virale B": 0.10, "Meningite": 0.10,
    },
}

# --- Saisonnalite mensuelle (indice 0 = janvier) ----------------------------
# Abidjan : grande saison des pluies (mai-juillet), petite saison (oct-nov),
# harmattan / saison seche fraiche (dec-fevrier).
SAISON_PLAT      = [1.0] * 12
SAISON_PALU      = [0.7, 0.6, 0.8, 1.1, 1.6, 1.9, 1.7, 1.0, 1.2, 1.4, 1.1, 0.8]
SAISON_RESPI     = [1.6, 1.5, 1.1, 0.8, 0.7, 0.7, 0.8, 0.9, 0.9, 1.0, 1.2, 1.6]
SAISON_DIGESTIVE = [0.8, 0.8, 0.9, 1.1, 1.4, 1.6, 1.5, 1.1, 1.1, 1.3, 1.0, 0.9]
SAISON_TRAUMA    = [1.2, 1.0, 1.0, 1.0, 1.1, 1.1, 1.0, 1.1, 1.0, 1.0, 1.1, 1.4]

SAISONNALITE = {
    "Paludisme simple": SAISON_PALU,
    "Paludisme grave": SAISON_PALU,
    "Anemie severe": SAISON_PALU,
    "Pneumopathie": SAISON_RESPI,
    "Infection respiratoire aigue": SAISON_RESPI,
    "Covid-19": SAISON_RESPI,
    "Meningite": SAISON_RESPI,
    "Gastro-enterite": SAISON_DIGESTIVE,
    "Fievre typhoide": SAISON_DIGESTIVE,
    "Traumatisme routier": SAISON_TRAUMA,
    "Fracture": SAISON_TRAUMA,
}

# --- Profils d'age par pathologie -------------------------------------------
# ("loi", parametres, borne_min, borne_max)
PROFIL_AGE = {
    "Prematurite":                  ("fixe",   (0,),        0,  0),
    "Denutrition aigue":            ("gamma",  (1.6, 2.0),  0,  12),
    "Paludisme simple":             ("gamma",  (2.2, 12.0), 0,  88),
    "Paludisme grave":              ("gamma",  (2.0, 11.0), 0,  90),
    "Gastro-enterite":              ("gamma",  (1.8, 10.0), 0,  85),
    "Infection respiratoire aigue": ("gamma",  (1.7, 6.0),  0,  80),
    "Pneumopathie":                 ("gamma",  (2.2, 14.0), 0,  92),
    "Drepanocytose (crise)":        ("gamma",  (2.0, 8.0),  1,  50),
    "Accouchement eutocique":       ("normal", (27, 6.0),   14, 47),
    "Cesarienne":                   ("normal", (29, 6.5),   15, 48),
    "Pre-eclampsie":                ("normal", (28, 7.0),   15, 46),
    "Hypertension arterielle":      ("normal", (58, 13.0),  30, 95),
    "Insuffisance cardiaque":       ("normal", (62, 13.0),  30, 95),
    "AVC ischemique":               ("normal", (63, 12.0),  30, 96),
    "AVC hemorragique":             ("normal", (59, 13.0),  28, 94),
    "Diabete decompense":           ("normal", (55, 13.0),  18, 92),
    "Insuffisance renale":          ("normal", (54, 15.0),  18, 90),
    "Tuberculose":                  ("normal", (37, 13.0),  2,  85),
    "VIH - infection opportuniste": ("normal", (39, 11.0),  1,  80),
    "Hepatite virale B":            ("normal", (36, 12.0),  5,  80),
    "Traumatisme routier":          ("normal", (31, 12.0),  2,  82),
    "Fracture":                     ("normal", (34, 17.0),  1,  92),
    "Appendicite aigue":            ("normal", (24, 10.0),  4,  70),
    "Hernie etranglee":             ("normal", (42, 17.0),  1,  88),
    "Occlusion intestinale":        ("normal", (48, 17.0),  2,  90),
    "Brulure":                      ("gamma",  (1.8, 12.0), 0,  85),
    "Epilepsie":                    ("normal", (28, 15.0),  2,  80),
    "Meningite":                    ("gamma",  (1.6, 12.0), 0,  85),
    "Covid-19":                     ("normal", (47, 17.0),  2,  95),
    "Fievre typhoide":              ("gamma",  (2.2, 9.0),  1,  75),
}
PROFIL_AGE_DEFAUT = ("normal", (38, 14.0), 15, 90)

# --- Proportion d'hommes par pathologie (defaut 0.49) -----------------------
PROP_HOMMES = {
    "Accouchement eutocique": 0.0, "Cesarienne": 0.0, "Pre-eclampsie": 0.0,
    "Traumatisme routier": 0.68, "Fracture": 0.62, "Brulure": 0.55,
    "VIH - infection opportuniste": 0.42, "Tuberculose": 0.58,
    "Hepatite virale B": 0.57, "Occlusion intestinale": 0.55,
    "Hernie etranglee": 0.66, "Appendicite aigue": 0.52,
}

# --- Duree moyenne de sejour (jours) et mortalite de reference --------------
DUREE_MOYENNE = {
    "Accouchement eutocique": 2.2, "Cesarienne": 5.0, "Pre-eclampsie": 6.5,
    "Paludisme simple": 3.0, "Paludisme grave": 6.5, "Gastro-enterite": 3.0,
    "Pneumopathie": 7.0, "Infection respiratoire aigue": 4.0,
    "Tuberculose": 14.0, "VIH - infection opportuniste": 12.0,
    "Hepatite virale B": 8.0, "Drepanocytose (crise)": 5.0,
    "Anemie severe": 4.0, "Hypertension arterielle": 5.0,
    "Insuffisance cardiaque": 9.0, "AVC ischemique": 12.0,
    "AVC hemorragique": 14.0, "Diabete decompense": 8.0,
    "Insuffisance renale": 10.0, "Epilepsie": 4.0, "Meningite": 11.0,
    "Traumatisme routier": 9.0, "Fracture": 8.0, "Appendicite aigue": 4.0,
    "Hernie etranglee": 5.0, "Occlusion intestinale": 7.0, "Brulure": 15.0,
    "Denutrition aigue": 12.0, "Prematurite": 16.0, "Covid-19": 8.0,
    "Fievre typhoide": 6.0,
}

TAUX_DECES = {
    "Accouchement eutocique": 0.002, "Cesarienne": 0.005, "Pre-eclampsie": 0.035,
    "Paludisme simple": 0.004, "Paludisme grave": 0.075, "Gastro-enterite": 0.012,
    "Pneumopathie": 0.055, "Infection respiratoire aigue": 0.010,
    "Tuberculose": 0.070, "VIH - infection opportuniste": 0.120,
    "Hepatite virale B": 0.040, "Drepanocytose (crise)": 0.030,
    "Anemie severe": 0.045, "Hypertension arterielle": 0.020,
    "Insuffisance cardiaque": 0.100, "AVC ischemique": 0.160,
    "AVC hemorragique": 0.280, "Diabete decompense": 0.050,
    "Insuffisance renale": 0.130, "Epilepsie": 0.012, "Meningite": 0.180,
    "Traumatisme routier": 0.060, "Fracture": 0.010, "Appendicite aigue": 0.008,
    "Hernie etranglee": 0.030, "Occlusion intestinale": 0.055, "Brulure": 0.120,
    "Denutrition aigue": 0.100, "Prematurite": 0.120, "Covid-19": 0.050,
    "Fievre typhoide": 0.020,
}

# Pathologies chroniques -> forte probabilite de rehospitalisation
PATHOLOGIES_CHRONIQUES = {
    "Drepanocytose (crise)", "Diabete decompense", "Hypertension arterielle",
    "Insuffisance cardiaque", "Insuffisance renale", "VIH - infection opportuniste",
    "Tuberculose", "Epilepsie", "Hepatite virale B",
}

# --- Referentiels administratifs --------------------------------------------
COMMUNES = {
    "Treichville": 0.14, "Marcory": 0.11, "Koumassi": 0.12, "Port-Bouet": 0.09,
    "Abobo": 0.11, "Yopougon": 0.13, "Adjame": 0.08, "Cocody": 0.07,
    "Attecoube": 0.05, "Plateau": 0.03, "Bingerville": 0.03, "Anyama": 0.02,
    "Songon": 0.02,
}
ASSURANCES = {"Aucune": 0.46, "CMU": 0.31, "Mutuelle employeur": 0.14,
              "Assurance privee": 0.09}
MODES_ADMISSION = {"Urgence": 0.58, "Programmee": 0.27, "Transfert": 0.15}
GRAVITES = ["Legere", "Moderee", "Severe", "Critique"]

# Profil horaire des admissions (0h -> 23h) : creux nocturne, pic matinal
PROFIL_HORAIRE = np.array([
    2.0, 1.6, 1.3, 1.2, 1.4, 2.2, 3.4, 5.2, 7.0, 7.6, 7.2, 6.4,
    5.5, 5.2, 5.4, 5.6, 5.8, 6.0, 5.6, 4.6, 3.8, 3.2, 2.8, 2.4,
])
PROFIL_HORAIRE = PROFIL_HORAIRE / PROFIL_HORAIRE.sum()

# --- Identites (patronymes et prenoms courants en Cote d'Ivoire) ------------
NOMS = ["Kouassi", "Kouame", "Yao", "Konan", "Aka", "Bamba", "Traore", "Coulibaly",
        "Diarra", "Ouattara", "Toure", "Diallo", "Cisse", "Sangare", "Fofana",
        "Gnaore", "Zadi", "Tape", "Gbagbo", "Zoro", "Beugre", "Assi", "Adjoua",
        "Kone", "Doumbia", "Sylla", "Camara", "Keita", "Sanogo", "Dosso",
        "Kacou", "Amani", "Ehui", "Anoh", "N'Guessan", "Brou", "Yeboua",
        "Digbeu", "Gueu", "Seri", "Irie", "Bohoussou", "Ette", "Assamoi"]
PRENOMS_H = ["Kouadio", "Yao", "Serge", "Emmanuel", "Ibrahim", "Moussa", "Adama",
             "Jean-Marc", "Franck", "Aristide", "Herve", "Didier", "Arsene",
             "Cheick", "Souleymane", "Patrice", "Landry", "Olivier", "Marcel",
             "Bakary", "Vamara", "Desire", "Wilfried", "Ismael", "Rodrigue"]
PRENOMS_F = ["Aminata", "Fatoumata", "Adjoua", "Akissi", "Affoue", "Mariam",
             "Christelle", "Nadege", "Sylvie", "Rosine", "Aya", "Amenan",
             "Awa", "Kadidja", "Solange", "Prisca", "Edwige", "Mariame",
             "Josiane", "Habiba", "Djeneba", "Emeline", "Carine", "Yasmine"]

# =============================================================================
# 2. FONCTIONS UTILITAIRES
# =============================================================================

def tirage(dico, taille):
    """Tire `taille` valeurs dans les cles d'un dict {valeur: probabilite}."""
    cles = list(dico.keys())
    probas = np.array(list(dico.values()), dtype=float)
    return rng.choice(cles, size=taille, p=probas / probas.sum())


def tirer_ages(pathologie, taille):
    """Genere des ages coherents avec le profil epidemiologique de la pathologie."""
    loi, params, bmin, bmax = PROFIL_AGE.get(pathologie, PROFIL_AGE_DEFAUT)
    if loi == "fixe":
        ages = np.zeros(taille)
    elif loi == "gamma":
        ages = rng.gamma(params[0], params[1], taille)
    else:  # normal
        ages = rng.normal(params[0], params[1], taille)
    return np.clip(ages, bmin, bmax).round().astype(int)


def tirer_dates(pathologie, taille):
    """
    Genere des horodatages d'admission ponderes par :
      - la saisonnalite propre a la pathologie,
      - une legere croissance de l'activite en 2024 (+7%),
      - le profil horaire des admissions hospitalieres.
    """
    profil = SAISONNALITE.get(pathologie, SAISON_PLAT)
    poids_mois = np.array([profil[m % 12] * (1.07 if m >= 12 else 1.0)
                           for m in range(NB_MOIS)])
    poids_mois = poids_mois / poids_mois.sum()

    idx_mois = rng.choice(NB_MOIS, size=taille, p=poids_mois)
    annees = 2023 + idx_mois // 12
    mois = idx_mois % 12 + 1
    nb_jours = np.array([monthrange(a, m)[1] for a, m in zip(annees, mois)])
    jours = (rng.random(taille) * nb_jours).astype(int) + 1
    heures = rng.choice(24, size=taille, p=PROFIL_HORAIRE)
    minutes = rng.integers(0, 60, taille)

    return pd.to_datetime(pd.DataFrame({
        "year": annees, "month": mois, "day": jours,
        "hour": heures, "minute": minutes,
    }))


def tirer_issues(pathologie, ages, durees):
    """
    Determine l'issue de l'hospitalisation. Le risque de deces depend de la
    pathologie, de l'age (fragilite des nourrissons et des plus de 65 ans)
    et de la duree de sejour (proxy de la gravite).
    """
    taille = len(ages)
    base = TAUX_DECES.get(pathologie, 0.03)
    facteur = np.ones(taille)
    facteur[ages >= 65] *= 1.7
    facteur[ages >= 80] *= 1.3
    facteur[ages < 5] *= 1.35
    facteur *= 1 + 0.02 * np.clip(durees - DUREE_MOYENNE.get(pathologie, 6), 0, 30)

    p_deces = np.clip(base * facteur, 0, 0.85)
    p_transfert = np.full(taille, 0.07)
    p_contre_avis = np.full(taille, 0.04)
    p_guerison = 1 - p_deces - p_transfert - p_contre_avis

    tirages = rng.random(taille)
    issues = np.full(taille, "Gueri", dtype=object)
    issues[tirages > p_guerison] = "Transfert"
    issues[tirages > p_guerison + p_transfert] = "Sortie contre avis"
    issues[tirages > p_guerison + p_transfert + p_contre_avis] = "Deces"
    return issues


def sans_accent(txt):
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()


# =============================================================================
# 3. GENERATION DU CORPS DU DATASET
# =============================================================================

print("[1/6] Generation des admissions par service et pathologie...")

services = tirage(SERVICES, N_ADMISSIONS)
pathologies = np.empty(N_ADMISSIONS, dtype=object)

# La pathologie depend du service d'hospitalisation
for service in SERVICES:
    masque = services == service
    pathologies[masque] = tirage(PATHOLOGIES_PAR_SERVICE[service], masque.sum())

ages = np.zeros(N_ADMISSIONS, dtype=int)
sexes = np.empty(N_ADMISSIONS, dtype=object)
dates_entree = np.empty(N_ADMISSIONS, dtype="datetime64[ns]")
durees = np.zeros(N_ADMISSIONS)
issues = np.empty(N_ADMISSIONS, dtype=object)
gravites = np.empty(N_ADMISSIONS, dtype=object)

print("[2/6] Age, sexe, saisonnalite, duree de sejour et issue...")

for pathologie in np.unique(pathologies):
    masque = pathologies == pathologie
    n = int(masque.sum())

    # Age (borne a 15 ans en pediatrie, a 15-49 ans en maternite)
    a = tirer_ages(pathologie, n)
    serv = services[masque]
    a = np.where(serv == "Pediatrie", np.minimum(a, 15), a)
    a = np.where(serv == "Maternite", np.clip(a, 14, 49), a)
    ages[masque] = a

    # Sexe
    p_h = PROP_HOMMES.get(pathologie, 0.49)
    s = np.where(rng.random(n) < p_h, "M", "F")
    s = np.where(serv == "Maternite", "F", s)
    sexes[masque] = s

    # Date d'admission (saisonnalite)
    dates_entree[masque] = tirer_dates(pathologie, n).values

    # Duree de sejour : loi log-normale, moyenne propre a la pathologie
    moy = DUREE_MOYENNE.get(pathologie, 6.0)
    d = rng.lognormal(np.log(moy) - 0.18, 0.6, n)
    d = np.where(a >= 65, d * 1.25, d)          # sejours plus longs chez les aines
    durees[masque] = np.clip(d, 0.5, 90).round(1)

    # Issue
    issues[masque] = tirer_issues(pathologie, a, durees[masque])

    # Gravite a l'admission, correlee au risque de la pathologie
    risque = TAUX_DECES.get(pathologie, 0.03)
    poids = np.array([max(0.05, 0.55 - 2.2 * risque), 0.30,
                      0.10 + 1.2 * risque, 0.05 + 1.0 * risque])
    gravites[masque] = rng.choice(GRAVITES, size=n, p=poids / poids.sum())

print("[3/6] Constitution du fichier patients (avec rehospitalisations)...")

# Un meme patient peut etre admis plusieurs fois. Les pathologies chroniques
# generent davantage de rehospitalisations : c'est un axe d'analyse du projet.
registre = {}          # (sexe, tranche_age) -> liste d'identites deja creees
identites = []
compteur_patient = 0

for i in range(N_ADMISSIONS):
    sexe, age, patho = sexes[i], ages[i], pathologies[i]
    cle = (sexe, age // 10)
    p_readmission = 0.45 if patho in PATHOLOGIES_CHRONIQUES else 0.12

    if registre.get(cle) and random.random() < p_readmission:
        identites.append(random.choice(registre[cle]))
    else:
        compteur_patient += 1
        prenom = random.choice(PRENOMS_H if sexe == "M" else PRENOMS_F)
        identite = (
            f"PAT{compteur_patient:06d}",
            random.choice(NOMS),
            prenom,
            f"+225 0{random.choice('157')} {random.randint(10, 99)} "
            f"{random.randint(10, 99)} {random.randint(10, 99)} "
            f"{random.randint(10, 99)}",
        )
        registre.setdefault(cle, []).append(identite)
        identites.append(identite)

id_patient, noms, prenoms, telephones = zip(*identites)

print("[4/6] Assemblage du DataFrame...")

dates_entree = pd.to_datetime(dates_entree)
dates_sortie = dates_entree + pd.to_timedelta(durees, unit="D")

# Date de naissance approximative (deduite de l'age, format JJ/MM/AAAA)
jours_naissance = (dates_entree
                   - pd.to_timedelta(ages * 365.25 + rng.integers(0, 365, N_ADMISSIONS),
                                     unit="D"))

# Cout : forfait de service + cout journalier + majoration chirurgicale
cout_journalier = pd.Series(services).map({
    "Urgences": 18_000, "Pediatrie": 12_000, "Medecine interne": 15_000,
    "Maternite": 20_000, "Chirurgie": 32_000, "Cardiologie": 28_000,
    "Neurologie": 30_000, "Maladies infectieuses": 16_000,
}).values
forfait = pd.Series(services).map({
    "Urgences": 25_000, "Pediatrie": 15_000, "Medecine interne": 20_000,
    "Maternite": 45_000, "Chirurgie": 150_000, "Cardiologie": 60_000,
    "Neurologie": 70_000, "Maladies infectieuses": 25_000,
}).values
couts = ((forfait + cout_journalier * durees)
         * rng.normal(1.0, 0.15, N_ADMISSIONS)).round(-2).astype(int)

# Temperature a l'admission : fievre marquee pour les pathologies infectieuses
infectieuses = np.isin(pathologies, [
    "Paludisme simple", "Paludisme grave", "Fievre typhoide", "Pneumopathie",
    "Meningite", "Covid-19", "Infection respiratoire aigue", "Gastro-enterite",
    "Tuberculose"])
temperatures = np.where(infectieuses,
                        rng.normal(38.9, 0.9, N_ADMISSIONS),
                        rng.normal(37.1, 0.6, N_ADMISSIONS)).round(1)

df = pd.DataFrame({
    "id_admission":            [f"ADM{i:07d}" for i in range(1, N_ADMISSIONS + 1)],
    "id_patient":              id_patient,
    "nom_patient":             noms,
    "prenom_patient":          prenoms,
    "date_naissance":          jours_naissance.strftime("%d/%m/%Y"),
    "age":                     ages,
    "sexe":                    sexes,
    "telephone":               telephones,
    "commune_residence":       tirage(COMMUNES, N_ADMISSIONS),
    "assurance":               tirage(ASSURANCES, N_ADMISSIONS),
    "mode_admission":          tirage(MODES_ADMISSION, N_ADMISSIONS),
    "date_entree":             dates_entree,
    "date_sortie":             dates_sortie,
    "duree_sejour_j":          durees,
    "service":                 services,
    "pathologie":              pathologies,
    "gravite":                 gravites,
    "id_medecin":              [f"MED{random.randint(1, 120):03d}"
                                for _ in range(N_ADMISSIONS)],
    "temperature_entree_c":    temperatures,
    "cout_hospitalisation_fcfa": couts,
    "issue":                   issues,
})

# Un deces met fin au sejour : pas de "date de sortie" administrative normale
df.loc[df["issue"] == "Deces", "date_sortie"] = pd.NaT

# =============================================================================
# 4. INJECTION DES DEFAUTS DE QUALITE (matiere premiere de l'ETL)
# =============================================================================

print("[5/6] Injection des anomalies de qualite...")

anomalies = {}

# 4.1 Valeurs manquantes : dossiers incomplets
idx = df.sample(frac=0.052, random_state=1).index
df.loc[idx, "duree_sejour_j"] = np.nan
anomalies["duree_sejour_j manquante"] = len(idx)

idx = df.sample(frac=0.031, random_state=2).index
df.loc[idx, "id_medecin"] = ""
anomalies["id_medecin vide"] = len(idx)

idx = df.sample(frac=0.028, random_state=3).index
df.loc[idx, "temperature_entree_c"] = np.nan
anomalies["temperature manquante"] = len(idx)

idx = df.sample(frac=0.019, random_state=4).index
df.loc[idx, "assurance"] = None
anomalies["assurance manquante"] = len(idx)

# 4.2 Codification heterogene du sexe (plusieurs agents de saisie)
idx = df.sample(frac=0.09, random_state=5).index
df.loc[idx, "sexe"] = df.loc[idx, "sexe"].map({"M": "Masculin", "F": "Feminin"})
idx = df.sample(frac=0.06, random_state=6).index
df.loc[idx, "sexe"] = df.loc[idx, "sexe"].astype(str).str.lower()
idx = df.sample(frac=0.008, random_state=7).index
df.loc[idx, "sexe"] = ""
anomalies["sexe non normalise ou vide"] = "~15%"

# 4.3 Casse et espaces parasites sur la pathologie
idx = df.sample(frac=0.05, random_state=8).index
df.loc[idx, "pathologie"] = df.loc[idx, "pathologie"].str.upper()
idx = df.sample(frac=0.04, random_state=9).index
df.loc[idx, "pathologie"] = "  " + df.loc[idx, "pathologie"] + " "
anomalies["pathologie mal formatee"] = "~9%"

# 4.4 Fautes de frappe sur la commune
FAUTES = {"Yopougon": "Yopougn", "Treichville": "Treichvile",
          "Koumassi": "Koumasi", "Abobo": "ABOBO", "Cocody": "cocody",
          "Port-Bouet": "Port Bouet"}
idx = df.sample(frac=0.035, random_state=10).index
df.loc[idx, "commune_residence"] = df.loc[idx, "commune_residence"].replace(FAUTES)
anomalies["commune mal orthographiee"] = len(idx)

# 4.5 Valeurs aberrantes
idx = df.sample(frac=0.011, random_state=11).index
df.loc[idx, "age"] = rng.choice([-3, 0, 150, 200, 999], size=len(idx))
anomalies["age aberrant"] = len(idx)

idx = df.sample(frac=0.009, random_state=12).index
df.loc[idx, "duree_sejour_j"] = rng.choice([-5.0, -1.0, 365.0, 500.0], size=len(idx))
anomalies["duree_sejour_j aberrante"] = len(idx)

idx = df.sample(frac=0.014, random_state=13).index
df.loc[idx, "cout_hospitalisation_fcfa"] = rng.choice([0, -1, -9999], size=len(idx))
anomalies["cout aberrant"] = len(idx)

idx = df.sample(frac=0.006, random_state=14).index
df.loc[idx, "temperature_entree_c"] = rng.choice([0.0, 3.5, 89.0], size=len(idx))
anomalies["temperature aberrante"] = len(idx)

# 4.6 Incoherence chronologique : sortie anterieure a l'entree
idx = df[df["date_sortie"].notna()].sample(frac=0.008, random_state=15).index
df.loc[idx, "date_sortie"] = df.loc[idx, "date_entree"] - pd.Timedelta(days=2)
anomalies["date_sortie < date_entree"] = len(idx)

# 4.7 Doublons stricts (double saisie dans le SIH)
doublons = df.sample(frac=TAUX_DOUBLONS, random_state=16)
df = pd.concat([df, doublons], ignore_index=True)
df = df.sample(frac=1, random_state=17).reset_index(drop=True)
anomalies["lignes dupliquees"] = len(doublons)

# =============================================================================
# 5. EXPORT ET RAPPORT D'AUDIT
# =============================================================================

print("[6/6] Export...")

df.to_csv(FICHIER_SORTIE, index=False, encoding="utf-8")

print("\n" + "=" * 70)
print(f"  Dataset genere : {FICHIER_SORTIE}")
print("=" * 70)
print(f"  Lignes                : {len(df):,}")
print(f"  Colonnes              : {df.shape[1]}")
print(f"  Patients distincts    : {df['id_patient'].nunique():,}")
print(f"  Periode               : {df['date_entree'].min():%d/%m/%Y} "
      f"-> {df['date_entree'].max():%d/%m/%Y}")
print(f"  Services              : {df['service'].nunique()}")
print(f"  Pathologies (brutes)  : {df['pathologie'].nunique()} "
      f"(dont variantes de casse)")
print("\n  ANOMALIES INJECTEES (a traiter dans l'ETL)")
print("  " + "-" * 66)
for cle, val in anomalies.items():
    val = f"{val:,}" if isinstance(val, int) else val
    print(f"  {cle:<38} {val}")
print("\n  DONNEES A CARACTERE PERSONNEL (a pseudonymiser - loi 2019-992)")
print("  " + "-" * 66)
for col in ["id_patient", "nom_patient", "prenom_patient",
            "date_naissance", "telephone"]:
    print(f"  - {col}")
print("=" * 70)


# --- Dictionnaire des donnees (a reprendre dans le rapport, section 3) ------
DICTIONNAIRE = """# Dictionnaire des donnees — admissions CHU de Treichville

Source : systeme d'information hospitalier (extraction CSV brute, 2023-2024).

| Colonne | Type | Description | Remarques qualite |
|---|---|---|---|
| id_admission | texte | Identifiant du sejour | Doublons stricts presents (~2%) |
| id_patient | texte | Identifiant patient interne | **Donnee personnelle** — a pseudonymiser |
| nom_patient | texte | Nom de famille | **Donnee personnelle** — a supprimer |
| prenom_patient | texte | Prenom | **Donnee personnelle** — a supprimer |
| date_naissance | texte | Format JJ/MM/AAAA | **Donnee personnelle** — a generaliser (annee) |
| age | entier | Age a l'admission | Valeurs aberrantes (-3, 150, 999) |
| sexe | texte | Sexe du patient | Codification heterogene (M/F/Masculin/f/vide) |
| telephone | texte | Contact | **Donnee personnelle** — a supprimer |
| commune_residence | texte | Commune du district d'Abidjan | Fautes de frappe, casse variable |
| assurance | texte | Couverture (CMU, mutuelle, privee, aucune) | ~2% manquants |
| mode_admission | texte | Urgence / Programmee / Transfert | — |
| date_entree | datetime | Horodatage d'admission | — |
| date_sortie | datetime | Horodatage de sortie | Nul si deces ; ~1% anterieure a l'entree |
| duree_sejour_j | decimal | Duree du sejour en jours | ~5% manquants, ~1% aberrants |
| service | texte | Service d'hospitalisation | 8 modalites |
| pathologie | texte | Diagnostic principal | Casse et espaces non normalises |
| gravite | texte | Legere / Moderee / Severe / Critique | — |
| id_medecin | texte | Medecin responsable | ~3% vides |
| temperature_entree_c | decimal | Temperature a l'admission | ~3% manquants, ~1% aberrants |
| cout_hospitalisation_fcfa | entier | Cout facture en FCFA | ~1% negatifs ou nuls |
| issue | texte | Gueri / Transfert / Deces / Sortie contre avis | — |

## Correlations metier presentes dans les donnees

- **Saisonnalite** : le paludisme culmine en saison des pluies (mai-juillet,
  octobre) ; les pathologies respiratoires en periode d'harmattan (dec-fevrier).
- **Service x pathologie** : chaque service a son profil de diagnostics.
- **Age x pathologie** : accouchements 14-49 ans, AVC et HTA apres 55 ans,
  paludisme et denutrition chez l'enfant.
- **Issue** : la mortalite depend de la pathologie, de l'age (>65 ans, <5 ans)
  et de la duree de sejour.
- **Cout** : forfait de service + cout journalier x duree.
- **Rehospitalisations** : les pathologies chroniques (drepanocytose, diabete,
  VIH, insuffisance renale) generent des sejours repetes pour un meme patient.
"""

with open("dictionnaire_donnees.md", "w", encoding="utf-8") as f:
    f.write(DICTIONNAIRE)

print("\nDictionnaire des donnees ecrit dans : dictionnaire_donnees.md")
