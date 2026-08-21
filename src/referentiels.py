"""
Référentiels métier du pipeline CHU de Treichville.

Ce module centralise toutes les tables de correspondance utilisées par l'ETL.
Les isoler ici plutôt que de les disséminer dans le code de nettoyage permet
de les faire évoluer sans toucher à la logique de transformation, et de les
réutiliser à l'identique dans le notebook et dans le DAG Airflow.
"""

# =============================================================================
# 1. NORMALISATION DU SEXE
# =============================================================================
# Le système d'information hospitalier accepte la saisie libre : on retrouve
# six écritures différentes pour deux modalités réelles.
MAPPING_SEXE = {
    "m": "M", "masculin": "M", "h": "M", "homme": "M",
    "f": "F", "feminin": "F", "féminin": "F", "femme": "F",
}
SEXE_INCONNU = "Non renseigné"

# =============================================================================
# 2. RÉFÉRENTIEL DES PATHOLOGIES
# =============================================================================
# Clé  : forme normalisée (minuscules, sans espaces superflus) telle qu'on la
#        trouve dans le fichier source, quelles que soient la casse et les
#        espaces parasites.
# Valeur : libellé officiel retenu pour l'entrepôt.
REFERENTIEL_PATHOLOGIES = {
    "paludisme simple": "Paludisme simple",
    "paludisme grave": "Paludisme grave",
    "fievre typhoide": "Fièvre typhoïde",
    "gastro-enterite": "Gastro-entérite",
    "pneumopathie": "Pneumopathie",
    "infection respiratoire aigue": "Infection respiratoire aiguë",
    "tuberculose": "Tuberculose",
    "vih - infection opportuniste": "VIH – infection opportuniste",
    "hepatite virale b": "Hépatite virale B",
    "meningite": "Méningite",
    "covid-19": "Covid-19",
    "drepanocytose (crise)": "Drépanocytose (crise)",
    "anemie severe": "Anémie sévère",
    "hypertension arterielle": "Hypertension artérielle",
    "insuffisance cardiaque": "Insuffisance cardiaque",
    "avc ischemique": "AVC ischémique",
    "avc hemorragique": "AVC hémorragique",
    "diabete decompense": "Diabète décompensé",
    "insuffisance renale": "Insuffisance rénale",
    "epilepsie": "Épilepsie",
    "accouchement eutocique": "Accouchement eutocique",
    "cesarienne": "Césarienne",
    "pre-eclampsie": "Pré-éclampsie",
    "traumatisme routier": "Traumatisme routier",
    "fracture": "Fracture",
    "appendicite aigue": "Appendicite aiguë",
    "hernie etranglee": "Hernie étranglée",
    "occlusion intestinale": "Occlusion intestinale",
    "brulure": "Brûlure",
    "denutrition aigue": "Dénutrition aiguë",
    "prematurite": "Prématurité",
}

# Regroupement analytique : 31 diagnostics ramenés à 5 axes épidémiologiques.
# C'est ce niveau d'agrégation qui rend les graphiques lisibles et qui permet
# de raisonner en termes de politique de santé publique.
CATEGORIES_PATHOLOGIE = {
    "Maladie infectieuse": [
        "Paludisme simple", "Paludisme grave", "Fièvre typhoïde",
        "Gastro-entérite", "Pneumopathie", "Infection respiratoire aiguë",
        "Tuberculose", "VIH – infection opportuniste", "Hépatite virale B",
        "Méningite", "Covid-19",
    ],
    "Maladie chronique non transmissible": [
        "Hypertension artérielle", "Insuffisance cardiaque", "AVC ischémique",
        "AVC hémorragique", "Diabète décompensé", "Insuffisance rénale",
        "Épilepsie", "Drépanocytose (crise)", "Anémie sévère",
    ],
    "Santé maternelle": [
        "Accouchement eutocique", "Césarienne", "Pré-eclampsie",
        "Pré-éclampsie",
    ],
    "Traumatologie et chirurgie": [
        "Traumatisme routier", "Fracture", "Appendicite aiguë",
        "Hernie étranglée", "Occlusion intestinale", "Brûlure",
    ],
    "Néonatalogie et nutrition": [
        "Prématurité", "Dénutrition aiguë",
    ],
}
# Inversion pour un accès direct pathologie -> catégorie
PATHOLOGIE_VERS_CATEGORIE = {
    patho: categorie
    for categorie, liste in CATEGORIES_PATHOLOGIE.items()
    for patho in liste
}

# =============================================================================
# 3. RÉFÉRENTIEL DES SERVICES
# =============================================================================
REFERENTIEL_SERVICES = {
    "urgences": "Urgences",
    "pediatrie": "Pédiatrie",
    "medecine interne": "Médecine interne",
    "maternite": "Maternité",
    "chirurgie": "Chirurgie",
    "cardiologie": "Cardiologie",
    "neurologie": "Neurologie",
    "maladies infectieuses": "Maladies infectieuses",
}

# Capacité en lits déclarée par service : indispensable pour calculer un
# taux d'occupation, qui est un rapport et non un simple volume.
#
# Ces capacités correspondent au périmètre modélisé (CHU de Treichville et
# services rattachés). Elles ont été calibrées sur l'activité observée de
# façon à situer l'occupation moyenne autour de 80 %, valeur cible d'un
# établissement de recours : un taux inférieur signale un sous-emploi des
# lits, un taux durablement supérieur une saturation structurelle.
LITS_PAR_SERVICE = {
    "Urgences": 350, "Médecine interne": 270, "Pédiatrie": 220,
    "Chirurgie": 190, "Cardiologie": 140, "Neurologie": 110,
    "Maternité": 100, "Maladies infectieuses": 65,
}   # total : 1 445 lits

# =============================================================================
# 4. RÉFÉRENTIEL GÉOGRAPHIQUE (district autonome d'Abidjan)
# =============================================================================
# Les clés couvrent les fautes de frappe relevées lors de l'audit du fichier
# source (Yopougn, Treichvile, Koumasi, Port Bouet...).
REFERENTIEL_COMMUNES = {
    "abobo": "Abobo",
    "adjame": "Adjamé",
    "attecoube": "Attécoubé",
    "cocody": "Cocody",
    "koumassi": "Koumassi", "koumasi": "Koumassi",
    "marcory": "Marcory",
    "plateau": "Plateau",
    "port-bouet": "Port-Bouët", "port bouet": "Port-Bouët",
    "treichville": "Treichville", "treichvile": "Treichville",
    "yopougon": "Yopougon", "yopougn": "Yopougon",
    "bingerville": "Bingerville",
    "anyama": "Anyama",
    "songon": "Songon",
}

# =============================================================================
# 5. TRANCHES D'ÂGE (découpage OMS simplifié)
# =============================================================================
TRANCHES_AGE = [
    # (ordre, libellé, borne_min, borne_max)
    (1, "0-4 ans (petite enfance)", 0, 4),
    (2, "5-14 ans (enfance)", 5, 14),
    (3, "15-24 ans (adolescence)", 15, 24),
    (4, "25-44 ans (adulte jeune)", 25, 44),
    (5, "45-64 ans (adulte)", 45, 64),
    (6, "65 ans et plus (âgé)", 65, 120),
]

# =============================================================================
# 6. CALENDRIER IVOIRIEN
# =============================================================================
# Abidjan connaît quatre saisons. Le découpage retenu oppose les périodes
# pluvieuses (vecteur anophèle actif) à la saison sèche et à l'harmattan.
SAISON_PAR_MOIS = {
    1: "Saison sèche", 2: "Saison sèche", 3: "Saison sèche",
    4: "Grande saison des pluies", 5: "Grande saison des pluies",
    6: "Grande saison des pluies", 7: "Grande saison des pluies",
    8: "Petite saison sèche", 9: "Petite saison sèche",
    10: "Petite saison des pluies", 11: "Petite saison des pluies",
    12: "Saison sèche",
}
LIBELLE_MOIS = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
    7: "juillet", 8: "août", 9: "septembre", 10: "octobre",
    11: "novembre", 12: "décembre",
}
LIBELLE_JOUR = {
    0: "lundi", 1: "mardi", 2: "mercredi", 3: "jeudi",
    4: "vendredi", 5: "samedi", 6: "dimanche",
}

# =============================================================================
# 7. AUTRES RÉFÉRENTIELS
# =============================================================================
ISSUES_VALIDES = ["Gueri", "Transfert", "Deces", "Sortie contre avis"]
LIBELLE_ISSUE = {
    "Gueri": "Guérison", "Transfert": "Transfert",
    "Deces": "Décès", "Sortie contre avis": "Sortie contre avis médical",
}
ASSURANCES_VALIDES = ["Aucune", "CMU", "Mutuelle employeur", "Assurance privée"]
MAPPING_ASSURANCE = {
    "aucune": "Aucune", "cmu": "CMU",
    "mutuelle employeur": "Mutuelle employeur",
    "assurance privee": "Assurance privée",
}
MODES_ADMISSION_VALIDES = ["Urgence", "Programmee", "Transfert"]
GRAVITES_VALIDES = ["Legere", "Moderee", "Severe", "Critique"]

# =============================================================================
# 8. BORNES DE PLAUSIBILITÉ (détection des valeurs aberrantes)
# =============================================================================
BORNES = {
    "age": (0, 110),                        # doyenne ivoirienne < 110 ans
    "duree_sejour_j": (0.1, 90),            # au-delà : erreur de saisie
    "temperature_entree_c": (30.0, 45.0),   # hors de ces bornes : incompatible
    "cout_hospitalisation_fcfa": (1, 20_000_000),
}

# =============================================================================
# 9. DONNÉES À CARACTÈRE PERSONNEL (loi ivoirienne n° 2019-992)
# =============================================================================
# Colonnes directement identifiantes : supprimées après pseudonymisation.
COLONNES_IDENTIFIANTES = ["nom_patient", "prenom_patient", "telephone"]
# Colonne quasi-identifiante : généralisée (date exacte -> année seule).
COLONNE_A_GENERALISER = "date_naissance"
# Colonne pivot : hachée avec sel pour conserver le chaînage des séjours.
COLONNE_A_HACHER = "id_patient"
