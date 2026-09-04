# Construction d'un entrepôt épidémiologique pour le CHU de Treichville

### Projet de fin de module — Data Engineering
### Sujet 3 : Santé publique — suivi épidémiologique au CHU d'Abidjan

---

**Membres du binôme**

| Nom et prénoms |
|---|---|
| *KOUAME GUY MARC AXEL* | 


**Sujet choisi :** n° 3 — Santé publique, suivi épidémiologique au CHU d'Abidjan\
**Dépôt GitHub :** *https://github.com/Vxell/projet-de-chu-treichville.git*\
**Enseignant :** GOUAH Tato Serge — Module Data Engineering

---

\newpage

## Sommaire

1. Introduction
2. Architecture du pipeline
3. Sources de données et audit initial
4. ETL — Extraction
5. ETL — Transformation
6. ETL — Chargement
7. Modélisation dimensionnelle
8. Analyses SQL
9. Tableau de bord
10. Orchestration
11. Conclusion

---

\newpage

## 1. Introduction

### 1.1 Contexte

Le CHU de Treichville est l'un des principaux établissements de recours du
district autonome d'Abidjan. Son activité est marquée par le profil
épidémiologique ivoirien : prédominance des maladies infectieuses, paludisme en
tête, poids croissant des traumatismes de la route, et transition
épidémiologique en cours avec la montée des pathologies chroniques non
transmissibles — hypertension artérielle, diabète, accidents vasculaires
cérébraux.

L'établissement dispose d'un système d'information hospitalier qui enregistre
chaque admission, mais ces données ne sont pas exploitées à des fins d'analyse.
Elles restent dans un format transactionnel, conçu pour la gestion du dossier
patient et non pour la décision.

### 1.2 Problématique métier

La Direction Générale de la Santé nous confie la construction d'un entrepôt
épidémiologique répondant à trois questions opérationnelles :

1. **Quels services sont saturés, et à quelle période de l'année ?** La réponse
   conditionne l'allocation des lits et le calendrier des congés du personnel.
2. **Quelles pathologies concentrent la charge et la mortalité ?** La réponse
   oriente les priorités de formation et d'approvisionnement en médicaments.
3. **Les pics d'activité sont-ils prévisibles ?** Si l'activité suit le
   calendrier climatique, l'établissement peut anticiper plusieurs semaines à
   l'avance au lieu de subir.

### 1.3 Contrainte réglementaire

Les données de santé constituent des données sensibles au sens de la loi
ivoirienne n° 2019-992 relative à la protection des données à caractère
personnel. L'export du SIH contient le nom, le prénom, le numéro de téléphone et
la date de naissance de chaque patient. Aucune de ces données ne peut entrer
dans un entrepôt analytique : le pipeline doit intégrer une étape de
pseudonymisation en amont de tout chargement, et en apporter la preuve.

### 1.4 Objectifs du projet

- Construire un pipeline ETL reproductible, de l'export brut à l'entrepôt.
- Garantir la qualité des données par des contrôles automatiques et bloquants.
- Respecter la loi n° 2019-992 et documenter la conformité.
- Modéliser un schéma dimensionnel permettant l'analyse multi-axes.
- Restituer les résultats sous forme de tableau de bord interprétable.
- Automatiser l'ensemble par une orchestration quotidienne.


---

\newpage

## 2. Architecture du pipeline

### 2.1 Vue d'ensemble

```
   SIH — CHU Treichville
   Export CSV brut (122 400 lignes, 21 colonnes, 24,7 Mo)
            │
   ┌────────▼─────────┐
   │   EXTRACTION     │  Lecture sans inférence de type + audit
   └────────┬─────────┘
   ┌────────▼─────────┐
   │  TRANSFORMATION  │  6 étapes ordonnées (Pandas)
   └────────┬─────────┘
   ┌────────▼─────────┐
   │ PSEUDONYMISATION │  SHA-256 salé · suppression · généralisation
   └────────┬─────────┘
   ┌────────▼─────────┐
   │  ENRICHISSEMENT  │  7 colonnes calculées
   └────────┬─────────┘
   ┌────────▼─────────┐
   │ CONTRÔLE QUALITÉ │  8 règles, dont 6 bloquantes
   └────────┬─────────┘
   ┌────────▼─────────┐
   │   CHARGEMENT     │  Supabase / PostgreSQL 15
   └────────┬─────────┘
            │
   ┌────────┴────────┐
   │                 │
ANALYSES SQL    TABLEAU DE BORD

Orchestration : Airflow, DAG quotidien 02h00 · Conteneurisation : Docker Compose
```

### 2.2 Choix techniques et justifications

| Composant | Choix | Justification |
|---|---|---|
| Transformation | **Pandas** | Volume de 120 000 lignes largement traitable en mémoire (120 Mo). Spark introduirait une complexité d'exploitation sans gain. |
| Entrepôt | **Supabase / PostgreSQL** | PostgreSQL managé, gratuit, avec éditeur SQL intégré. Le SQL est standard : une migration vers une autre instance PostgreSQL ne demanderait aucune réécriture. |
| Format intermédiaire | **Parquet** | Colonnaire et typé : contrairement au CSV, il préserve les types entre deux tâches Airflow, ce qui évite de reconvertir les dates à chaque étape. |
| Orchestration | **Airflow** | Reprise sur erreur, alertes, historique d'exécution. Un simple `cron` ne fournirait ni l'un ni l'autre. |
| Restitution | **Matplotlib** | Export PNG haute résolution intégrable au rapport, sans dépendance à un serveur de restitution. |

### 2.3 Principe de conception : une seule implémentation

Le code métier est regroupé dans `src/pipeline_etl.py`. Le notebook et le DAG
Airflow appellent **les mêmes fonctions**. Ce choix garantit que le pipeline
exploratoire et le pipeline automatisé produisent le même résultat, et qu'une
correction s'applique aux deux simultanément. Recopier le code dans le notebook
aurait créé deux versions divergentes dès la première correction.

---

\newpage

## 3. Sources de données et audit initial

### 3.1 Description de la source

Le fichier provient d'une extraction du SIH couvrant les admissions du
1er janvier 2023 au 31 décembre 2024.

| Caractéristique | Valeur |
|---|---|
| Lignes | 122 400 |
| Colonnes | 21 |
| Taille | 24,7 Mo (120,2 Mo en mémoire) |
| Période | 01/01/2023 → 31/12/2024 |
| Encodage | UTF-8 |

Le jeu de données a été produit par le script `src/generate_dataset.py`, à partir
du profil épidémiologique ivoirien documenté. Les corrélations qu'il porte —
saisonnalité du paludisme, distribution des âges par pathologie, létalité
différenciée — ont été construites explicitement afin que les analyses portent
sur des relations interprétables.

### 3.2 Résultat de l'audit

| Indicateur | Valeur |
|---|---|
| Doublons stricts | 2 400 (1,96 %) |
| Identifiants d'admission dupliqués | 2 400 |
| Cellules manquantes | 24 114 |
| Taux de valeurs manquantes global | 0,94 % |

**Complétude par colonne**

| Colonne | Valeurs manquantes | Interprétation |
|---|---|---|
| `date_sortie` | 7 332 | Séjours clos par décès (sortie non renseignée) |
| `duree_sejour_j` | 6 295 | Dossiers non clôturés |
| `id_medecin` | 3 780 | Séjours non attribués à un praticien |
| `temperature_entree_c` | 3 401 | Constante non relevée à l'admission |
| `assurance` | 2 328 | Couverture non déclarée |
| `sexe` | 978 | Champ laissé vide |

**Cardinalité anormale**

| Colonne | Modalités observées | Modalités réelles | Écart |
|---|---|---|---|
| `pathologie` | 123 | 31 | Casse et espaces parasites |
| `sexe` | 8 | 2 | Six écritures pour deux modalités |
| `commune_residence` | 19 | 13 | Fautes de frappe |

La colonne `pathologie` illustre le risque principal : une analyse menée
directement sur le fichier source aurait produit 123 groupes au lieu de 31, en
éclatant le même diagnostic en trois lignes distinctes — sans qu'aucune erreur
ne soit signalée. Le résultat aurait été faux et vraisemblable.

### 3.3 Données à caractère personnel identifiées

| Colonne | Nature |
|---|---|
| `id_patient` | Identifiant direct |
| `nom_patient`, `prenom_patient` | Identifiants directs |
| `telephone` | Identifiant direct |
| `date_naissance` | Quasi-identifiant |

---

\newpage

## 4. ETL — Extraction

L'extraction est réalisée par la fonction `extraire()` avec une précaution
particulière sur le typage :

```python
df = pd.read_csv(chemin_csv, dtype={"date_naissance": str}, low_memory=False)
```

**Pourquoi désactiver l'inférence sur `date_naissance`.** La colonne est au
format `JJ/MM/AAAA`. L'inférence automatique de pandas suppose par défaut le
format américain `MM/JJ/AAAA` : toute date dont le jour est inférieur ou égal à
12 serait alors silencieusement inversée. Le 03/07/1985 deviendrait le
7 mars 1985. L'erreur ne produirait aucun message et affecterait environ 40 %
des lignes. La conversion est donc reportée à l'étape de typage, avec un format
explicite.

L'extraction produit également un audit de complétude conservé à chaque
exécution : c'est ce suivi qui permettra de détecter une dérive progressive de
la source, par exemple un champ qui se vide au fil des mois à la suite d'un
changement de paramétrage du SIH.

---

\newpage

## 5. ETL — Transformation

### 5.1 Ordre des opérations

L'ordre n'est pas arbitraire : chaque étape dépend du résultat de la précédente.

| Ordre | Opération | Dépendance |
|---|---|---|
| 1 | Dédoublonnage | Doit précéder tout comptage |
| 2 | Normalisation des libellés | Doit précéder tout regroupement |
| 3 | Conversion des types | Doit précéder toute comparaison numérique |
| 4 | Valeurs aberrantes | Nécessite des colonnes typées |
| 5 | Incohérences inter-colonnes | Nécessite des dates typées |
| 6 | Imputation | Doit intervenir en dernier, sur des données saines |

### 5.2 Journal d'exécution

| Étape | Effet mesuré |
|---|---|
| Dédoublonnage | 2 400 doublons stricts supprimés → 120 000 lignes |
| Normalisation | 123 → 31 pathologies, 8 → 3 modalités de sexe, 19 → 13 communes |
| Typage | Conversion des dates, numériques et identifiants |
| Valeurs aberrantes | 1 065 âges, 1 080 durées, 1 680 coûts, 720 températures neutralisés |
| Incohérences | 902 chronologies inversées corrigées, 6 780 durées reconstituées |
| Imputation | 476 durées imputées, 1 065 âges recalculés |

### 5.3 Décisions de conception

**Neutraliser plutôt que supprimer.** Une valeur aberrante est remplacée par
`NaN`, la ligne étant conservée. Un âge saisi à 999 n'invalide pas l'admission :
le séjour a eu lieu, sa durée, son coût et son issue restent exploitables.
Supprimer la ligne aurait détruit vingt colonnes valides pour corriger une seule
cellule. Appliquée aux quatre colonnes concernées, cette approche a préservé
environ 4 500 admissions qui auraient autrement été écartées.

**Reconstruire avant d'imputer.** 6 780 durées de séjour manquantes ont été
recalculées à partir de l'écart entre les dates d'entrée et de sortie, et
1 065 âges à partir de la date de naissance. Il ne restait ensuite que
476 durées à imputer statistiquement, soit 0,4 % des lignes au lieu de 5,2 %.
Une donnée reconstruite à partir d'une autre colonne du même dossier est une
donnée réelle ; une donnée imputée est une estimation.

**Imputer par pathologie, jamais globalement.** La médiane de séjour varie de
2,2 jours pour un accouchement eutocique à 14 jours pour un AVC hémorragique.
Imputer une médiane globale d'environ 5 jours aurait allongé artificiellement
les séjours courts et raccourci les séjours longs, biaisant systématiquement
toute analyse de durée moyenne. La médiane est préférée à la moyenne parce que
la distribution des durées est fortement asymétrique à droite.

Une colonne booléenne `duree_imputee` trace chaque valeur reconstituée : les
analyses sensibles peuvent les exclure.

### 5.4 Pseudonymisation

| Donnée | Traitement | Justification |
|---|---|---|
| `id_patient` | SHA-256 salé, tronqué à 16 caractères | Préserve le chaînage des séjours |
| `nom`, `prénom`, `téléphone` | Suppression | Aucune valeur analytique — minimisation |
| `date_naissance` | Généralisation à l'année | Quasi-identifiant réidentifiant |

**Le rôle du sel.** L'espace des identifiants patients compte moins de
120 000 valeurs de forme connue (`PAT` suivi de six chiffres). Un hachage non
salé serait cassé par force brute en quelques secondes : il suffirait de hacher
les 120 000 candidats et de comparer. Le sel, conservé hors du dépôt dans une
variable d'environnement, rend cette attaque impraticable.

**Pseudonymisation et non anonymisation.** Le traitement reste réversible pour
qui détient le sel. C'est le régime attendu : le CHU doit pouvoir remonter au
dossier patient en cas d'alerte sanitaire. Une anonymisation véritable
interdirait ce retour et rendrait par ailleurs impossible le calcul du taux de
réadmission, qui suppose de reconnaître qu'un même patient revient.

Chaque exécution alimente la table `audit_rgpd` (colonne traitée, opération,
justification, horodatage, volume). Un contrôle bloquant vérifie ensuite
l'absence de toute colonne identifiante — contrôle volontairement distinct de
l'étape qu'il vérifie.

### 5.5 Enrichissement

| Colonne | Calcul | Apport |
|---|---|---|
| `tranche_age` | Découpage OMS en 6 classes | Axe d'analyse épidémiologique |
| `saison` | Mois → calendrier climatique ivoirien | Relie l'activité au climat |
| `categorie_pathologie` | 31 diagnostics → 5 axes | Rend les graphiques lisibles |
| `cout_journalier_fcfa` | Coût ÷ durée | Compare les services à durée égale |
| `sejour_prolonge` | Durée > 90e centile **du service** | Seuil relatif au service |
| `delai_readmission_j` | Écart avec l'admission précédente du patient | Mesure le suivi |
| `est_readmission_30j` | Délai ≤ 30 jours | Indicateur international de qualité |

Le seuil du séjour prolongé est relatif : douze jours sont normaux en neurologie
(durée moyenne 10,8 jours) et anormaux en maternité (3,3 jours). Un seuil unique
aurait classé la neurologie entière en anomalie.

### 5.6 Contrôle qualité

Huit règles, dont six bloquantes.

| Règle | Nature | Résultat |
|---|---|---|
| R1 — Unicité de `id_admission` | Bloquante | Satisfaite |
| R2 — Absence de données personnelles | Bloquante | Satisfaite |
| R3 — Âge dans [0 ; 110] | Bloquante | Satisfaite |
| R4 — Durée de séjour positive | Bloquante | Satisfaite |
| R5 — Cohérence entrée/sortie | Bloquante | Satisfaite |
| R6 — Issue conforme au référentiel | Bloquante | Satisfaite |
| R7 — Pathologie rattachée au référentiel | Informative | Satisfaite |
| R8 — Complétude de la température | Informative | **4 061 valeurs manquantes** |

La distinction entre règle bloquante et règle informative est structurante. Une
règle bloquante porte sur l'intégrité du modèle ou sur la conformité légale :
charger malgré son échec produirait un entrepôt faux ou illégal. Une règle
informative signale une dégradation de la complétude : l'analyse en souffre mais
reste juste, à condition que le défaut soit documenté — ce que fait le rapport
de qualité, conservé à chaque exécution.

En cas d'échec bloquant, la tâche de chargement n'est pas exécutée et l'entrepôt
conserve les données de la veille. Charger des données invalides serait pire que
ne rien charger : les analyses en aval seraient fausses sans que personne ne
s'en aperçoive.

---

\newpage

## 6. ETL — Chargement

### 6.1 Tables créées

| Table | Lignes |
|---|---|
| `faits_admissions` | 120 000 |
| `dim_date` | 731 |
| `dim_pathologie` | 31 |
| `dim_commune` | 13 |
| `dim_service` | 8 |
| `dim_tranche_age` | 7 |
| `dim_issue` | 4 |
| `audit_rgpd` | 5 par exécution |

*[Compléter avec le temps de chargement mesuré sur votre instance Supabase —
critère 1, « chargement Supabase ».]*

### 6.2 Optimisations

**Insertions groupées.** Une insertion ligne à ligne de 120 000 enregistrements
dépasse la vingtaine de minutes et rendrait le notebook inexploitable. Deux
paramètres corrigent cela : `method="multi"` regroupe les lignes en insertions
multi-valeurs, `chunksize=5000` borne la taille de chaque requête pour rester
sous la limite de paramètres du driver PostgreSQL.

**Schéma préservé, index maintenus.** Les index sont créés par le DDL et
restent actifs pendant l'insertion. Les supprimer avant chargement puis les
recréer accélérerait marginalement l'opération, mais ferait perdre les
contraintes d'intégrité pendant toute sa durée. Sur un volume de cette taille, le
temps de chargement reste acceptable : préserver l'intégrité vaut mieux que
gagner quelques secondes.

**Connexion directe.** Supabase expose deux points d'entrée : le pooler
(port 6543) et la connexion directe (port 5432). Le pooler, en mode transaction,
ne conserve pas l'état entre deux requêtes et supporte mal les insertions par
lot. Le chargement utilise donc le port 5432.

La transaction est explicitement validée par `conn.commit()` et le pool de
connexions libéré par `engine.dispose()`.

---

\newpage

## 7. Modélisation dimensionnelle

### 7.1 Schéma en étoile

```
                     ┌──────────────┐
                     │   dim_date   │  731 jours
                     └──────┬───────┘
   ┌──────────────┐         │         ┌────────────────┐
   │ dim_service  ├─────────┼─────────┤ dim_pathologie │
   └──────────────┘         │         └────────────────┘
                   ┌────────┴─────────┐
                   │ faits_admissions │  120 000 lignes
                   └────────┬─────────┘
   ┌──────────────┐         │         ┌────────────────┐
   │ dim_commune  ├─────────┼─────────┤   dim_issue    │
   └──────────────┘         │         └────────────────┘
                     ┌──────┴────────┐
                     │dim_tranche_age│
                     └───────────────┘
```

### 7.2 Justification des choix

**Grain : une ligne = une admission.** Le grain est le choix structurant. Un
grain plus fin — une ligne par journée d'hospitalisation — permettrait des
analyses d'occupation au jour près, au prix d'une multiplication du volume par
la durée moyenne de séjour, soit environ 830 000 lignes. Le grain « admission »
suffit à tous les besoins exprimés : l'occupation se calcule en sommant les
durées.

**Étoile plutôt que flocon.** Les dimensions sont dénormalisées :
`dim_pathologie` porte sa catégorie, `dim_date` porte sa saison. La redondance
est négligeable — 31 lignes — et chaque requête analytique économise une
jointure.

**Clés techniques entières.** Les jointures sur entier sont plus rapides que sur
chaîne, et une correction de libellé n'oblige pas à réécrire la table de faits.

**`id_patient_pseudo` en dimension dégénérée.** Une dimension patient ne
porterait aucun attribut, tous ayant été supprimés ou généralisés par la
pseudonymisation. Elle constituerait un point de concentration de données
sensibles sans contrepartie analytique.

**`dim_service` porte la capacité en lits.** Sans cette donnée, on ne peut
produire qu'un volume d'activité, jamais un taux d'occupation — c'est-à-dire
l'indicateur que la direction attend.

### 7.3 Peuplement

Les dimensions sont construites et peuplées avant la table de faits. Les clés
étrangères sont résolues par dictionnaires de correspondance, puis leur
intégrité est vérifiée avant restitution : les six clés étrangères ont été
résolues sans valeur nulle ni valeur orpheline. Une seule clé non résolue aurait
provoqué un échec d'insertion PostgreSQL après plusieurs minutes de chargement.

---

\newpage

## 8. Analyses SQL

Six requêtes, dont trois mobilisent des fonctions de fenêtrage.

### R1 — Taux d'occupation mensuel par service

**Résultat.** Les urgences atteignent **101,2 % d'occupation en juin 2024**
(10 625 journées d'hospitalisation pour 10 500 journées-lits disponibles),
contre une moyenne annuelle proche de 78 %.

**Interprétation.** Le service fonctionne au-delà de sa capacité déclarée
pendant le pic palustre. La saturation n'est pas structurelle mais saisonnière :
elle appelle un renfort temporaire — lits d'appoint, personnel supplémentaire de
mai à juillet — plutôt qu'une extension permanente.

### R2 — Saisonnalité du paludisme *(fonction de fenêtrage `LAG`)*

**Résultat.** Les admissions pour paludisme passent de **397 cas en février 2024
à 1 380 en juin 2024**, soit une progression de 248 %. La séquence de variations
mensuelles montre une montée continue d'avril à juin, un reflux en août, puis un
second pic en octobre.

**Interprétation.** La courbe suit strictement le calendrier des pluies :
grande saison d'avril à juillet, petite saison en octobre-novembre. Les pics
d'activité sont donc prévisibles plusieurs semaines à l'avance. La conséquence
opérationnelle est directe : les commandes d'antipaludiques et de dérivés
sanguins doivent être passées en mars, non en juin.

### R3 — Durée de séjour et létalité par pathologie et tranche d'âge

**Résultat.** L'AVC hémorragique chez les 65 ans et plus présente une létalité
de **53,4 %** pour une durée moyenne de séjour de 17,5 jours. Suivent l'AVC
ischémique chez les 65 ans et plus (32,7 %) et l'AVC hémorragique chez les
45-64 ans (28,9 %).

**Interprétation.** Les pathologies neuro-vasculaires concentrent à la fois la
mortalité la plus élevée, les séjours les plus longs et les coûts les plus
lourds. C'est le triple critère qui désigne une priorité d'investissement :
une unité neuro-vasculaire dédiée agirait simultanément sur la mortalité et sur
l'occupation des lits.

### R4 — Charge financière par couverture et par commune

**Résultat.** **45,3 % des admissions** concernent des patients sans couverture
maladie. Treichville, Yopougon et Koumassi concentrent les volumes les plus
élevés, avec un coût cumulé supérieur à 1,2 milliard de FCFA par commune.

**Interprétation.** Près d'une admission sur deux repose sur un reste à charge
intégral. Le déploiement de la Couverture Maladie Universelle sur ces trois
communes toucherait la population la plus exposée financièrement.

### R5 — Top 3 des pathologies par service *(fonction de fenêtrage `RANK`)*

**Résultat.** Chaque service présente une concentration marquée : la maternité
consacre 62,4 % de son activité aux accouchements eutociques, la chirurgie
26,5 % aux traumatismes routiers, la cardiologie 33,4 % à l'hypertension
artérielle.

**Interprétation.** Trois diagnostics suffisent à couvrir la majorité de
l'activité de chaque service. Cette concentration permet de cibler la formation
continue et l'approvisionnement plutôt que de les répartir uniformément.

### R6 — Réadmissions à 30 jours par catégorie de pathologie

**Résultat.** Les maladies chroniques non transmissibles affichent un taux de
réadmission à 30 jours de **3,14 %**, contre 1,26 % pour la santé maternelle.
Elles présentent également le plus grand nombre de séjours par patient (1,22).

**Interprétation.** Les patients chroniques reviennent deux fois et demie plus
souvent que la moyenne. Le suivi ambulatoire post-hospitalisation constitue donc
le levier le plus efficace pour réduire la charge : chaque réadmission évitée
libère un lit sans investissement en capacité.

Cette analyse n'aurait pas été possible sans le **hachage déterministe** de
l'identifiant patient. C'est lui qui permet de reconnaître qu'un même patient
revient, tout en respectant l'obligation de pseudonymisation.

---

\newpage

## 9. Tableau de bord

Six graphiques, exportés en PNG à 150 dpi.

### 9.1 Saisonnalité des admissions par catégorie de pathologie

![Saisonnalité des admissions par catégorie de pathologie](dashboards/01_saisonnalite_categories.png)

La heatmap normalise chaque catégorie par sa propre moyenne, faute de quoi seule
la plus volumineuse serait lisible. Les maladies infectieuses présentent deux
bandes chaudes annuelles, correspondant aux deux saisons des pluies. Les
maladies chroniques restent d'intensité constante : elles ne dépendent pas du
climat, ce qui confirme que la variabilité de l'activité hospitalière est
d'origine infectieuse.

### 9.2 Les quinze premiers motifs d'hospitalisation

![Les quinze premiers motifs d'hospitalisation](dashboards/02_top_pathologies.png)

Le paludisme simple (10 744 admissions) et l'accouchement eutocique (10 460)
dominent, suivis du traumatisme routier (9 186). La coloration par catégorie
révèle la structure du profil épidémiologique : les trois premiers motifs
relèvent de trois logiques distinctes — infectieuse, physiologique et
traumatique — qui appellent trois politiques différentes.

### 9.3 Évolution mensuelle des admissions

![Évolution mensuelle des admissions](dashboards/03_evolution_mensuelle.png)

La courbe totale et l'aire du paludisme évoluent en phase. Les bandes bleutées
matérialisent les saisons des pluies. La lecture est sans ambiguïté : les pics
d'activité de l'établissement sont portés par le paludisme, donc par le climat.

### 9.4 Distribution des durées de séjour par service

![Distribution des durées de séjour par service](dashboards/04_duree_par_service.png)

La boîte à moustaches est préférée à un histogramme des moyennes : la
distribution est fortement asymétrique et la moyenne seule masquerait la
dispersion. La maternité présente des séjours courts et resserrés (médiane
3,3 jours), la neurologie des séjours longs et très dispersés (10,8 jours). Un
gestionnaire de lits doit dimensionner sur la dispersion, pas sur la moyenne.

### 9.5 Volume d'activité et létalité par tranche d'âge

![Volume d'activité et létalité par tranche d'âge](dashboards/05_letalite_par_age.png)

Les deux axes racontent deux histoires opposées. La tranche 25-44 ans concentre
le plus grand volume d'admissions avec une létalité de 4,1 %. Les 65 ans et
plus, quatre fois moins nombreux, atteignent 17,8 %. Volume et gravité ne se
superposent pas : le premier détermine la charge de travail, le second l'exigence
de compétence. Les deux appellent des décisions distinctes.

### 9.6 Taux d'occupation moyen par service

![Taux d'occupation moyen par service](dashboards/06_occupation_services.png)

Les barres d'amplitude portent ici l'information principale : elles mesurent
l'irrégularité de la charge mensuelle. Un service dont l'occupation moyenne est
acceptable mais l'amplitude large connaît des périodes de saturation invisibles
dans la moyenne annuelle. C'est le cas des urgences et de la pédiatrie.

---

\newpage

## 10. Orchestration

### 10.1 Le DAG

```
debut → extraire → transformer → controler_qualite → charger_supabase
      → generer_dashboard → fin
```

| Tâche | Rôle |
|---|---|
| `extraire` | Lecture du fichier du jour, audit de complétude |
| `transformer` | Nettoyage, pseudonymisation, enrichissement |
| `controler_qualite` | Huit règles ; échec bloquant → arrêt du DAG |
| `charger_supabase` | Construction du schéma en étoile et chargement |
| `generer_dashboard` | Régénération des six graphiques |

**Planification :** `0 2 * * *` — 02h00 chaque jour. Le SIH clôture ses
écritures à minuit ; deux heures de marge évitent de lire un extrait incomplet.

### 10.2 Gestion des erreurs

| Paramètre | Valeur | Motif |
|---|---|---|
| `retries` | 2 | Les échecs observés sont des coupures réseau vers Supabase |
| `retry_delay` | 5 min, exponentiel | Laisse le temps au service de se rétablir |
| `email_on_failure` | Activé | Alerte après épuisement des tentatives seulement |
| `email_on_retry` | Désactivé | Évite de noyer l'exploitant sous des alertes sans objet |
| `max_active_runs` | 1 | La base cible est en écriture exclusive pendant le chargement |
| `catchup` | `False` | Pas de rattrapage : seul l'extrait du jour a un sens |

### 10.3 Circulation des données entre tâches

Les données transitent par fichiers Parquet, non par XCom. XCom stocke ses
valeurs dans la base de métadonnées d'Airflow et n'est dimensionné que pour
quelques kilo-octets ; y faire passer 120 000 lignes la saturerait. Seuls les
chemins de fichiers et les compteurs circulent par XCom.

### 10.4 Conteneurisation

`docker-compose.yml` définit trois services : PostgreSQL (métadonnées Airflow),
Airflow (ordonnanceur et interface web en `LocalExecutor`), et Jupyter
(exécution du notebook). Les trois partagent la même image, donc les mêmes
versions de dépendances — ce qui garantit que le notebook et le DAG s'exécutent
dans des conditions identiques.

Les secrets sont injectés depuis le fichier `.env` au démarrage et ne figurent
jamais dans l'image.

### 10.5 Deux images distinctes, et pourquoi

Le projet construit **deux images** et non une seule. Airflow 2.10 ne fonctionne
qu'avec SQLAlchemy 1.4 ; le pipeline, lui, a été écrit avec SQLAlchemy 2.x pour
Supabase. Les faire cohabiter dans un même environnement est impossible.

Chaque service reçoit donc l'environnement dont il a besoin : l'image Airflow
installe `docker/requirements-airflow.txt`, qui exclut délibérément SQLAlchemy et
psycopg2 — déjà fournis et contraints par Airflow — et plafonne pandas à la
série 2.1. L'image Jupyter, construite sur une base Python nue, installe le
`requirements.txt` du projet sans contrainte.

C'est la justification profonde de la conteneurisation dans ce projet. Elle ne
sert pas d'abord à la portabilité, mais à faire coexister deux composants du même
système qui exigent des versions incompatibles de la même bibliothèque.

---

\newpage

## 11. Conclusion

### 11.1 Difficultés rencontrées

**L'inférence de format de date.** Le format `JJ/MM/AAAA` de la colonne
`date_naissance` était silencieusement inversé par pandas pour toutes les dates
dont le jour est inférieur ou égal à 12. L'erreur ne produisait aucun message et
n'a été détectée qu'en contrôlant la cohérence entre l'âge déclaré et l'âge
recalculé. Correction : format explicite à la conversion.

**Le schéma détruit au chargement.** Le pipeline chargeait initialement les
tables avec `if_exists="replace"`. Or ce mode supprime la table et la recrée avec
les types devinés par pandas : le schéma déclaré dans le DDL était écrasé à
chaque exécution, avec ses contraintes de clé étrangère et ses `CHECK`. Rien ne
le signalait — les tables existaient, elles se remplissaient, et l'entrepôt
n'avait plus aucune intégrité référentielle. Le symptôme n'est apparu que
plusieurs jours plus tard, sous la forme d'une erreur SQL sur le type d'une
colonne de mesure devenue `double precision` au lieu de `numeric`. Correction :
`TRUNCATE` puis `append`, le DDL faisant seul autorité sur le schéma.

**Le conflit de versions entre Airflow et le pipeline.** L'incident le plus
instructif du projet, parce qu'il s'est manifesté trois fois sous trois formes
différentes avant que la cause commune n'apparaisse.

| Symptôme observé | Cause immédiate |
|---|---|
| `MappedAnnotationError` sur `TaskInstance` au démarrage d'Airflow | SQLAlchemy 2.x installé par-dessus celui d'Airflow |
| `Connection object has no attribute 'commit'` | API SQLAlchemy 2.0 appelée sur une connexion 1.4 |
| `Engine object has no attribute 'cursor'` | pandas 2.2 exige SQLAlchemy ≥ 2.0 |

Une seule cause racine : Airflow 2.10 impose SQLAlchemy 1.4, le pipeline a été
écrit avec SQLAlchemy 2.x. Chaque correction partielle n'a fait que déplacer le
point de rupture vers la dépendance suivante — d'abord Airflow lui-même, puis
notre code, puis pandas.

Deux enseignements. D'une part, une contrainte de version ne s'arrête pas à la
bibliothèque concernée : fixer une version d'Airflow revient à fixer
indirectement celle de pandas. D'autre part, la solution n'est pas de trouver un
jeu de versions commun — il n'existe pas — mais d'isoler les environnements.

**Des incohérences entre points d'entrée.** Le notebook chargeait le fichier
`.env`, le script `dashboard.py` non : la pseudonymisation s'interrompait faute
de sel lorsqu'on lançait le tableau de bord en ligne de commande. De même, le
rendu Matplotlib échouait sur une version de FreeType différente de celle du
poste de développement. Ces défauts n'apparaissent jamais dans l'environnement où
le code est écrit ; ils apparaissent à chaque frontière entre environnements.

**La lenteur du chargement initial.** Le premier chargement de 120 000 lignes
par `to_sql` a dépassé vingt minutes, rendant le notebook inutilisable.
`method="multi"` combiné à `chunksize=5000` a ramené le temps à moins d'une
minute.

**Le calcul des parts avec fonction de fenêtrage.** Dans la requête R4, filtrer
sur les patients non assurés avant de calculer la part ramenait toutes les parts
à 100 %, la fenêtre ne voyant plus qu'une modalité par commune. La correction a
consisté à découper la requête en deux CTE : agrégation complète, calcul de la
part, puis filtre.

**L'articulation entre pseudonymisation et analyse.** Supprimer purement et
simplement l'identifiant patient aurait rendu impossible le calcul du taux de
réadmission. C'est le hachage déterministe qui a permis de concilier les deux
exigences — préserver le chaînage sans conserver l'identifiant.

### 11.2 Compétences acquises

- Conception d'un pipeline ETL reproductible et testé, avec séparation nette
  entre code métier, orchestration et restitution.
- Modélisation dimensionnelle : choix du grain, arbitrage étoile/flocon, gestion
  de l'intégrité référentielle.
- Traitement des données de santé sous contrainte réglementaire, et articulation
  entre exigence de confidentialité et besoin analytique.
- SQL analytique avancé : CTE, fonctions de fenêtrage, agrégats conditionnels.
- Orchestration Airflow et conteneurisation Docker.

---

\newpage

## Annexe  — Environnement technique

| Composant | Version |
|---|---|
| Python | 3.11 |
| pandas | 2.2.3 |
| SQLAlchemy | 2.0.36 |
| PostgreSQL (Supabase) | 15 |
| Apache Airflow | 2.10.3 |
| Matplotlib | 3.9.2 |
