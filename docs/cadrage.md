# Cadrage du projet

**Sujet 3 — Santé publique : suivi épidémiologique au CHU d'Abidjan**

Document rédigé avant le début du développement. Il fixe la problématique
métier, les contraintes et les choix techniques, et sert de référence pour
arbitrer les décisions de conception rencontrées en cours de route.

---

## 1. Contexte

Le CHU de Treichville est l'un des principaux établissements de recours du
district autonome d'Abidjan. Son activité reflète le profil épidémiologique
ivoirien :

- prédominance des maladies infectieuses, le paludisme en tête ;
- poids important des traumatismes de la route ;
- transition épidémiologique en cours, avec la montée des pathologies
  chroniques non transmissibles (hypertension artérielle, diabète, accidents
  vasculaires cérébraux).

L'établissement dispose d'un système d'information hospitalier qui enregistre
chaque admission, mais ces données restent dans un format transactionnel,
conçu pour la gestion du dossier patient et non pour la décision. Elles ne
sont pas exploitées à des fins d'analyse.

## 2. Problématique métier

La Direction Générale de la Santé attend des réponses à trois questions
opérationnelles :

| Question | Décision qu'elle conditionne |
|---|---|
| Quels services sont saturés, et à quelle période ? | Allocation des lits, calendrier des congés du personnel |
| Quelles pathologies concentrent la charge et la mortalité ? | Priorités de formation et d'approvisionnement |
| Les pics d'activité sont-ils prévisibles ? | Anticipation des commandes et des renforts |

La troisième question est la plus structurante. Si l'activité suit le
calendrier climatique, l'établissement peut anticiper plusieurs semaines à
l'avance au lieu de subir.

## 3. Contraintes

### 3.1 Contrainte réglementaire

Les données de santé constituent des données sensibles au sens de la loi
ivoirienne **n° 2019-992** relative à la protection des données à caractère
personnel.

L'export du SIH contient le nom, le prénom, le numéro de téléphone et la date
de naissance de chaque patient. Aucune de ces données ne peut entrer dans un
entrepôt analytique. Le pipeline doit intégrer une étape de pseudonymisation
en amont de tout chargement, et en apporter la preuve.

Difficulté anticipée : la suppression pure et simple de l'identifiant patient
rendrait impossible toute analyse de réhospitalisation. Il faudra concilier
confidentialité et chaînage des séjours.

### 3.2 Contrainte de qualité

L'export est un fichier brut, alimenté par plusieurs dizaines d'agents de
saisie. Les défauts attendus :

- codifications divergentes pour un même champ ;
- doublons issus de doubles soumissions du formulaire ;
- valeurs aberrantes et incohérences entre colonnes ;
- dossiers incomplets.

Le pipeline doit les détecter, les documenter et les traiter — sans les faire
disparaître silencieusement.

## 4. Choix techniques

| Composant | Choix | Justification |
|---|---|---|
| Transformation | **Pandas** | Volume de ~120 000 lignes (≈120 Mo en mémoire), largement traitable sur une seule machine. Spark introduirait une complexité d'exploitation sans gain. |
| Entrepôt | **Supabase / PostgreSQL 15** | PostgreSQL managé, gratuit, éditeur SQL intégré. Le SQL reste standard : une migration vers une autre instance ne demanderait aucune réécriture. |
| Modélisation | **Schéma en étoile** | Adapté à l'analyse multi-axes. Le flocon économiserait quelques dizaines de lignes de stockage au prix d'une jointure supplémentaire par requête. |
| Format intermédiaire | **Parquet** | Colonnaire et typé : préserve les types entre deux tâches, contrairement au CSV qui obligerait à reconvertir les dates à chaque étape. |
| Orchestration | **Apache Airflow** | Reprise sur erreur, alertes, historique d'exécution. Un `cron` ne fournirait ni l'un ni l'autre. |
| Restitution | **Matplotlib** | Export PNG haute résolution intégrable au rapport, sans dépendance à un serveur de restitution. |
| Conteneurisation | **Docker Compose** | Garantit que le notebook et le DAG s'exécutent dans des conditions identiques. |

## 5. Principe de conception retenu

**Une seule implémentation du code métier.**

Le code de transformation sera regroupé dans `src/`, et appelé aussi bien par
le notebook que par le DAG Airflow. Recopier la logique dans le notebook
créerait deux versions qui divergeraient dès la première correction.

Conséquence : le notebook n'est pas un brouillon, c'est la démonstration
pas à pas d'un pipeline dont le code fait autorité.

## 6. Jeu de données

Le sujet autorise la génération du jeu de données. Il sera produit par un
script versionné plutôt que téléchargé, pour deux raisons :

1. **Reproductibilité** — le script figé par une graine aléatoire reproduit le
   fichier à l'identique, ce qu'un fichier de 25 Mo versionné ne garantirait
   pas mieux tout en alourdissant le dépôt.
2. **Réalisme contrôlé** — les corrélations métier (saisonnalité du paludisme,
   distribution des âges par pathologie, létalité différenciée) doivent être
   construites explicitement, faute de quoi les analyses porteraient sur du
   bruit et ne seraient pas interprétables.

Volume cible : **120 000 admissions** sur deux années civiles (2023-2024), au
delà du minimum de 60 000 demandé, afin de disposer d'effectifs suffisants sur
les croisements pathologie × tranche d'âge.

## 7. Plan de travail

| Étape | Livrable |
|---|---|
| 1 | Générateur du jeu de données et dictionnaire |
| 2 | Extraction et audit du fichier source |
| 3 | Nettoyage (6 opérations ordonnées) |
| 4 | Pseudonymisation et journal d'audit |
| 5 | Enrichissement métier |
| 6 | Contrôles qualité bloquants |
| 7 | Schéma en étoile et chargement Supabase |
| 8 | Requêtes analytiques |
| 9 | Tableau de bord |
| 10 | Orchestration Airflow et conteneurisation |
| 11 | Rapport et soutenance |

## 8. Risques identifiés

| Risque | Parade |
|---|---|
| Chargement de 120 000 lignes trop lent | Insertions groupées, index posés après chargement |
| Notebook non exécutable sur un autre poste | Aucun secret en dur, dépendances figées, test sur clone propre avant rendu |
| Analyses faussées par des libellés non normalisés | Référentiels métier explicites, contrôle du nombre de modalités après nettoyage |
| Perte du chaînage patient après pseudonymisation | Hachage déterministe salé plutôt que suppression |