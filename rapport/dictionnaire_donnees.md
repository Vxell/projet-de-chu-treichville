# Dictionnaire des donnees — admissions CHU de Treichville

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
