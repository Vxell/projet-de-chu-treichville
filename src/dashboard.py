"""
=============================================================================
 TABLEAU DE BORD — ENTREPÔT ÉPIDÉMIOLOGIQUE DU CHU DE TREICHVILLE
=============================================================================
 Six graphiques Matplotlib, exportés individuellement en PNG à 150 dpi ainsi
 qu'en planche de synthèse.

 Les graphiques sont construits à partir des tables du schéma en étoile
 (dimensions + faits) et non des données brutes : ils valident donc au passage
 que le modèle dimensionnel répond bien aux besoins d'analyse.
=============================================================================
"""

import os

import matplotlib
matplotlib.use("Agg")           # backend sans affichage, nécessaire en conteneur
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Charte graphique du projet : une couleur par catégorie épidémiologique,
# conservée sur l'ensemble des graphiques pour faciliter la lecture croisée.
COULEURS = {
    "Maladie infectieuse": "#C0392B",
    "Maladie chronique non transmissible": "#2471A3",
    "Santé maternelle": "#B7950B",
    "Traumatologie et chirurgie": "#7D3C98",
    "Néonatalogie et nutrition": "#1E8449",
}
BLEU, ROUGE, GRIS = "#2471A3", "#C0392B", "#5D6D7E"

plt.rcParams.update({
    "figure.dpi": 110,
    "savefig.dpi": 150,          # exigence du sujet : >= 150 dpi
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def _joindre(dims, faits):
    """Reconstitue une vue analytique à plat par jointure sur les dimensions."""
    return (faits
            .merge(dims["dim_date"], on="date_id")
            .merge(dims["dim_service"], on="service_id")
            .merge(dims["dim_pathologie"], on="pathologie_id")
            .merge(dims["dim_tranche_age"], on="tranche_id")
            .merge(dims["dim_commune"], on="commune_id")
            .merge(dims["dim_issue"], on="issue_id"))


# =============================================================================
# GRAPHIQUE 1 — Heatmap mensuelle des catégories de pathologies
# =============================================================================

def g1_heatmap_saisonnalite(v, ax):
    """
    Croise les 24 mois de la période avec les 5 catégories épidémiologiques.

    La heatmap est le format adapté ici : elle rend visible d'un coup d'œil
    la double saisonnalité des maladies infectieuses, qu'une courbe par
    catégorie noierait dans les écarts de volume entre catégories.
    """
    pivot = (v.pivot_table(index="categorie", columns="annee_mois",
                           values="id_admission", aggfunc="count")
             .fillna(0))
    # Normalisation ligne par ligne : chaque catégorie est comparée à sa
    # propre moyenne, sinon seule la plus volumineuse serait lisible.
    normalise = pivot.div(pivot.mean(axis=1), axis=0)

    im = ax.imshow(normalise.values, aspect="auto", cmap="RdYlBu_r",
                   vmin=0.5, vmax=1.5)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=90, fontsize=7)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([c.replace(" non transmissible", "")
                        for c in pivot.index], fontsize=8)
    ax.set_title("Saisonnalité des admissions par catégorie de pathologie\n"
                 "(indice 1,0 = moyenne de la catégorie)")
    ax.grid(False)
    plt.colorbar(im, ax=ax, shrink=0.7, label="Indice d'activité")


# =============================================================================
# GRAPHIQUE 2 — Pyramide des pathologies
# =============================================================================

def g2_pyramide_pathologies(v, ax):
    """Classe les 15 premiers motifs d'hospitalisation, colorés par catégorie."""
    top = (v.groupby(["libelle_pathologie", "categorie"])
           .size().reset_index(name="nb")
           .nlargest(15, "nb").sort_values("nb"))

    ax.barh(top["libelle_pathologie"], top["nb"],
            color=[COULEURS.get(c, GRIS) for c in top["categorie"]])
    for y, (nb, ) in enumerate(zip(top["nb"])):
        ax.text(nb + 200, y, f"{nb:,}".replace(",", " "),
                va="center", fontsize=8)
    ax.set_xlabel("Nombre d'admissions")
    ax.set_title("Les 15 premiers motifs d'hospitalisation")
    ax.set_xlim(0, top["nb"].max() * 1.15)

    poignees = [plt.Rectangle((0, 0), 1, 1, color=c)
                for c in COULEURS.values()]
    ax.legend(poignees, [k.replace(" non transmissible", "")
                         for k in COULEURS], fontsize=7, loc="lower right")


# =============================================================================
# GRAPHIQUE 3 — Évolution mensuelle et pics de paludisme
# =============================================================================

def g3_evolution_mensuelle(v, ax):
    """
    Superpose l'activité totale et les admissions pour paludisme.

    L'objectif est de montrer que les pics d'activité de l'établissement ne
    sont pas aléatoires : ils suivent la courbe du paludisme, donc le
    calendrier des pluies.
    """
    total = v.groupby("annee_mois").size()
    palu = (v[v["libelle_pathologie"].str.startswith("Paludisme")]
            .groupby("annee_mois").size().reindex(total.index, fill_value=0))

    x = range(len(total))
    ax.plot(x, total.values, marker="o", ms=3, color=BLEU, lw=1.8,
            label="Toutes admissions")
    ax.fill_between(x, palu.values, color=ROUGE, alpha=0.35,
                    label="dont paludisme")
    ax.plot(x, palu.values, color=ROUGE, lw=1.5)

    # Repérage des deux saisons des pluies
    for i, mois in enumerate(total.index):
        m = int(mois.split("-")[1])
        if m in (4, 5, 6, 7, 10, 11):
            ax.axvspan(i - 0.5, i + 0.5, color="#5DADE2", alpha=0.07)

    ax.set_xticks(list(x)[::2])
    ax.set_xticklabels(total.index[::2], rotation=90, fontsize=7)
    ax.set_ylabel("Admissions")
    ax.set_title("Évolution mensuelle des admissions\n"
                 "(bandes bleutées : saisons des pluies)")
    ax.legend(fontsize=8)


# =============================================================================
# GRAPHIQUE 4 — Distribution des durées de séjour par service
# =============================================================================

def g4_duree_par_service(v, ax):
    """
    Boîtes à moustaches des durées de séjour.

    Le boxplot est retenu plutôt qu'un histogramme des moyennes : la
    distribution des durées est fortement asymétrique et la moyenne seule
    masquerait la dispersion, qui est précisément ce qui intéresse un
    gestionnaire de lits.
    """
    ordre = (v.groupby("libelle_service")["duree_sejour_j"].median()
             .sort_values().index)
    donnees = [v.loc[v["libelle_service"] == s, "duree_sejour_j"].dropna()
               for s in ordre]

    bp = ax.boxplot(donnees, vert=False, showfliers=False, patch_artist=True,
                    medianprops=dict(color="black", lw=1.6))
    for boite in bp["boxes"]:
        boite.set(facecolor=BLEU, alpha=0.55)

    ax.set_yticklabels(ordre, fontsize=8)
    ax.set_xlabel("Durée de séjour (jours)")
    ax.set_title("Distribution des durées de séjour par service")


# =============================================================================
# GRAPHIQUE 5 — Létalité et volume par tranche d'âge
# =============================================================================

def g5_letalite_par_age(v, ax):
    """
    Croise volume d'activité et létalité sur deux axes.

    Lecture attendue : les tranches les plus nombreuses ne sont pas les plus
    létales. C'est la distinction entre charge de travail et gravité, qui
    n'appelle pas les mêmes décisions d'allocation de moyens.
    """
    stats = (v.groupby(["ordre", "libelle_tranche"])
             .agg(nb=("id_admission", "count"),
                  deces=("est_deces", "sum"))
             .reset_index().sort_values("ordre"))
    stats = stats[stats["ordre"] < 90]        # exclut « âge non renseigné »
    stats["letalite"] = 100 * stats["deces"] / stats["nb"]
    libelles = [t.split(" (")[0] for t in stats["libelle_tranche"]]

    ax.bar(libelles, stats["nb"], color=BLEU, alpha=0.75, label="Admissions")
    ax.set_ylabel("Nombre d'admissions", color=BLEU)
    ax.tick_params(axis="y", labelcolor=BLEU)
    ax.tick_params(axis="x", labelrotation=25, labelsize=8)

    ax2 = ax.twinx()
    ax2.plot(libelles, stats["letalite"], color=ROUGE, marker="s", ms=6, lw=2,
             label="Létalité")
    ax2.set_ylabel("Taux de létalité (%)", color=ROUGE)
    ax2.tick_params(axis="y", labelcolor=ROUGE)
    ax2.grid(False)
    for i, t in enumerate(stats["letalite"]):
        ax2.text(i, t + 0.4, f"{t:.1f} %", ha="center", color=ROUGE, fontsize=8)

    ax.set_title("Volume d'activité et létalité par tranche d'âge")


# =============================================================================
# GRAPHIQUE 6 — Taux d'occupation par service
# =============================================================================

def g6_occupation_services(v, dims, ax):
    """
    Taux d'occupation mensuel moyen, avec l'amplitude entre le mois le plus
    creux et le mois le plus chargé. La barre d'erreur est ici l'information
    la plus utile : elle mesure l'irrégularité de la charge, donc le besoin
    de capacité tampon.
    """
    jours = dims["dim_date"].groupby("annee_mois").size()
    occup = (v.groupby(["libelle_service", "capacite_lits", "annee_mois"])
             ["duree_sejour_j"].sum().reset_index())
    occup["taux"] = (100 * occup["duree_sejour_j"]
                     / (occup["capacite_lits"] * occup["annee_mois"].map(jours)))

    stats = (occup.groupby("libelle_service")["taux"]
             .agg(["mean", "min", "max"]).sort_values("mean"))

    y = np.arange(len(stats))
    ax.barh(y, stats["mean"], color=BLEU, alpha=0.8)
    ax.errorbar(stats["mean"], y,
                xerr=[stats["mean"] - stats["min"], stats["max"] - stats["mean"]],
                fmt="none", ecolor=GRIS, capsize=4, lw=1.2)
    ax.axvline(85, color=ROUGE, ls="--", lw=1.3)
    ax.text(85.8, 0.1, "seuil de tension (85 %)", color=ROUGE, fontsize=8,
            rotation=90, va="bottom")

    ax.set_yticks(y)
    ax.set_yticklabels(stats.index, fontsize=8)
    ax.set_xlabel("Taux d'occupation (%)")
    ax.set_title("Taux d'occupation moyen par service\n"
                 "(barres : amplitude mensuelle min-max)")


# =============================================================================
# ORCHESTRATION
# =============================================================================

def generer_dashboard(dims, faits, dossier="dashboards"):
    """Produit les six graphiques individuels puis la planche de synthèse."""
    os.makedirs(dossier, exist_ok=True)
    v = _joindre(dims, faits)

    graphiques = [
        ("01_saisonnalite_categories", lambda ax: g1_heatmap_saisonnalite(v, ax)),
        ("02_top_pathologies",         lambda ax: g2_pyramide_pathologies(v, ax)),
        ("03_evolution_mensuelle",     lambda ax: g3_evolution_mensuelle(v, ax)),
        ("04_duree_par_service",       lambda ax: g4_duree_par_service(v, ax)),
        ("05_letalite_par_age",        lambda ax: g5_letalite_par_age(v, ax)),
        ("06_occupation_services",     lambda ax: g6_occupation_services(v, dims, ax)),
    ]

    fichiers = []
    for nom, tracer in graphiques:
        fig, ax = plt.subplots(figsize=(9, 5.2))
        tracer(ax)
        fig.tight_layout()
        chemin = os.path.join(dossier, f"{nom}.png")
        fig.savefig(chemin, bbox_inches="tight")
        plt.close(fig)
        fichiers.append(chemin)
        print(f"  {chemin}")

    # Planche de synthèse pour la soutenance
    fig, axes = plt.subplots(3, 2, figsize=(18, 16))
    for (nom, tracer), ax in zip(graphiques, axes.ravel()):
        tracer(ax)
    fig.suptitle("CHU de Treichville — Tableau de bord épidémiologique 2023-2024",
                 fontsize=17, fontweight="bold", y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    chemin = os.path.join(dossier, "00_planche_synthese.png")
    fig.savefig(chemin, bbox_inches="tight")
    plt.close(fig)
    fichiers.insert(0, chemin)
    print(f"  {chemin}")

    return fichiers


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from pipeline_etl import executer_pipeline

    resultat = executer_pipeline(
        os.environ.get("DE_FICHIER_SOURCE",
                       "data/raw/admissions_chu_treichville.csv"))
    generer_dashboard(resultat["dimensions"], resultat["faits"])
