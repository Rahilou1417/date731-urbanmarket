# Databricks notebook source
# MAGIC %md
# MAGIC # TP1 · 01 — Créer les zones et ingérer en bronze
# MAGIC **UrbanMarket · DATE731**
# MAGIC
# MAGIC Ce notebook :
# MAGIC 1. crée les 3 zones du lake : les schémas `bronze`, `silver`, `gold` ;
# MAGIC 2. génère les 6 fichiers sources d'UrbanMarket (même script et même graine 731 que le kit du prof) ;
# MAGIC 3. les dépose **sans modification** dans la zone bronze, au chemin conventionnel :
# MAGIC    `<source>/<entité>/ingestion_date=AAAA-MM-JJ/<entité>_<horodatage>.<ext>` ;
# MAGIC 4. écrit un **manifeste** (lignes + empreinte md5 de chaque fichier).
# MAGIC
# MAGIC > En production : stockage ADLS Gen2 + ingestion Azure Data Factory.
# MAGIC > Ici : Databricks Free Edition, qui bloque l'accès à un compte Azure externe.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md ## 1. Créer les trois zones (avec leur règle écrite)

# COMMAND ----------

CATALOGUE = "workspace"

regles = {
    "bronze": "Copie exacte des sources. Règle : on ne modifie jamais. Écrit par : l'ingestion.",
    "silver": "Nettoyé, typé, dédoublonné. Règle : chaque contrôle est écrit. Écrit par : les transformations.",
    "gold": "Prêt pour un usage métier. Règle : chaque jeu a un propriétaire. Lu par : BI, métiers, partenaires.",
}

for zone, regle in regles.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGUE}.{zone} COMMENT '{regle.replace(chr(39), chr(92) + chr(39))}'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{zone}.fichiers")
    with open(f"/Volumes/{CATALOGUE}/{zone}/fichiers/_LISEZMOI.txt", "w") as f:
        f.write(f"Zone {zone.upper()}\n{regle}\n")
    print(f"Zone prête : {zone}")

# COMMAND ----------

# MAGIC %md ## 2. Générer les données sources (script du kit, graine 731)

# COMMAND ----------

import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEED = 731
rng = np.random.default_rng(SEED)
random.seed(SEED)
fake = Faker("fr_FR")
Faker.seed(SEED)

OUT = Path("/tmp/urbanmarket_source")
OUT.mkdir(parents=True, exist_ok=True)
DEBUT, FIN = pd.Timestamp("2025-01-01"), pd.Timestamp("2026-09-30")

regions = {
    "Paris": "Île-de-France", "Lyon": "Auvergne-Rhône-Alpes",
    "Marseille": "PACA", "Nice": "PACA", "Lille": "Hauts-de-France",
    "Bordeaux": "Nouvelle-Aquitaine", "Nantes": "Pays de la Loire",
    "Toulouse": "Occitanie", "Strasbourg": "Grand Est", "Rennes": "Bretagne",
}
villes = list(regions)
magasins = pd.DataFrame({
    "id_magasin": [f"MAG{i:03d}" for i in range(1, 21)],
    "ville": [villes[i % 10] for i in range(20)],
})
magasins["nom"] = "UrbanMarket " + magasins["ville"] + " " + (magasins.index // 10 + 1).astype(str)
magasins["region"] = magasins["ville"].map(regions)
magasins["surface_m2"] = rng.integers(400, 2500, 20)
magasins.to_csv(OUT / "magasins.csv", index=False)

categories = {"Épicerie": (1, 15), "Boissons": (1, 25), "Frais": (2, 20),
              "Hygiène": (2, 18), "Maison": (5, 80), "High-tech": (15, 400)}
produits = []
for i in range(1, 201):
    cat = random.choice(list(categories))
    lo, hi = categories[cat]
    produits.append({
        "id_produit": f"PRD{i:04d}",
        "nom": f"{fake.word().capitalize()} {cat.lower()}",
        "categorie": cat,
        "prix_unitaire": round(random.uniform(lo, hi), 2),
    })
produits = pd.DataFrame(produits)
produits.to_csv(OUT / "produits.csv", index=False)

clients = pd.DataFrame([{
    "id_client": f"CLI{i:05d}",
    "prenom": fake.first_name(),
    "nom": fake.last_name(),
    "email": fake.email(),
    "ville": random.choice(villes),
    "date_naissance": fake.date_of_birth(minimum_age=18, maximum_age=80),
    "date_inscription": fake.date_between(DEBUT.date(), FIN.date()),
    "consentement_marketing": random.random() < 0.6,
} for i in range(1, 5001)])
clients.loc[clients.sample(frac=0.03, random_state=SEED).index, "email"] = None
clients.to_csv(OUT / "clients.csv", index=False)

N = 300_000
jours = rng.integers(0, (FIN - DEBUT).days + 1, N)
secondes = rng.integers(8 * 3600, 21 * 3600, N)
dates = DEBUT + pd.to_timedelta(jours, unit="D") + pd.to_timedelta(secondes, unit="s")
canal = rng.choice(["MAGASIN", "WEB"], N, p=[0.8, 0.2])
idx_prod = rng.integers(0, len(produits), N)
anonyme = (canal == "MAGASIN") & (rng.random(N) < 0.35)

ventes = pd.DataFrame({
    "id_vente": [f"V{i:07d}" for i in range(1, N + 1)],
    "date_vente": dates,
    "canal": canal,
    "id_magasin": np.where(canal == "MAGASIN", rng.choice(magasins["id_magasin"], N), "WEB"),
    "id_client": np.where(anonyme, None, rng.choice(clients["id_client"], N)),
    "id_produit": produits["id_produit"].values[idx_prod],
    "quantite": rng.integers(1, 6, N),
    "prix_unitaire": produits["prix_unitaire"].values[idx_prod],
})
retours = rng.random(N) < 0.01
ventes.loc[retours, "quantite"] *= -1
ventes["montant"] = (ventes["quantite"] * ventes["prix_unitaire"]).round(2)
doublons = ventes.sample(frac=0.005, random_state=SEED)
ventes = pd.concat([ventes, doublons]).sort_values("date_vente")
ventes.to_csv(OUT / "ventes.csv", index=False)

mois = pd.date_range(DEBUT, FIN, freq="ME")
stocks = pd.MultiIndex.from_product(
    [mois.date, magasins["id_magasin"], produits["id_produit"]],
    names=["date_stock", "id_magasin", "id_produit"],
).to_frame(index=False)
stocks["quantite_stock"] = rng.integers(0, 300, len(stocks))
stocks.to_csv(OUT / "stocks.csv", index=False)

M = 100_000
duree = int((FIN - DEBUT).total_seconds())
logs = pd.DataFrame({
    "event_time": DEBUT + pd.to_timedelta(rng.integers(0, duree, M), unit="s"),
    "session_id": [f"S{x:08d}" for x in rng.integers(0, 40_000, M)],
    "id_client": np.where(rng.random(M) < 0.5, None, rng.choice(clients["id_client"], M)),
    "page": rng.choice(["/", "/promo", "/produit", "/panier", "/compte"], M),
    "action": rng.choice(["view", "add_to_cart", "purchase"], M, p=[0.75, 0.18, 0.07]),
    "device": rng.choice(["mobile", "desktop", "tablet"], M, p=[0.6, 0.32, 0.08]),
})
logs.sort_values("event_time").to_json(
    OUT / "logs_web.json", orient="records", lines=True, date_format="iso")

for f in sorted(OUT.iterdir()):
    print(f"{f.name:<15} {f.stat().st_size / 1_048_576:6.1f} Mo")
print("Données UrbanMarket générées.")

# COMMAND ----------

# MAGIC %md ## 3. Ingérer en bronze, sans modification, + manifeste

# COMMAND ----------

import hashlib
import shutil
from datetime import datetime

BRONZE = Path(f"/Volumes/{CATALOGUE}/bronze/fichiers")
maintenant = datetime.now()
jour = maintenant.strftime("%Y-%m-%d")
horodatage = maintenant.strftime("%Y%m%dT%H%M%S")

# fichier source -> (système source, entité), d'après la diapo « Six sources »
RANGEMENT = {
    "magasins.csv": ("referentiel", "magasins"),
    "produits.csv": ("erp", "produits"),
    "clients.csv": ("crm", "clients"),
    "ventes.csv": ("caisse", "ventes"),
    "stocks.csv": ("erp", "stocks"),
    "logs_web.json": ("site_web", "logs_web"),
}

manifeste = []
for nom, (source, entite) in RANGEMENT.items():
    fichier = OUT / nom
    extension = fichier.suffix
    dossier = BRONZE / source / entite / f"ingestion_date={jour}"
    dossier.mkdir(parents=True, exist_ok=True)
    cible = dossier / f"{entite}_{horodatage}{extension}"
    shutil.copyfile(fichier, cible)

    contenu = fichier.read_bytes()
    lignes = contenu.count(b"\n") - (1 if extension == ".csv" else 0)  # sans l'en-tête
    manifeste.append({
        "fichier": nom,
        "source": source,
        "chemin_bronze": str(cible),
        "lignes": lignes,
        "octets": len(contenu),
        "md5": hashlib.md5(contenu).hexdigest(),
        "ingere_le": maintenant.isoformat(timespec="seconds"),
    })
    print(f"Ingéré : {nom:<15} -> {cible}")

dossier_manifeste = BRONZE / "_manifestes"
dossier_manifeste.mkdir(parents=True, exist_ok=True)
pd.DataFrame(manifeste).to_csv(dossier_manifeste / f"manifeste_{horodatage}.csv", index=False)
display(pd.DataFrame(manifeste))
