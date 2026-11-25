# Databricks notebook source
# MAGIC %md
# MAGIC # TP1 · 02 — Profil des fichiers bronze
# MAGIC Pour chaque fichier de la dernière ingestion : nombre de lignes, doublons sur la clé,
# MAGIC puis pour chaque colonne : type, min, max, valeurs distinctes, % de nulls.
# MAGIC
# MAGIC Ensuite, pour chaque anomalie : **erreur technique ou réalité métier ?**
# MAGIC Puis **décider** : corriger, mettre en quarantaine, ou garder et signaler.

# COMMAND ----------

from pathlib import Path

import pandas as pd

BRONZE = Path("/Volumes/workspace/bronze/fichiers")

# entité -> (dossier source, clé qui doit être unique)
FICHIERS = {
    "magasins": ("referentiel", ["id_magasin"]),
    "produits": ("erp", ["id_produit"]),
    "clients": ("crm", ["id_client"]),
    "ventes": ("caisse", ["id_vente"]),
    "stocks": ("erp", ["date_stock", "id_magasin", "id_produit"]),
    "logs_web": ("site_web", None),  # pas de clé : on compte les lignes identiques
}


def dernier_fichier(source, entite):
    """Renvoie le fichier de la dernière ingestion (le plus récent)."""
    fichiers = sorted((BRONZE / source / entite).glob("ingestion_date=*/*"))
    return fichiers[-1]


def lire(fichier):
    if fichier.suffix == ".json":
        return pd.read_json(fichier, lines=True)
    return pd.read_csv(fichier)


def profiler(entite, source, cle):
    fichier = dernier_fichier(source, entite)
    df = lire(fichier)
    print("=" * 70)
    print(f"{entite} : {len(df):,} lignes   ({fichier.name})".replace(",", " "))
    if cle:
        print(f"Doublons sur la clé {cle} : {df.duplicated(subset=cle).sum()}")
    print(f"Lignes entièrement identiques : {df.duplicated().sum()}")

    lignes = []
    for col in df.columns:
        s = df[col]
        try:
            mini, maxi = s.min(), s.max()
        except TypeError:
            mini, maxi = "?", "?"
        lignes.append({
            "colonne": col,
            "type": str(s.dtype),
            "min": mini,
            "max": maxi,
            "distinctes": s.nunique(),
            "% nulls": round(s.isna().mean() * 100, 2),
        })
    display(pd.DataFrame(lignes))

# COMMAND ----------

for entite, (source, cle) in FICHIERS.items():
    profiler(entite, source, cle)

# COMMAND ----------

# MAGIC %md ## Contrôles complémentaires (anomalies trouvées)

# COMMAND ----------

ventes = lire(dernier_fichier("caisse", "ventes"))
clients = lire(dernier_fichier("crm", "clients"))

print("Ventes avec quantité négative (retours) :", (ventes["quantite"] < 0).sum())
print("Ventes magasin sans client (anonymes)    :", ((ventes["canal"] == "MAGASIN") & ventes["id_client"].isna()).sum())
print("Ventes web sans client                   :", ((ventes["canal"] == "WEB") & ventes["id_client"].isna()).sum())
print("Emails clients en double                 :", clients["email"].dropna().duplicated().sum())
print("Ventes vers un produit inconnu           :", (~ventes["id_produit"].isin(lire(dernier_fichier("erp", "produits"))["id_produit"])).sum())
print("Ventes vers un client inconnu            :", (ventes["id_client"].notna() & ~ventes["id_client"].isin(clients["id_client"])).sum())
