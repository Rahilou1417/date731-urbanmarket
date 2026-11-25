# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — Export des tables silver pour Power BI en ligne
# MAGIC **UrbanMarket · DATE731**
# MAGIC
# MAGIC Choix du groupe : le **modèle en étoile est construit dans Power BI** (relations, tables calculées DAX,
# MAGIC mesures). Databricks fournit seulement les tables silver propres du TP1, dans un fichier Excel
# MAGIC (une feuille par table), à importer dans Power BI en ligne.
# MAGIC
# MAGIC Minimisation RGPD : le prénom, le nom et l'email des clients **ne sont pas exportés** ; Power BI n'en a pas besoin.
# MAGIC
# MAGIC Le fichier se trouve dans **Catalog → workspace → silver → Volumes → fichiers → export_powerbi**.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import os
import shutil

import pandas as pd

TABLES = {
    "ventes": ["id_vente", "date_vente", "canal", "id_magasin", "id_client", "id_produit",
               "quantite", "prix_unitaire", "montant", "est_retour"],
    "magasins": ["id_magasin", "nom", "ville", "region", "surface_m2"],
    "produits": ["id_produit", "nom", "categorie", "prix_unitaire"],
    "clients": ["id_client", "ville", "date_naissance", "date_inscription", "consentement_marketing"],
}

tmp = "/tmp/urbanmarket_silver_powerbi.xlsx"
with pd.ExcelWriter(tmp, engine="openpyxl") as xl:
    for table, colonnes in TABLES.items():
        pdf = spark.table(f"workspace.silver.{table}").select(*colonnes).toPandas()
        pdf.attrs = {}
        for c in ["prix_unitaire", "montant"]:
            if c in pdf.columns:
                pdf[c] = pdf[c].astype(float)
        pdf.to_excel(xl, sheet_name=table, index=False)
        print(f"Feuille {table} : {len(pdf)} lignes, colonnes : {', '.join(colonnes)}")

cible = "/Volumes/workspace/silver/fichiers/export_powerbi"
os.makedirs(cible, exist_ok=True)
shutil.copyfile(tmp, f"{cible}/urbanmarket_silver_powerbi.xlsx")
print(f"\nExport prêt : {cible}/urbanmarket_silver_powerbi.xlsx ({os.path.getsize(tmp) / 1_048_576:.1f} Mo)")
