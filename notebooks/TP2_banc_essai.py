# Databricks notebook source
# MAGIC %md
# MAGIC # TP2 — Banc d'essai : formats et partitionnement
# MAGIC **UrbanMarket · DATE731**
# MAGIC
# MAGIC > « La direction veut savoir quel format et quel partitionnement utiliser pour les ventes.
# MAGIC > Répondez avec des mesures, pas avec des opinions. »
# MAGIC
# MAGIC Données de test : la table `workspace.silver.ventes` (300 000 ventes, TP1).
# MAGIC
# MAGIC | # | Expérience | Mesures |
# MAGIC |---|---|---|
# MAGIC | 1 | Formats : CSV, JSON, Parquet snappy, Parquet zstd, Delta | taille, temps de lecture |
# MAGIC | 2 | Partitionnement : aucun, par mois, par jour, par magasin et par jour | nombre de fichiers, temps de requête |
# MAGIC | 3 | Petits fichiers : 600 petits chargements, puis compaction | nombre de fichiers, temps de requête avant / après |
# MAGIC | 4 | Delta : UPDATE, MERGE, historique, schéma protégé, `_delta_log` | ce que Delta permet |
# MAGIC
# MAGIC Toutes les lectures sont faites avec le **même moteur (Spark)** et la **même requête**, et chaque temps
# MAGIC est la **médiane de 3 exécutions**, pour que la comparaison soit juste.

# COMMAND ----------

# MAGIC %pip install deltalake

# COMMAND ----------

# MAGIC %md ## 0. Préparation

# COMMAND ----------

import os
import shutil
import statistics
import time

import pandas as pd
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.banc_essai COMMENT 'TP2 : banc d essai formats et partitionnement'")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.banc_essai.fichiers")
BASE = "/Volumes/workspace/banc_essai/fichiers"
TMP = "/tmp/banc_essai"
shutil.rmtree(TMP, ignore_errors=True)
os.makedirs(TMP, exist_ok=True)

ventes = (spark.table("workspace.silver.ventes")
    .drop("_source_fichier", "_traite_le", "client_identifie")
    .withColumn("prix_unitaire", F.col("prix_unitaire").cast("double"))
    .withColumn("montant", F.col("montant").cast("double"))
    .withColumn("jour", F.to_date("date_vente"))
    .withColumn("annee_mois", F.date_format("date_vente", "yyyy-MM")))
pdf = ventes.toPandas()
pdf.attrs = {}  # retire les métadonnées Spark que Parquet ne sait pas enregistrer
pdf["date_vente"] = pdf["date_vente"].astype("datetime64[us]")  # dates en microsecondes, lisibles par Spark
print(f"{len(pdf):,} ventes chargées".replace(",", " "))

mesures = []  # toutes les mesures du banc d'essai


def chrono(action, essais=3):
    """Médiane du temps d'exécution (secondes) sur plusieurs essais."""
    temps = []
    for _ in range(essais):
        debut = time.perf_counter()
        action()
        temps.append(time.perf_counter() - debut)
    return round(statistics.median(temps), 2)


def taille_mo(chemin):
    if os.path.isfile(chemin):
        return round(os.path.getsize(chemin) / 1_048_576, 2)
    total = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(chemin) for f in fs)
    return round(total / 1_048_576, 2)


def nb_fichiers(chemin):
    return sum(1 for _, _, fs in os.walk(chemin) for f in fs
               if not f.startswith(("_", ".")) and not f.endswith(".crc"))


def ca_par_canal(df):
    """La requête de référence : chiffre d'affaires par canal (lit 2 colonnes)."""
    return df.groupBy("canal").agg(F.sum("montant")).collect()


def copier_dans_volume(source, cible):
    shutil.rmtree(cible, ignore_errors=True)
    if os.path.isdir(source):
        shutil.copytree(source, cible)
    else:
        os.makedirs(os.path.dirname(cible), exist_ok=True)
        shutil.copyfile(source, cible)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Formats : CSV, JSON, Parquet snappy, Parquet zstd, Delta
# MAGIC Mêmes 300 000 ventes, écrites dans 5 formats. On mesure la taille, puis le temps de la requête
# MAGIC « CA par canal ».

# COMMAND ----------

pdf.to_csv(f"{TMP}/ventes.csv", index=False)
pdf.to_json(f"{TMP}/ventes.json", orient="records", lines=True, date_format="iso")
pdf.to_parquet(f"{TMP}/ventes_snappy.parquet", compression="snappy", index=False)
pdf.to_parquet(f"{TMP}/ventes_zstd.parquet", compression="zstd", index=False)
for nom in ["ventes.csv", "ventes.json", "ventes_snappy.parquet", "ventes_zstd.parquet"]:
    copier_dans_volume(f"{TMP}/{nom}", f"{BASE}/formats/{nom}")

ventes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.banc_essai.ventes_delta")
taille_delta = round(spark.sql("DESCRIBE DETAIL workspace.banc_essai.ventes_delta").first()["sizeInBytes"] / 1_048_576, 2)

lecteurs = {
    "CSV": (lambda: spark.read.option("header", True).option("inferSchema", True).csv(f"{BASE}/formats/ventes.csv"),
            taille_mo(f"{BASE}/formats/ventes.csv")),
    "JSON lines": (lambda: spark.read.json(f"{BASE}/formats/ventes.json"),
                   taille_mo(f"{BASE}/formats/ventes.json")),
    "Parquet snappy": (lambda: spark.read.parquet(f"{BASE}/formats/ventes_snappy.parquet"),
                       taille_mo(f"{BASE}/formats/ventes_snappy.parquet")),
    "Parquet zstd": (lambda: spark.read.parquet(f"{BASE}/formats/ventes_zstd.parquet"),
                     taille_mo(f"{BASE}/formats/ventes_zstd.parquet")),
    "Delta": (lambda: spark.table("workspace.banc_essai.ventes_delta"), taille_delta),
}

taille_csv = lecteurs["CSV"][1]
for fmt, (lire, taille) in lecteurs.items():
    t = chrono(lambda: ca_par_canal(lire()))
    mesures.append({"experience": "1 formats", "variante": fmt, "taille_mo": taille,
                    "gain_vs_csv": f"{round(100 * (1 - taille / taille_csv))} %",
                    "nb_fichiers": 1, "temps_requete_s": t})

display(pd.DataFrame([m for m in mesures if m["experience"] == "1 formats"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Partitionnement
# MAGIC Les ventes en Parquet, rangées de 4 façons. Requête testée : **le CA de mars 2026 par magasin**
# MAGIC (le filtre typique de Sophie), puis le CA total (toute la table).

# COMMAND ----------

dispositions = {
    "aucun": [],
    "par mois (annee_mois)": ["annee_mois"],
    "par jour": ["jour"],
    "par magasin et par jour": ["id_magasin", "jour"],
}


def mars_2026(df):
    return (df.filter((F.col("annee_mois") == "2026-03")
                      & (F.col("jour") >= "2026-03-01") & (F.col("jour") < "2026-04-01"))
              .groupBy("id_magasin").agg(F.sum("montant")).collect())


def fichiers_spark(chemin):
    """Nombre de fichiers et taille (Mo), lus par Spark : bien plus rapide que de parcourir le volume."""
    m = (spark.read.parquet(chemin).select("_metadata.file_path", "_metadata.file_size").distinct()
         .agg(F.count("*").alias("n"), F.sum("file_size").alias("o")).first())
    return m["n"], round(m["o"] / 1_048_576, 2)


for nom, colonnes in dispositions.items():
    chemin = f"{BASE}/partitions/{nom.split(' (')[0].replace(' ', '_')}"
    ecriture = ventes.repartition(*colonnes) if colonnes else ventes.coalesce(1)
    t_ecriture = chrono(lambda: ecriture.write.mode("overwrite").partitionBy(*colonnes).parquet(chemin), essais=1)
    lire = lambda: spark.read.parquet(chemin)
    n, taille = fichiers_spark(chemin)
    mesures.append({
        "experience": "2 partitionnement", "variante": nom,
        "taille_mo": taille, "nb_fichiers": n,
        "temps_ecriture_s": t_ecriture,
        "temps_requete_s": chrono(lambda: mars_2026(lire())),
        "temps_ca_total_s": chrono(lambda: ca_par_canal(lire())),
    })

display(pd.DataFrame([m for m in mesures if m["experience"] == "2 partitionnement"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Petits fichiers : 600 petits chargements, puis compaction
# MAGIC On simule 600 petits chargements (par exemple un fichier toutes les quelques minutes),
# MAGIC puis on compacte. Deux versions :
# MAGIC - **Parquet** : 600 fichiers, puis réécriture en 1 seul fichier ;
# MAGIC - **Delta** : 600 fichiers, puis `OPTIMIZE`.

# COMMAND ----------

import numpy as np

# 3a — Parquet : 600 fichiers écrits un par un (600 « chargements »)
dossier_tmp = f"{TMP}/petits"
os.makedirs(dossier_tmp, exist_ok=True)
for i, morceau in enumerate(np.array_split(pdf, 600)):
    morceau.to_parquet(f"{dossier_tmp}/chargement_{i:03d}.parquet", index=False)
copier_dans_volume(dossier_tmp, f"{BASE}/petits_fichiers/avant")
avant = f"{BASE}/petits_fichiers/avant"
mesures.append({"experience": "3 petits fichiers", "variante": "Parquet : 600 petits fichiers",
                "taille_mo": taille_mo(avant), "nb_fichiers": nb_fichiers(avant),
                "temps_requete_s": chrono(lambda: ca_par_canal(spark.read.parquet(avant)))})

apres = f"{BASE}/petits_fichiers/apres"
spark.read.parquet(avant).coalesce(1).write.mode("overwrite").parquet(apres)
mesures.append({"experience": "3 petits fichiers", "variante": "Parquet : après compaction",
                "taille_mo": taille_mo(apres), "nb_fichiers": nb_fichiers(apres),
                "temps_requete_s": chrono(lambda: ca_par_canal(spark.read.parquet(apres)))})

# 3b — Delta : 600 fichiers, puis OPTIMIZE
TABLE_PF = "workspace.banc_essai.ventes_petits_fichiers"
spark.sql(f"DROP TABLE IF EXISTS {TABLE_PF}")
(ventes.limit(0).write.option("delta.autoOptimize.optimizeWrite", "false")
       .option("delta.autoOptimize.autoCompact", "false").saveAsTable(TABLE_PF))
spark.sql(f"ALTER TABLE {TABLE_PF} SET TBLPROPERTIES ("
          "'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')")
ventes.repartition(600).write.mode("append").saveAsTable(TABLE_PF)


def detail(table):
    ligne = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return ligne["numFiles"], round(ligne["sizeInBytes"] / 1_048_576, 2)


n, t = detail(TABLE_PF)
mesures.append({"experience": "3 petits fichiers", "variante": "Delta : avant OPTIMIZE", "taille_mo": t,
                "nb_fichiers": n, "temps_requete_s": chrono(lambda: ca_par_canal(spark.table(TABLE_PF)))})
spark.sql(f"OPTIMIZE {TABLE_PF}")
n, t = detail(TABLE_PF)
mesures.append({"experience": "3 petits fichiers", "variante": "Delta : après OPTIMIZE", "taille_mo": t,
                "nb_fichiers": n, "temps_requete_s": chrono(lambda: ca_par_canal(spark.table(TABLE_PF)))})

display(pd.DataFrame([m for m in mesures if m["experience"] == "3 petits fichiers"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta : ce qu'un simple dossier Parquet ne sait pas faire
# MAGIC Sur la table `workspace.banc_essai.ventes_delta` : UPDATE, MERGE, historique des versions
# MAGIC (*time travel*), schéma protégé.

# COMMAND ----------

T = "workspace.banc_essai.ventes_delta"
version_initiale = spark.sql(f"DESCRIBE HISTORY {T}").agg(F.max("version")).first()[0]
nb_initial = spark.table(T).count()

# 4a — UPDATE : hausse de 10 % du prix du produit PRD0001
r = spark.sql(f"""UPDATE {T}
                  SET prix_unitaire = round(prix_unitaire * 1.1, 2),
                      montant = round(quantite * round(prix_unitaire * 1.1, 2), 2)
                  WHERE id_produit = 'PRD0001'""").first()
print("UPDATE : lignes modifiées =", r["num_affected_rows"])

# 4b — MERGE : une vente corrigée + deux ventes nouvelles, en une seule opération
existante = spark.table(T).filter("id_vente = 'V0000001'").withColumn("quantite", F.lit(9))
nouvelles = (spark.table(T).filter("id_vente IN ('V0000002', 'V0000003')")
             .withColumn("id_vente", F.concat(F.lit("NOUV-"), "id_vente")))
existante.unionByName(nouvelles).createOrReplaceTempView("arrivage")
r = spark.sql(f"""MERGE INTO {T} AS cible USING arrivage AS src ON cible.id_vente = src.id_vente
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""").first()
print("MERGE : mises à jour =", r["num_updated_rows"], "| insertions =", r["num_inserted_rows"])

# 4c — Historique et time travel
display(spark.sql(f"DESCRIBE HISTORY {T}").select("version", "timestamp", "operation", "operationMetrics"))
nb_avant = spark.sql(f"SELECT COUNT(*) AS n FROM {T} VERSION AS OF {version_initiale}").first()["n"]
print(f"Time travel : {nb_avant} lignes à la version {version_initiale}, {spark.table(T).count()} aujourd'hui")

# 4d — Schéma protégé : une colonne inattendue est refusée
try:
    spark.table(T).limit(1).withColumn("colonne_surprise", F.lit("x")).write.mode("append").saveAsTable(T)
    print("Schéma protégé : écriture ACCEPTÉE (inattendu)")
except Exception as e:
    print("Schéma protégé : écriture REFUSÉE ->", str(e).split("\n")[0][:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4e. Le dossier `_delta_log`
# MAGIC Une table Delta = des fichiers Parquet **+ un journal de transactions** `_delta_log/`.
# MAGIC Chaque écriture ajoute un fichier JSON numéroté : c'est lui qui rend possibles l'ACID et le time travel.
# MAGIC Les tables gérées de la Free Edition cachent leur dossier ; on en crée donc une petite ici pour l'ouvrir.

# COMMAND ----------

import json

from deltalake import DeltaTable, write_deltalake

demo = f"{TMP}/demo_delta"
shutil.rmtree(demo, ignore_errors=True)
write_deltalake(demo, pdf.head(1000)[["id_vente", "canal", "quantite", "montant"]])
write_deltalake(demo, pdf.iloc[1000:1500][["id_vente", "canal", "quantite", "montant"]], mode="append")
DeltaTable(demo).delete("canal = 'WEB'")

print("Contenu de _delta_log :")
for f in sorted(os.listdir(f"{demo}/_delta_log")):
    print("  ", f)
print("\nPremière action du commit 2 (la suppression) :")
with open(f"{demo}/_delta_log/00000000000000000002.json") as f:
    for ligne in f:
        action = json.loads(ligne)
        print("  ", list(action.keys())[0], "->", json.dumps(action)[:160])

# COMMAND ----------

# MAGIC %md ## 5. Toutes les mesures

# COMMAND ----------

resultats = pd.DataFrame(mesures)
spark.createDataFrame(resultats.astype(str)).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable("workspace.banc_essai.mesures")
display(resultats)
