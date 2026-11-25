# Databricks notebook source
# MAGIC %md
# MAGIC # TP3 — Du lake au décisionnel : modèle en étoile, data marts, export Power BI
# MAGIC **UrbanMarket · DATE731**
# MAGIC
# MAGIC > « Sophie veut son tableau de bord du lundi matin. Concevez le modèle, construisez-le en gold,
# MAGIC > chargez l'entrepôt, et livrez le tableau de bord. »
# MAGIC
# MAGIC ## Étape 1 — Concevoir avant de coder
# MAGIC **Grain de la table de faits : une ligne = une vente (une ligne de ticket : un produit, une quantité, un montant).**
# MAGIC
# MAGIC | Question | Décision | Pourquoi |
# MAGIC |---|---|---|
# MAGIC | Ventes du site web, sans magasin ? | Un membre **WEB** dans `dim_magasin` (canal WEB, région « Web ») | Aucune vente perdue dans les jointures ; le web devient un « magasin » comparable |
# MAGIC | Ventes sans client ? | Un membre **ANONYME** dans `dim_client` | Sinon les jointures perdent environ 28 % des ventes et le CA est faux |
# MAGIC | Les retours sont-ils dans le CA ? | **Oui**, en négatif, avec un indicateur `est_retour` | Le CA net doit être égal à celui de la comptabilité (demande de Sophie) |
# MAGIC | Le nom du client dans le modèle ? | **Non** : seulement ville et tranche d'âge | Minimisation RGPD (Marc) : Sophie n'a pas besoin d'identifier un client |
# MAGIC
# MAGIC | Table gold | Contenu |
# MAGIC |---|---|
# MAGIC | `dim_date` | un jour par ligne : année, mois, trimestre, semaine ISO, week-end |
# MAGIC | `dim_magasin` | 20 magasins + WEB : nom, ville, région, surface, canal |
# MAGIC | `dim_produit` | 200 produits : nom, catégorie, prix catalogue |
# MAGIC | `dim_client` | 5 000 clients + ANONYME : ville, tranche d'âge, consentement |
# MAGIC | `fait_ventes` | 300 000 ventes : clés vers les 4 dimensions, quantité, montant, est_retour |
# MAGIC
# MAGIC En production, l'entrepôt serait Fabric Warehouse ou Synapse ; ici, les tables gold Delta et le SQL Warehouse
# MAGIC de Databricks jouent ce rôle (Databricks Free Edition ne peut pas joindre une base PostgreSQL externe).

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER, GOLD = "workspace.silver", "workspace.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")


def ecrire(df, table, commentaire):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{table}")
    spark.sql(f"COMMENT ON TABLE {GOLD}.{table} IS '{commentaire}'")
    print(f"✅ {GOLD}.{table} : {spark.table(f'{GOLD}.{table}').count()} lignes")

# COMMAND ----------

# MAGIC %md ## Étape 2 — Les dimensions

# COMMAND ----------

# dim_date : un jour par ligne, sur toute la période des ventes
bornes = spark.table(f"{SILVER}.ventes").agg(F.min("date_vente").alias("d"), F.max("date_vente").alias("f")).first()
dim_date = (spark.sql(f"SELECT explode(sequence(to_date('{bornes['d'].date().replace(month=1, day=1)}'), "
                      f"to_date('{bornes['f'].date().replace(month=12, day=31)}'), interval 1 day)) AS date")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("annee", F.year("date"))
    .withColumn("trimestre", F.concat(F.lit("T"), F.quarter("date")))
    .withColumn("mois", F.month("date"))
    .withColumn("nom_mois", F.element_at(F.array(*[F.lit(m) for m in [
        "janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août",
        "septembre", "octobre", "novembre", "décembre"]]), F.month("date")))
    .withColumn("annee_mois", F.date_format("date", "yyyy-MM"))
    .withColumn("semaine_iso", F.weekofyear("date"))
    .withColumn("jour_semaine", F.element_at(F.array(*[F.lit(j) for j in [
        "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]]),
        ((F.dayofweek("date") + 5) % 7) + 1))
    .withColumn("est_weekend", F.dayofweek("date").isin(1, 7))
    .select("date_key", "date", "annee", "trimestre", "mois", "nom_mois", "annee_mois",
            "semaine_iso", "jour_semaine", "est_weekend"))
ecrire(dim_date, "dim_date", "Dimension date : un jour par ligne. Propriétaire : Contrôle de gestion.")

# dim_magasin : 20 magasins + le membre WEB
magasins = spark.table(f"{SILVER}.magasins").select(
    "id_magasin", "nom", "ville", "region", "surface_m2", F.lit("MAGASIN").alias("canal"))
web = spark.createDataFrame([("WEB", "Site web UrbanMarket", "Internet", "Web", None, "WEB")],
                            "id_magasin string, nom string, ville string, region string, surface_m2 int, canal string")
dim_magasin = (magasins.unionByName(web)
    .withColumn("magasin_key", F.row_number().over(Window.orderBy("id_magasin")))
    .select("magasin_key", "id_magasin", "nom", "ville", "region", "surface_m2", "canal"))
ecrire(dim_magasin, "dim_magasin", "Dimension magasin, 20 magasins + WEB. Propriétaire : Direction commerciale.")

# dim_produit
dim_produit = (spark.table(f"{SILVER}.produits")
    .withColumn("produit_key", F.row_number().over(Window.orderBy("id_produit")))
    .select("produit_key", "id_produit", "nom", "categorie",
            F.col("prix_unitaire").cast("double").alias("prix_catalogue")))
ecrire(dim_produit, "dim_produit", "Dimension produit. Propriétaire : Direction commerciale.")

# dim_client : minimisée (ni nom, ni email, ni date de naissance) + ANONYME
age = F.floor(F.months_between(F.current_date(), "date_naissance") / 12)
dim_client = (spark.table(f"{SILVER}.clients")
    .withColumn("tranche_age",
        F.when(F.col("id_client") == "ANONYME", "Inconnue")
         .when(age < 26, "18-25").when(age < 36, "26-35").when(age < 46, "36-45")
         .when(age < 56, "46-55").when(age < 66, "56-65").otherwise("66 et plus"))
    .withColumn("ville", F.coalesce("ville", F.lit("Inconnue")))
    .withColumn("client_key", F.row_number().over(Window.orderBy("id_client")))
    .select("client_key", "id_client", "ville", "tranche_age",
            F.coalesce("consentement_marketing", F.lit(False)).alias("consentement_marketing")))
ecrire(dim_client, "dim_client", "Dimension client minimisée : ville et tranche d âge, sans donnée directement identifiante. Propriétaire : Direction marketing.")

# COMMAND ----------

# MAGIC %md ## Étape 2 (suite) — La table de faits

# COMMAND ----------

v = spark.table(f"{SILVER}.ventes")
fait_ventes = (v
    .join(dim_magasin.select("id_magasin", "magasin_key"), "id_magasin", "left")
    .join(dim_produit.select("id_produit", "produit_key"), "id_produit", "left")
    .join(dim_client.select("id_client", "client_key"), "id_client", "left")
    .withColumn("date_key", F.date_format("date_vente", "yyyyMMdd").cast("int"))
    .select("id_vente", "date_key", "magasin_key", "produit_key", "client_key", "canal",
            "date_vente", "quantite",
            F.col("prix_unitaire").cast("double").alias("prix_unitaire"),
            F.col("montant").cast("double").alias("montant"), "est_retour"))

# Intégrité : aucune clé ne doit être vide
orphelines = fait_ventes.filter("magasin_key IS NULL OR produit_key IS NULL OR client_key IS NULL OR date_key IS NULL").count()
assert orphelines == 0, f"{orphelines} ventes sans clé de dimension"
ecrire(fait_ventes, "fait_ventes", "Table de faits, grain : une vente. Propriétaire : Contrôle de gestion.")

try:
    for t in ["dim_date", "dim_magasin", "dim_produit", "dim_client"]:
        cle = {"dim_date": "date_key", "dim_magasin": "magasin_key", "dim_produit": "produit_key", "dim_client": "client_key"}[t]
        spark.sql(f"ALTER TABLE {GOLD}.{t} ALTER COLUMN {cle} SET NOT NULL")
        spark.sql(f"ALTER TABLE {GOLD}.{t} DROP CONSTRAINT IF EXISTS pk_{t}")
        spark.sql(f"ALTER TABLE {GOLD}.{t} ADD CONSTRAINT pk_{t} PRIMARY KEY ({cle})")
    print("Clés primaires posées sur les 4 dimensions (contraintes informatives Unity Catalog).")
except Exception as e:
    print("Clés primaires non posées :", str(e).split("\n")[0][:150])

# COMMAND ----------

# MAGIC %md ## Étape 3 — Les data marts (vues gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. CA mensuel par magasin, comparé au même mois de l'année précédente
# MAGIC CREATE OR REPLACE VIEW workspace.gold.v_ca_mensuel_magasin AS
# MAGIC WITH m AS (
# MAGIC   SELECT d.annee, d.mois, d.annee_mois, mg.nom AS magasin, mg.ville, mg.region, mg.canal,
# MAGIC          ROUND(SUM(f.montant), 2) AS ca
# MAGIC   FROM workspace.gold.fait_ventes f
# MAGIC   JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
# MAGIC   JOIN workspace.gold.dim_magasin mg ON f.magasin_key = mg.magasin_key
# MAGIC   GROUP BY ALL)
# MAGIC SELECT cur.*, prev.ca AS ca_n_1,
# MAGIC        ROUND(100 * (cur.ca - prev.ca) / prev.ca, 1) AS evolution_pct
# MAGIC FROM m cur LEFT JOIN m prev
# MAGIC   ON prev.magasin = cur.magasin AND prev.mois = cur.mois AND prev.annee = cur.annee - 1;
# MAGIC
# MAGIC -- 2. Top 10 des produits par mois
# MAGIC CREATE OR REPLACE VIEW workspace.gold.v_top_produits_mois AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT d.annee_mois, p.nom AS produit, p.categorie, ROUND(SUM(f.montant), 2) AS ca,
# MAGIC          RANK() OVER (PARTITION BY d.annee_mois ORDER BY SUM(f.montant) DESC) AS rang
# MAGIC   FROM workspace.gold.fait_ventes f
# MAGIC   JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
# MAGIC   JOIN workspace.gold.dim_produit p ON f.produit_key = p.produit_key
# MAGIC   GROUP BY d.annee_mois, p.nom, p.categorie)
# MAGIC WHERE rang <= 10;
# MAGIC
# MAGIC -- 3. Part de chaque canal dans le CA, par mois
# MAGIC CREATE OR REPLACE VIEW workspace.gold.v_parts_canal AS
# MAGIC SELECT d.annee_mois, f.canal, ROUND(SUM(f.montant), 2) AS ca,
# MAGIC        ROUND(100 * SUM(f.montant) / SUM(SUM(f.montant)) OVER (PARTITION BY d.annee_mois), 1) AS part_pct
# MAGIC FROM workspace.gold.fait_ventes f
# MAGIC JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.annee_mois, f.canal;

# COMMAND ----------

# MAGIC %md ## Étape 4 — Contrôles
# MAGIC Attendu par le cours : en mars 2026, le web pèse environ 20 % du CA et Nice progresse d'environ 14 % sur un an.

# COMMAND ----------

ca_silver = spark.table(f"{SILVER}.ventes").agg(F.sum("montant")).first()[0]
ca_gold = spark.table(f"{GOLD}.fait_ventes").agg(F.sum("montant")).first()[0]
print(f"Cohérence : CA silver = {float(ca_silver):,.2f} € | CA gold = {ca_gold:,.2f} € | écart = {abs(float(ca_silver) - ca_gold):.2f} €")

display(spark.sql("SELECT * FROM workspace.gold.v_parts_canal WHERE annee_mois = '2026-03'"))
display(spark.sql("""SELECT magasin, annee_mois, ca, ca_n_1, evolution_pct
                     FROM workspace.gold.v_ca_mensuel_magasin
                     WHERE annee_mois = '2026-03' AND ville = 'Nice'"""))
display(spark.sql("""SELECT ville, ROUND(SUM(ca), 2) AS ca_mars_2026, ROUND(SUM(ca_n_1), 2) AS ca_mars_2025,
                            ROUND(100 * (SUM(ca) - SUM(ca_n_1)) / SUM(ca_n_1), 1) AS evolution_pct
                     FROM workspace.gold.v_ca_mensuel_magasin WHERE annee_mois = '2026-03'
                     GROUP BY ville ORDER BY evolution_pct DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 5 — Export pour Power BI en ligne
# MAGIC Un seul fichier Excel, une feuille par table du modèle en étoile. Il se trouve dans
# MAGIC **Catalog → workspace → gold → Volumes → fichiers → export_powerbi** : le télécharger, puis l'importer dans Power BI.

# COMMAND ----------

import os
import shutil

spark.sql(f"CREATE VOLUME IF NOT EXISTS {GOLD}.fichiers")
sortie_tmp = "/tmp/urbanmarket_gold.xlsx"
with __import__("pandas").ExcelWriter(sortie_tmp, engine="openpyxl") as xl:
    for t in ["fait_ventes", "dim_date", "dim_magasin", "dim_produit", "dim_client"]:
        pdf = spark.table(f"{GOLD}.{t}").toPandas()
        pdf.attrs = {}
        for c in pdf.columns:
            if str(pdf[c].dtype).startswith("datetime64"):
                pdf[c] = pdf[c].dt.tz_localize(None) if getattr(pdf[c].dt, "tz", None) else pdf[c]
        pdf.to_excel(xl, sheet_name=t, index=False)
        print(f"Feuille {t} : {len(pdf)} lignes")
os.makedirs("/Volumes/workspace/gold/fichiers/export_powerbi", exist_ok=True)
shutil.copyfile(sortie_tmp, "/Volumes/workspace/gold/fichiers/export_powerbi/urbanmarket_gold.xlsx")
print("Export prêt : /Volumes/workspace/gold/fichiers/export_powerbi/urbanmarket_gold.xlsx",
      f"({os.path.getsize(sortie_tmp) / 1_048_576:.1f} Mo)")
