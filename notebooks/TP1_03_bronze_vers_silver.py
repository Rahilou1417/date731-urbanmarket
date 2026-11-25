# Databricks notebook source
# MAGIC %md
# MAGIC # TP1 · 03 — Passage bronze → silver
# MAGIC **UrbanMarket · DATE731**
# MAGIC
# MAGIC Règle de la zone silver : *nettoyé, typé, dédoublonné — **chaque contrôle est écrit***.
# MAGIC
# MAGIC Ce notebook lit la **dernière ingestion bronze** (jamais modifiée), applique les règles de passage
# MAGIC décidées après le profil (notebook 02), et écrit dans `workspace.silver` :
# MAGIC
# MAGIC | Table Delta | Contenu |
# MAGIC |---|---|
# MAGIC | `ventes`, `clients`, `produits`, `magasins`, `stocks`, `logs_web` | données propres et typées |
# MAGIC | `quarantaine` | lignes rejetées, avec la règle qui les a rejetées |
# MAGIC | `journal_controles` | chaque règle, le nombre de lignes concernées et la décision |
# MAGIC
# MAGIC Chaque ligne silver garde sa **traçabilité** : `_source_fichier`, `_traite_le`.

# COMMAND ----------

# MAGIC %md ## 0. Outils : lire la dernière ingestion, tenir le journal

# COMMAND ----------

from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

BRONZE = Path("/Volumes/workspace/bronze/fichiers")
SILVER = "workspace.silver"
TRAITE_LE = datetime.now().isoformat(timespec="seconds")

journal = []      # une ligne par règle appliquée
quarantaines = [] # DataFrames des lignes rejetées


def dernier_fichier(source, entite):
    """Chemin du fichier de la dernière ingestion bronze."""
    return str(sorted((BRONZE / source / entite).glob("ingestion_date=*/*"))[-1])


def lire_csv(source, entite):
    chemin = dernier_fichier(source, entite)
    df = spark.read.option("header", True).option("inferSchema", False).csv(chemin)
    return df.withColumn("_source_fichier", F.lit(chemin)), chemin


def noter(table, regle, type_controle, lignes, decision):
    """Écrit une règle dans le journal des contrôles."""
    journal.append({
        "table": table, "regle": regle, "type_controle": type_controle,
        "lignes_concernees": int(lignes), "decision": decision, "traite_le": TRAITE_LE,
    })
    print(f"[{table}] {regle} : {lignes} ligne(s) -> {decision}")


def mettre_en_quarantaine(df, table, regle):
    quarantaines.append(
        df.select(F.lit(table).alias("table"), F.lit(regle).alias("regle"),
                  F.to_json(F.struct(*df.columns)).alias("ligne"),
                  F.lit(TRAITE_LE).alias("traite_le"))
    )


def ecrire(df, table):
    (df.withColumn("_traite_le", F.lit(TRAITE_LE).cast("timestamp"))
       .write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{table}"))
    print(f"✅ {SILVER}.{table} : {spark.table(f'{SILVER}.{table}').count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ventes
# MAGIC | Règle | Anomalie trouvée au profil | Erreur ou métier ? | Décision |
# MAGIC |---|---|---|---|
# MAGIC | V1 | lignes entièrement identiques (même vente reçue 2 fois) | erreur technique | **corriger** : dédoublonner |
# MAGIC | V2 | même `id_vente` mais contenu différent | erreur technique | **quarantaine** |
# MAGIC | V3 | dates et nombres lus comme du texte | format | **corriger** : typer |
# MAGIC | V4 | quantité négative | **réalité métier** : retour | **garder et signaler** : `est_retour` |
# MAGIC | V5 | `id_client` vide en magasin | **réalité métier** : client sans carte | **garder** : `ANONYME` |
# MAGIC | V6 | produit ou client inconnu | intégrité | **quarantaine** |
# MAGIC | V7 | `montant` ≠ `quantite × prix_unitaire` | validité | **quarantaine** |

# COMMAND ----------

ventes, chemin = lire_csv("caisse", "ventes")
total = ventes.count()

# V1 — doublons exacts : on garde une seule copie
colonnes_metier = [c for c in ventes.columns if c != "_source_fichier"]
ventes_v1 = ventes.dropDuplicates(colonnes_metier)
noter("ventes", "V1 doublons exacts supprimés", "unicité", total - ventes_v1.count(), "corrigé")

# V2 — même identifiant, contenu différent : on ne sait pas laquelle est juste
conflits = ventes_v1.groupBy("id_vente").count().filter("count > 1").select("id_vente")
ventes_conflit = ventes_v1.join(conflits, "id_vente", "inner")
mettre_en_quarantaine(ventes_conflit, "ventes", "V2 id_vente en conflit")
noter("ventes", "V2 id_vente en conflit", "unicité", ventes_conflit.count(), "quarantaine")
ventes_v2 = ventes_v1.join(conflits, "id_vente", "left_anti")

# V3 — typage
ventes_v3 = (ventes_v2
    .withColumn("date_vente", F.to_timestamp("date_vente"))
    .withColumn("quantite", F.col("quantite").cast("int"))
    .withColumn("prix_unitaire", F.col("prix_unitaire").cast("decimal(10,2)"))
    .withColumn("montant", F.col("montant").cast("decimal(12,2)")))
noter("ventes", "V3 typage (date, entiers, montants)", "validité", 0, "corrigé")

# V4 — retours : on garde, on signale
ventes_v4 = ventes_v3.withColumn("est_retour", F.col("quantite") < 0)
noter("ventes", "V4 retours signalés (quantité négative)", "métier",
      ventes_v4.filter("est_retour").count(), "gardé et signalé")

# V5 — ventes sans client : membre inconnu ANONYME (sinon les jointures perdent du CA)
ventes_v5 = (ventes_v4
    .withColumn("client_identifie", F.col("id_client").isNotNull())
    .withColumn("id_client", F.coalesce("id_client", F.lit("ANONYME"))))
noter("ventes", "V5 client absent -> ANONYME", "complétude",
      ventes_v5.filter(~F.col("client_identifie")).count(), "gardé (ANONYME)")

# V6 — intégrité : le produit et le client doivent exister
produits_ids = spark.read.option("header", True).csv(dernier_fichier("erp", "produits")).select("id_produit")
clients_ids = spark.read.option("header", True).csv(dernier_fichier("crm", "clients")).select("id_client")
orphelins = (ventes_v5.join(produits_ids, "id_produit", "left_anti")
    .unionByName(ventes_v5.filter("id_client <> 'ANONYME'").join(clients_ids, "id_client", "left_anti"))
    .dropDuplicates(["id_vente"]))
mettre_en_quarantaine(orphelins, "ventes", "V6 produit ou client inconnu")
noter("ventes", "V6 produit ou client inconnu", "intégrité", orphelins.count(), "quarantaine")
ventes_v6 = ventes_v5.join(orphelins.select("id_vente"), "id_vente", "left_anti")

# V7 — cohérence du montant
incoherents = ventes_v6.filter(F.abs(F.col("montant") - F.col("quantite") * F.col("prix_unitaire")) > 0.01)
mettre_en_quarantaine(incoherents, "ventes", "V7 montant incohérent")
noter("ventes", "V7 montant ≠ quantité × prix", "validité", incoherents.count(), "quarantaine")
ventes_silver = ventes_v6.join(incoherents.select("id_vente"), "id_vente", "left_anti")

ecrire(ventes_silver, "ventes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clients (données personnelles : accès restreint)
# MAGIC | Règle | Anomalie | Erreur ou métier ? | Décision |
# MAGIC |---|---|---|---|
# MAGIC | C1 | dates lues comme du texte | format | **corriger** : typer |
# MAGIC | C2 | email vide (≈ 3 %) | donnée manquante, le client existe | **garder et signaler** : `email_manquant` |
# MAGIC | C3 | même email pour 2 clients différents | à vérifier par le métier | **garder et signaler** : `email_en_double` |
# MAGIC | C4 | client mineur ou né dans le futur | validité | **quarantaine** |
# MAGIC | C5 | membre inconnu pour les ventes anonymes | modélisation | **ajouter** la ligne `ANONYME` |

# COMMAND ----------

clients, _ = lire_csv("crm", "clients")

# C1 — typage
clients_c1 = (clients
    .withColumn("date_naissance", F.to_date("date_naissance"))
    .withColumn("date_inscription", F.to_date("date_inscription"))
    .withColumn("consentement_marketing", F.col("consentement_marketing").cast("boolean")))
noter("clients", "C1 typage des dates et du consentement", "validité", 0, "corrigé")

# C2 — email manquant
clients_c2 = clients_c1.withColumn("email_manquant", F.col("email").isNull())
noter("clients", "C2 email manquant", "complétude",
      clients_c2.filter("email_manquant").count(), "gardé et signalé")

# C3 — email partagé par plusieurs clients
emails_doubles = (clients_c2.filter(F.col("email").isNotNull())
    .groupBy("email").count().filter("count > 1")
    .select("email", F.lit(True).alias("email_en_double")))
clients_c3 = (clients_c2.join(emails_doubles, "email", "left")
    .fillna({"email_en_double": False}))
noter("clients", "C3 email partagé par plusieurs clients", "unicité",
      clients_c3.filter("email_en_double").count(), "gardé et signalé")

# C4 — âge plausible (majeur, pas né dans le futur)
invalides = clients_c3.filter(
    (F.col("date_naissance") > F.add_months(F.current_date(), -12 * 18)) | F.col("date_naissance").isNull())
mettre_en_quarantaine(invalides, "clients", "C4 date de naissance invalide")
noter("clients", "C4 date de naissance invalide", "validité", invalides.count(), "quarantaine")
clients_c4 = clients_c3.join(invalides.select("id_client"), "id_client", "left_anti")

# C5 — le membre ANONYME
anonyme = spark.createDataFrame(
    [("ANONYME", "Client", "anonyme", None, None, None, None, False, None, True, False)],
    "id_client string, prenom string, nom string, email string, ville string, date_naissance date, "
    "date_inscription date, consentement_marketing boolean, _source_fichier string, "
    "email_manquant boolean, email_en_double boolean")
clients_silver = clients_c4.unionByName(anonyme)
noter("clients", "C5 ajout du membre ANONYME", "modélisation", 1, "ajouté")

ecrire(clients_silver, "clients")
spark.sql(f"COMMENT ON TABLE {SILVER}.clients IS 'Données personnelles (RGPD). Accès restreint. Propriétaire : Direction marketing.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Référentiels et stocks
# MAGIC Aucune anomalie au profil : on **type** seulement, et on vérifie les règles de validité.
# MAGIC | Règle | Contrôle | Décision si échec |
# MAGIC |---|---|---|
# MAGIC | P1 | prix produit > 0 | quarantaine |
# MAGIC | S1 | quantité en stock ≥ 0 | quarantaine |
# MAGIC | S2 | stock vers un magasin ou produit inconnu | quarantaine |

# COMMAND ----------

magasins, _ = lire_csv("referentiel", "magasins")
magasins_silver = magasins.withColumn("surface_m2", F.col("surface_m2").cast("int"))
noter("magasins", "M1 typage", "validité", 0, "corrigé")
ecrire(magasins_silver, "magasins")

produits, _ = lire_csv("erp", "produits")
produits_t = produits.withColumn("prix_unitaire", F.col("prix_unitaire").cast("decimal(10,2)"))
prix_ko = produits_t.filter("prix_unitaire IS NULL OR prix_unitaire <= 0")
mettre_en_quarantaine(prix_ko, "produits", "P1 prix invalide")
noter("produits", "P1 prix invalide", "validité", prix_ko.count(), "quarantaine")
ecrire(produits_t.join(prix_ko.select("id_produit"), "id_produit", "left_anti"), "produits")

stocks, _ = lire_csv("erp", "stocks")
stocks_t = (stocks.withColumn("date_stock", F.to_date("date_stock"))
                  .withColumn("quantite_stock", F.col("quantite_stock").cast("int")))
stock_ko = (stocks_t.filter("quantite_stock IS NULL OR quantite_stock < 0")
    .unionByName(stocks_t.join(magasins.select("id_magasin"), "id_magasin", "left_anti"))
    .unionByName(stocks_t.join(produits.select("id_produit"), "id_produit", "left_anti"))
    .dropDuplicates(["date_stock", "id_magasin", "id_produit"]))
mettre_en_quarantaine(stock_ko, "stocks", "S1/S2 stock invalide ou orphelin")
noter("stocks", "S1/S2 stock invalide ou orphelin", "validité / intégrité", stock_ko.count(), "quarantaine")
ecrire(stocks_t.join(stock_ko.select("date_stock", "id_magasin", "id_produit"),
                     ["date_stock", "id_magasin", "id_produit"], "left_anti"), "stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Logs web
# MAGIC | Règle | Anomalie | Erreur ou métier ? | Décision |
# MAGIC |---|---|---|---|
# MAGIC | L1 | `id_client` vide (≈ 50 %) | **réalité métier** : visiteur non connecté | **garder et signaler** : `visiteur_connecte` |
# MAGIC | L2 | lignes entièrement identiques | erreur technique | **corriger** : dédoublonner |

# COMMAND ----------

chemin_logs = dernier_fichier("site_web", "logs_web")
logs = spark.read.json(chemin_logs).withColumn("_source_fichier", F.lit(chemin_logs))
total_logs = logs.count()
logs_l2 = logs.dropDuplicates([c for c in logs.columns if c != "_source_fichier"])
noter("logs_web", "L2 doublons exacts supprimés", "unicité", total_logs - logs_l2.count(), "corrigé")
logs_silver = (logs_l2.withColumn("event_time", F.to_timestamp("event_time"))
                      .withColumn("visiteur_connecte", F.col("id_client").isNotNull()))
noter("logs_web", "L1 visiteur non connecté", "complétude",
      logs_silver.filter(~F.col("visiteur_connecte")).count(), "gardé et signalé")
ecrire(logs_silver, "logs_web")

# COMMAND ----------

# MAGIC %md ## 5. Quarantaine et journal des contrôles

# COMMAND ----------

schema_q = "table string, regle string, ligne string, traite_le string"
quarantaine = spark.createDataFrame([], schema_q)
for q in quarantaines:
    quarantaine = quarantaine.unionByName(q)
ecrire(quarantaine, "quarantaine")

journal_df = spark.createDataFrame(journal)
ecrire(journal_df, "journal_controles")
display(journal_df.select("table", "regle", "type_controle", "lignes_concernees", "decision"))

# COMMAND ----------

# MAGIC %md ## 6. Vérification : ce qui est maintenant dans la zone silver

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contrôle de cohérence : le CA silver, retours compris, par canal
# MAGIC SELECT canal,
# MAGIC        COUNT(*)                                  AS nb_ventes,
# MAGIC        SUM(CASE WHEN est_retour THEN 1 ELSE 0 END) AS nb_retours,
# MAGIC        ROUND(SUM(montant), 2)                    AS chiffre_affaires
# MAGIC FROM workspace.silver.ventes
# MAGIC GROUP BY canal
