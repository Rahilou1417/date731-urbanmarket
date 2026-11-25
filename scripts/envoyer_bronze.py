"""Envoie les fichiers bruts de data/source/ dans la zone BRONZE du data lake Azure.

Lancer depuis le dossier du kit : python scripts/envoyer_bronze.py
"""
import os
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# 1. Lire les informations secrètes dans le fichier .env
load_dotenv()
COMPTE = os.getenv("AZURE_STORAGE_ACCOUNT")
CLE = os.getenv("AZURE_STORAGE_KEY")

# 2. Se connecter au compte de stockage Azure
service = BlobServiceClient(
    account_url=f"https://{COMPTE}.blob.core.windows.net",
    credential=CLE,
)

# 3. Créer les 3 zones du data lake (une seule fois)
for zone in ["bronze", "silver", "gold"]:
    try:
        service.create_container(zone)
        print(f"Zone créée : {zone}")
    except ResourceExistsError:
        print(f"Zone déjà existante : {zone}")

# 4. Où ranger chaque fichier dans la zone bronze (chemins du catalogue.yaml)
RANGEMENT = {
    "ventes.csv": "caisse/ventes/ventes.csv",
    "clients.csv": "crm/clients/clients.csv",
    "magasins.csv": "referentiel/magasins/magasins.csv",
    "produits.csv": "referentiel/produits/produits.csv",
    "stocks.csv": "logistique/stocks/stocks.csv",
    "logs_web.json": "web/logs/logs_web.json",
}

# 5. Envoyer chaque fichier tel quel (bronze = données brutes, sans modification)
bronze = service.get_container_client("bronze")
for nom, chemin in RANGEMENT.items():
    fichier = Path("data/source") / nom
    with open(fichier, "rb") as f:
        bronze.upload_blob(name=chemin, data=f, overwrite=True)
    print(f"Envoyé : {nom:<15} -> bronze/{chemin}")

print("Zone bronze remplie.")
