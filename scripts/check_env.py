"""Vérifie que l'environnement Python du module est complet."""
import importlib
import sys

MODULES = ["pandas", "pyarrow", "duckdb", "deltalake", "faker", "boto3", "azure.storage.blob",
           "azure.identity", "dotenv", "sqlalchemy", "psycopg", "yaml"]
print("Python", sys.version.split()[0])
manquants = []
for nom in MODULES:
    try:
        m = importlib.import_module(nom)
        print(f"  OK   {nom:<20} {getattr(m, '__version__', '')}")
    except ImportError:
        manquants.append(nom)
        print(f"  MANQUANT {nom}")
print("Environnement OK" if not manquants else "Installez : pip install -r requirements.txt")
