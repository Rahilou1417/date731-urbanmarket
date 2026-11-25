"""TP1 - Profil des fichiers bruts de data/source/.

Pour chaque fichier : nombre de lignes, doublons sur la clé,
puis pour chaque colonne : type, min, max, valeurs distinctes, % de nulls.
Lancer depuis le dossier du kit : python scripts/tp1_profiler.py
"""
from pathlib import Path

import pandas as pd

SOURCE = Path("data/source")

# Clé de chaque fichier (la ou les colonnes qui doivent être uniques)
CLES = {
    "magasins.csv": ["id_magasin"],
    "produits.csv": ["id_produit"],
    "clients.csv": ["id_client"],
    "ventes.csv": ["id_vente"],
    "stocks.csv": ["date_stock", "id_magasin", "id_produit"],
    "logs_web.json": None,  # pas de clé : on compte les lignes identiques
}


def lire(fichier):
    if fichier.suffix == ".json":
        return pd.read_json(fichier, lines=True)
    return pd.read_csv(fichier)


def profiler(nom, cle):
    df = lire(SOURCE / nom)
    print("=" * 70)
    print(f"{nom} : {len(df):,} lignes".replace(",", " "))
    if cle:
        doublons = df.duplicated(subset=cle).sum()
        print(f"Doublons sur la clé {cle} : {doublons}")
    print(f"Lignes entièrement identiques : {df.duplicated().sum()}")

    lignes = []
    for col in df.columns:
        s = df[col]
        try:
            mini, maxi = s.min(), s.max()
        except TypeError:  # colonne avec des types mélangés
            mini, maxi = "?", "?"
        lignes.append({
            "colonne": col,
            "type": str(s.dtype),
            "min": mini,
            "max": maxi,
            "distinctes": s.nunique(),
            "% nulls": round(s.isna().mean() * 100, 2),
        })
    with pd.option_context("display.width", 200, "display.max_colwidth", 30):
        print(pd.DataFrame(lignes).to_string(index=False))


if __name__ == "__main__":
    if not SOURCE.exists() or not any(SOURCE.glob("*.csv")):
        print("Aucune donnée : lancez d'abord python scripts/generate_data.py")
    else:
        for nom, cle in CLES.items():
            if (SOURCE / nom).exists():
                profiler(nom, cle)
