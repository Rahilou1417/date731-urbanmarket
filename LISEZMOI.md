# DATE731 – Kit des travaux pratiques UrbanMarket (version étudiants)

## Démarrer
1. `python -m venv .venv` puis activer l'environnement
   (Windows : `.venv\Scripts\activate` · Mac/Linux : `source .venv/bin/activate`)
2. `pip install -r requirements.txt`
3. Copier `.env.exemple` en `.env` et le compléter (ne jamais partager ce fichier)
4. `python scripts/check_env.py`
5. `python scripts/generate_data.py`

Toujours lancer les scripts depuis ce dossier.

## Les fichiers à compléter
| Fichier | TP | Ce que vous écrivez |
| --- | --- | --- |
| scripts/tp1_bronze_vers_silver.py | 1 | vos règles de passage bronze → silver |
| scripts/tp3_modele_etoile.py | 3 | les dimensions et la table de faits |
| sql/data_marts.sql | 3 | deux data marts |
| scripts/tp4_pseudonymiser.py | 4 | la pseudonymisation des clients |
| scripts/tp4_droit_effacement.py | 4 | le droit à l'effacement (défi) |
| catalogue.yaml | 5 | les jeux de données manquants |
| scripts/tp5_controles_qualite.py | 5 | au moins trois règles de qualité en plus |

Les autres scripts sont des outils fournis : vous les lancez et vous analysez leurs résultats.
