import duckdb
import os

db_path = "/app/data/database.duckdb"

print("Création de la base de données DuckDB...")

# Créer le répertoire si nécessaire
os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Connexion (créé le fichier s'il n'existe pas)
conn = duckdb.connect(db_path)

# Vérifier la connexion
version = conn.execute("SELECT version()").fetchone()[0]
print(f"Base de données créée avec DuckDB version: {version}")
print(f"Fichier créé: {db_path}")

conn.close()