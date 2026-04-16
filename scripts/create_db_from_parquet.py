# create_db_from_parquet.py

import duckdb

print("=== CRÉATION DE LA BASE DE DONNÉES ===")

try:
    # Créer une base de données DuckDB
    conn = duckdb.connect("database.duckdb")
    
    # Charger toutes les tables depuis les fichiers Parquet
    # peut être faire un model ? pour les nom des tables ?? car c'est en brut...
    conn.execute("CREATE TABLE erp_table AS SELECT * FROM 'erp_table.parquet'")
    print("Table erp_table créée")
    
    conn.execute("CREATE TABLE liaison_table AS SELECT * FROM 'liaison_table.parquet'")
    print("Table liaison_table créée")
    
    conn.execute("CREATE TABLE web_table AS SELECT * FROM 'web_table.parquet'")
    print("Table web_table créée")
    
    # Vérifier le contenu
    for table_name in ["erp_table", "liaison_table", "web_table"]:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        print(f"   - {table_name}: {result[0]:,} lignes")
    
    print("Base de données créée avec succès : database.duckdb")

except Exception as e:
    print(f" Erreur lors de la création de la base : {e}")
    raise
finally:
    if 'conn' in locals():
        conn.close()
        print(" Connexion fermée")